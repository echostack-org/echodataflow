"""Flows for hake prediction and downstream NASC processing."""

from __future__ import annotations

import datetime
from pathlib import Path

import echopype as ep
import pandas as pd
from prefect import flow, get_client, get_run_logger, runtime
from prefect.states import Failed

from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    PREDICTION_COLUMNS_POSTPROCESSING,
    MVBS_COLUMNS_REALTIME,
    PREDICTION_COLUMNS_REALTIME,
    read_manifest,
    write_manifest,
)
from echodataflow.operations.operations_postprocessing import (
    build_prediction_ledger,
    failure_state,
    plan_prediction_slices,
    propagate_status,
    read_or_create_ledger,
)
from echodataflow.operations.operations_predict_hake import (
    ComputeNASCSettings,
    ComputeNASCWorkItem,
    PredictHakeResult,
    PredictHakeSettings,
    PredictHakeWorkItem,
    get_hake_model,
)
from echodataflow.tasks.tasks_predict_hake import task_compute_NASC, task_predict_hake
from echodataflow.utils.utils import get_slice_start_end_times, round_up_mins

# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


@flow(log_prints=True)
async def flow_predict_hake(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    temperature: float = 0.5,
    softmax_threshold: float = 0.5,
    max_depth: float = 590.0,
    path_weight: str = "",
    path_main: str = "",
    file_MVBS_csv: str = "",
    file_prediction_csv: str = "",
):
    """
    Predict on MVBS files of specified length.

    Parameters
    ----------
    time_offset_seconds : float
        The time offset in seconds from current time to set the end time for MVBS computation.
    slice_mins : int
        Length of each slice in minutes.
    num_slices : int
        The number of slices to create.
    temperature : float
        Temperature parameter for softmax scaling in prediction.
    softmax_threshold : float
        Threshold to determine hake presence.
    max_depth : float
        Max depth to predict hake.
    """
    logger = get_run_logger()

    # Load binary hake model with weights
    model = get_hake_model(path_weight)

    # Set end_time to current time - time_offset_seconds
    end_time = round_up_mins(
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
        slice_mins=slice_mins,
    ).astimezone(
        datetime.timezone.utc
    )  # convert to UTC

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
        f"- num_slices: {num_slices}\n"
        f"- temperature: {temperature}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
    )

    # Assemble paths
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    file_prediction_csv = Path(path_main) / file_prediction_csv
    path_MVBS_zarr = Path(path_main) / "MVBS"
    path_prediction_zarr = Path(path_main) / "prediction"
    path_prediction_evr = Path(path_main) / "EVR"
    path_NASC_zarr = Path(path_main) / "NASC"
    if not path_MVBS_zarr.exists():
        raise ValueError("MVBS zarr store does not exist, check create_MVBS flow!")
    if not path_prediction_zarr.exists():
        path_prediction_zarr.mkdir(parents=True, exist_ok=True)
    if not path_prediction_evr.exists():
        path_prediction_evr.mkdir(parents=True, exist_ok=True)
    if not path_NASC_zarr.exists():
        path_NASC_zarr.mkdir(parents=True, exist_ok=True)
    # convert back to string to pass into task
    path_MVBS_zarr = str(path_MVBS_zarr)
    path_prediction_zarr = str(path_prediction_zarr)
    path_prediction_evr = str(path_prediction_evr)
    path_NASC_zarr = str(path_NASC_zarr)

    # Load Sv and MVBS info dataframes
    if not file_MVBS_csv.exists():
        raise ValueError("MVBS info csv does not exist, check create_MVBS flow!")
    df_MVBS = read_manifest(
        file_MVBS_csv,
        MVBS_COLUMNS_REALTIME,
        ["first_ping_time", "last_ping_time"],
    )
    if df_MVBS.empty:
        logger.info(
            "MVBS info csv is empty, create_MVBS flow may have just started! "
            "No prediction can be made, exiting flow."
        )
        return

    prediction_manifest_exists = file_prediction_csv.exists()
    df_prediction = read_manifest(
        file_prediction_csv,
        PREDICTION_COLUMNS_REALTIME,
        ["first_ping_time", "last_ping_time"],
    )
    if not prediction_manifest_exists:
        write_manifest(df_prediction, file_prediction_csv)

    prediction_settings = PredictHakeSettings(
        model=model,
        directory_MVBS=path_MVBS_zarr,
        directory_prediction=path_prediction_zarr,
        directory_evr=path_prediction_evr,
        temperature=temperature,
        softmax_threshold=softmax_threshold,
        max_depth=max_depth,
    )
    nasc_settings = ComputeNASCSettings(output_directory=path_NASC_zarr)

    # Sequentially predict over combined MVBS slices
    errors = []
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get MVBS files in the specified time range
        MVBS_filenames = sorted(
            df_MVBS[
                (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[snum])
                & (pd.to_datetime(df_MVBS["first_ping_time"]) < end_time[snum])
            ]["MVBS_filename"].tolist()
        )
        logger.info(
            f"Found {len(MVBS_filenames)} MVBS files in the specified time range: \n"
            + "".join([f"- {mvbsf}\n" for mvbsf in MVBS_filenames])
        )

        # Skip prediction if no MVBS files found
        if len(MVBS_filenames) == 0:
            logger.info(f"No MVBS files found for slice {snum+1}, skipping")
            continue

        # Predict on the MVBS files and compute NASC
        try:
            predict_filename_postfix = start_time[snum].strftime("%Y%m%dT%H%M%S")

            # Predict hake on the MVBS files
            # The structured result contains both bookkeeping metadata and
            # the arrays needed by task_compute_NASC
            prediction_result: PredictHakeResult = task_predict_hake.with_options(
                task_run_name=f"predict_{predict_filename_postfix}",
                name=f"predict_{predict_filename_postfix}",
            )(
                PredictHakeWorkItem(
                    start_time=start_time[snum],
                    end_time=end_time[snum],
                    filenames_MVBS=tuple(MVBS_filenames),
                    filename_postfix=predict_filename_postfix,
                ),
                prediction_settings,
            )

            # Compute NASC directly from the prediction
            task_compute_NASC.with_options(
                task_run_name=f"NASC_{predict_filename_postfix}",
                name=f"NASC_{predict_filename_postfix}",
            )(
                ComputeNASCWorkItem(
                    nasc_filename=f"NASC_{predict_filename_postfix}.zarr",
                    prediction=prediction_result,
                ),
                nasc_settings,
            )

            # Add prediction slice info to dataframe
            # Only add if NASC is also computed
            if predict_filename_postfix in df_prediction["prediction_filename_postfix"].values:
                logger.info(
                    f"Prediction file {predict_filename_postfix} already exists, updating first and last ping times"
                )
                idx_to_add = df_prediction.index[
                    df_prediction["prediction_filename_postfix"] == predict_filename_postfix
                ]
            else:
                logger.info(
                    f"Adding new prediction file {predict_filename_postfix} to tracking dataframe"
                )
                idx_to_add = len(df_prediction)
            df_prediction.loc[idx_to_add] = [
                predict_filename_postfix,
                prediction_result.score_filename,
                prediction_result.softmax_filename,
                prediction_result.prediction_filename,
                prediction_result.evr_filename,
                prediction_result.first_ping_time,
                prediction_result.last_ping_time,
            ]
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during prediction for slice {snum+1}: {e}")

        # Save updated prediction info dataframe
        write_manifest(df_prediction, file_prediction_csv)

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during prediction out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id, state=Failed(message=error_msg)
            )
        raise Exception(error_msg)  # Stop the flow execution


@flow(log_prints=True)
def flow_predict_hake_postprocessing(
    path_main: str,
    path_weight: str,
    prediction_slice_mins: int = 40,
    new_file_num_limit: int = -1,
    max_flow_run_attempts: int = 3,
    temperature: float = 0.5,
    softmax_threshold: float = 0.5,
    max_depth: float = 590.0,
    nasc_range_bin: str = "10m",
    nasc_dist_bin: str = "0.5nmi",
    file_MVBS_csv: str = "MVBS_files.csv",
    file_prediction_csv: str = "prediction_files.csv",
) -> None:
    """Predict all newly ready windows, combining aligned MVBS slices."""

    logger = get_run_logger()
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    if not file_MVBS_csv.exists():
        logger.info("MVBS ledger does not yet exist")
        return

    # Create persistent destinations before loading the model
    for directory in ["prediction", "EVR", "NASC"]:
        (Path(path_main) / directory).mkdir(parents=True, exist_ok=True)

    # Load available MVBS slices and prior prediction results for resume behavior
    df_MVBS = read_manifest(
        file_MVBS_csv,
        MVBS_COLUMNS_POSTPROCESSING,
        ["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
    )
    file_prediction_csv = Path(path_main) / file_prediction_csv
    df_prediction = read_or_create_ledger(
        ledger_path=file_prediction_csv,
        columns=PREDICTION_COLUMNS_POSTPROCESSING,
        date_columns=["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
        builder=lambda: build_prediction_ledger(
            df_MVBS,
            prediction_slice_mins,
        ),
    )

    # Make terminal MVBS failures explicit in downstream planning
    updated_prediction = propagate_status(
        df_MVBS,
        df_prediction,
        upstream_filename_column="MVBS_filename",
        upstream_status_column="MVBS_status",
        downstream_filenames_column="MVBS_filenames",
        downstream_status_column="prediction_status",
        upstream_label="MVBS slices",
    )
    if not updated_prediction.equals(df_prediction):
        df_prediction = updated_prediction
        write_manifest(df_prediction.sort_values("slice_start"), file_prediction_csv)

    # Assemble aligned prediction windows from completed MVBS slices
    planned = plan_prediction_slices(
        df_MVBS,
        df_prediction,
    )
    if new_file_num_limit != -1:
        planned = planned[:new_file_num_limit]
    if not planned:
        logger.info("No newly ready prediction windows")
        return

    # Load the model once and reuse it across all ready windows in this flow run
    model = get_hake_model(path_weight)
    prediction_settings = PredictHakeSettings(
        model=model,
        directory_MVBS=str(Path(path_main) / "MVBS"),
        directory_prediction=str(Path(path_main) / "prediction"),
        directory_evr=str(Path(path_main) / "EVR"),
        temperature=temperature,
        softmax_threshold=softmax_threshold,
        max_depth=max_depth,
    )
    nasc_settings = ComputeNASCSettings(
        output_directory=str(Path(path_main) / "NASC"),
        range_bin=nasc_range_bin,
        dist_bin=nasc_dist_bin,
    )
    # Prediction remains sequential because all windows share the loaded model
    errors = []
    for item in planned:
        postfix = item.start_time.strftime("%Y%m%dT%H%M%S")
        try:
            result = task_predict_hake.with_options(task_run_name=f"predict_{postfix}")(
                PredictHakeWorkItem(
                    start_time=item.start_time,
                    end_time=item.end_time,
                    filenames_MVBS=item.filenames,
                    filename_postfix=postfix,
                ),
                prediction_settings,
            )
            # Compute NASC only after the prediction for this window succeeds
            task_compute_NASC.with_options(task_run_name=f"NASC_{postfix}")(
                ComputeNASCWorkItem(
                    nasc_filename=f"NASC_{postfix}.zarr",
                    prediction=result,
                ),
                nasc_settings,
            )
            idx = df_prediction.index[df_prediction["prediction_filename_postfix"] == postfix][0]
            df_prediction.loc[
                idx,
                [
                    "score_filename",
                    "softmax_filename",
                    "prediction_filename",
                    "evr_filename",
                    "first_ping_time",
                    "last_ping_time",
                    "prediction_status",
                    "error",
                ],
            ] = [
                result.score_filename,
                result.softmax_filename,
                result.prediction_filename,
                result.evr_filename,
                result.first_ping_time,
                result.last_ping_time,
                "completed",
                "",
            ]
            # Persist after each window so a failed later window does not lose progress
            write_manifest(df_prediction.sort_values("slice_start"), file_prediction_csv)
        except Exception as exc:
            errors.append(exc)
            idx = df_prediction.index[df_prediction["prediction_filename_postfix"] == postfix][0]
            attempt_count, status = failure_state(
                int(df_prediction.loc[idx, "attempt_count"]), max_flow_run_attempts
            )
            df_prediction.loc[idx, ["prediction_status", "attempt_count", "error"]] = [
                status,
                attempt_count,
                str(exc),
            ]
            write_manifest(df_prediction.sort_values("slice_start"), file_prediction_csv)
            logger.error("Prediction window %s failed: %s", item.start_time, exc)
    if errors:
        raise RuntimeError(f"{len(errors)} prediction windows failed")
