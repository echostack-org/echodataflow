from __future__ import annotations

import re
import warnings
from importlib.resources import files
from pathlib import Path
import datetime
import asyncio

import numpy as np
import pandas as pd
import xarray as xr
import numpy as np

import echopype as ep
import echoregions as er

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from prefect import flow, task, get_run_logger, get_client
from prefect_dask import DaskTaskRunner
from prefect.states import Cancelled, Failed
from prefect import runtime
from prefect.variables import Variable

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.utils.utils import (
    round_up_mins,
    get_slice_start_end_times,
    extract_datetime_from_filename,
)

from segmentation_inference.model import binary_hake_model
from segmentation_inference.utils import get_MVBS_tensor

import torch

# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


# Load binary hake models with weights
def get_hake_model(model_path: str) -> binary_hake_model:
    model = binary_hake_model().eval()
    model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu'))["state_dict"])
    return model


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner()
)
def flow_raw2Sv(
    exclude_before: str|None = None,
    exclude_raw_file: list[str] = [],
    parallel: bool = False,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    datagram_type: str|None = None,
    nmea_sentence: str|None = None,
    filename_pattern: str = "*.raw",
    path_main: str = "",
    path_raw: str = "",
    file_Sv_csv: str = "Sv_files.csv",
    new_file_num_limit: int = 50,
):

    # Check if the deployment is already running
    already_running = asyncio.run(deployment_already_running())
    if already_running:
        async def cancel_run():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Cancelled(message="Another instance of this flow is already running")
                )
        asyncio.run(cancel_run())
        return  # exit the flow early

    # Assemble paths
    path_Sv_zarr = Path(path_main) / "Sv"
    file_Sv_csv = Path(path_main) / file_Sv_csv
    path_raw = Path(path_raw)

    # Set up folder to store converted Sv zarr
    if not path_Sv_zarr.exists():
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert backto string to pass into task

    # Load info dataframe containing raw to Sv correspondence
    if not file_Sv_csv.exists():
        df_Sv = pd.DataFrame(
            columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
        )
        df_Sv.to_csv(file_Sv_csv)
    else:
        df_Sv = pd.read_csv(
            file_Sv_csv,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"]
        )
        df_Sv.sort_values(
            by="first_ping_time",
            inplace=True,
            ignore_index=True
        )

    # Exclude raw files before exclude_before datetime
    if exclude_before is None:
        raw_files_in_folder = set([filename.name for filename in path_raw.glob(filename_pattern)])
    else:
        raw_files_in_folder = set([
            filename.name for filename in path_raw.glob(filename_pattern)
            if extract_datetime_from_filename(filename.name) >= datetime.datetime.fromisoformat(exclude_before)
        ])

    if df_Sv.empty:
        raw_files_in_df = set()
    else:
        raw_files_in_df = set(df_Sv["raw_filename"].tolist())
    last_raw_filename = df_Sv.iloc[-1]["raw_filename"] if not df_Sv.empty else None
    if last_raw_filename:
        df_Sv = df_Sv[:-1]  # drop the most recent file processed

    # Find new files to process
    new_files = raw_files_in_folder.difference(raw_files_in_df)
    print(f"Found {len(new_files)} new files to process")

    # Reprocess last file in case it was incomplete
    if last_raw_filename:
        print(f"Reprocess {last_raw_filename}")
        new_files.add(last_raw_filename)

    # Skip files in exclude_raw_file list
    if len(exclude_raw_file) > 0:
        print(f"Exclude {exclude_raw_file} from processing")
        new_files.difference_update(set(exclude_raw_file))

    # Sort new files
    new_files = sorted(list(new_files))

    # Limit number of new files to process
    if new_file_num_limit != -1 and len(new_files) > new_file_num_limit:
        print(
            f"More than {new_file_num_limit} new files to process. "
            f"Limiting to first {new_file_num_limit} files."
        )
        new_files = new_files[:new_file_num_limit]
    print(
        f"Files to process: \n"
        + "".join([f"- {nf}\n" for nf in new_files])
    )

    # Bundle up task_raw2Sv parameters
    task_kwargs = dict(
        path_Sv_zarr=path_Sv_zarr,
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        depth_offset=depth_offset,
        sonar_model=sonar_model,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
    )

    if parallel:
        # Convert raw files to Sv in parallel
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_raw2Sv.with_options(
                task_run_name=nf, name=nf, retries=3
            )
            future = new_processed_raw.submit(path_raw / nf, **task_kwargs)
            future_all.append(future)

        results = []
        for nf, ff in zip(new_files, future_all):
            result = [nf] + list(ff.result())
            results.append(result)

    else:
        # Convert raw files to Sv sequentially
        errors = []
        print("Processing raw files sequentially")
        results = []
        for nf in new_files:
            try:
                print(f"Converting {nf}")
                Sv_filename, first_ping_time, last_ping_time = task_raw2Sv.with_options(
                    task_run_name=nf, name=nf, retries=3
                )(
                    raw_path=path_raw / nf, **task_kwargs
                )
                results.append([nf, Sv_filename, first_ping_time, last_ping_time])
            except Exception as e:
                errors.append(e)
                print(f"Error converting {nf}: {e}")

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
        )
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.sort_values(
            by=["first_ping_time"],
            inplace=True,
            ignore_index=True
        )
        df_Sv.to_csv(file_Sv_csv, date_format="%Y-%m-%dT%H:%M:%S.%f")
        print(f"Added {len(new_files)} new entries to tracking CSV")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during raw to Sv conversion out of {len(new_files)} files"
        async def set_failed_state():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Failed(message=error_msg)
                )
        asyncio.run(set_failed_state())
        raise Exception(error_msg)


@task(
    log_prints=True,
    tags=["raw2Sv"],
)
def task_raw2Sv(
    raw_path: str,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,  # in meters
    sonar_model: str = "EK80",
    datagram_type: str|None = None,
    nmea_sentence: str|None = None,
    path_Sv_zarr: str = "PATH_TO_STORE_SV_ZARR",
):
    """
    Convert raw sonar data to Sv and save to zarr format.
    """
    # Convert raw file, consolidate Sv and save to zarr
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model=sonar_model,
    )

    # Compute Sv and consolidate depth and location
    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode=waveform_mode,
        encode_mode=encode_mode,
    )
    ds_Sv = ep.consolidate.add_depth(
        ds=ds_Sv,
        depth_offset=depth_offset,
    )
    ed["Platform"] = ed["Platform"].drop_duplicates("time1")
    ds_Sv = ep.consolidate.add_location(
        ds=ds_Sv,
        echodata=ed,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
    )
    
    # Save to zarr
    out_path = Path(path_Sv_zarr) / f"{Path(raw_path).stem}_Sv.zarr"
    ds_Sv.to_zarr(
        store=out_path,
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return (
        out_path.name, 
        pd.to_datetime(ds_Sv["ping_time"][0].values),
        pd.to_datetime(ds_Sv["ping_time"][-1].values),
    )


@flow(log_prints=True)
async def flow_create_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    range_bin: str = "1m",
    ping_time_bin: str = "5s",
    path_main: str = "",
    file_Sv_csv: str = "Sv_files.csv",
    file_MVBS_csv: str = "MVBS_files.csv",
):
    """
    Process raw files to create MVBS files of specified length.

    Parameters
    ----------
    time_offset_seconds : float
        The time offset in seconds from current time to set the end time for MVBS computation.
    slice_mins : int
        Length of each slice in minutes.
    num_slices : int
        The number of slices to create.
    """
    logger = get_run_logger()

    # with concurrency("create_MVBS", occupy=1):
    # Set end_time to current time - time_offset_seconds
    end_time = round_up_mins(
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
        slice_mins=slice_mins,
    ).astimezone(datetime.timezone.utc)  # convert to UTC

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
        f"- num_slices: {num_slices}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
    )

    # Assemble paths
    file_Sv_csv = Path(path_main) / file_Sv_csv
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    path_Sv_zarr = Path(path_main) / "Sv"
    path_MVBS_zarr = Path(path_main) / "MVBS"

    # Validate zarr store paths
    if not path_Sv_zarr.exists():
        # raise ValueError("Sv zarr store does not exist, check raw2Sv flow!")
        logger.info("Sv zarr store does not exist, check raw2Sv flow! Creating empty folder for now.")
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    if not path_MVBS_zarr.exists():
        path_MVBS_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert back to string to pass into task
    path_MVBS_zarr = str(path_MVBS_zarr)  # convert back to string to pass into task

    # Load Sv and MVBS info dataframes
    if not file_Sv_csv.exists():
        raise ValueError("Sv info csv does not exist, check raw2Sv flow!")
    df_Sv = pd.read_csv(
        file_Sv_csv,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    # Convert last_ping_time and first_ping_time to UTC
    if not df_Sv.empty:
        if df_Sv["last_ping_time"].dt.tz is None:
            df_Sv["last_ping_time"] = df_Sv["last_ping_time"].dt.tz_localize("UTC")
        if df_Sv["first_ping_time"].dt.tz is None:
            df_Sv["first_ping_time"] = df_Sv["first_ping_time"].dt.tz_localize("UTC")
    else:
        logger.info(
            "Sv info csv is empty, raw2Sv flow may have just started! "
            "No MVBS can be created, exiting flow."
        )
        return

    if not file_MVBS_csv.exists():
        df_MVBS = pd.DataFrame(
            columns=["MVBS_filename", "first_ping_time", "last_ping_time"]
        )
        df_MVBS.to_csv(file_MVBS_csv)
    else:
        df_MVBS = pd.read_csv(
            file_MVBS_csv,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"]
        )

    # Sequentially create MVBS slices
    errors = []
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get Sv files in the specified time range
        Sv_filenames = sorted(
        df_Sv[
                (pd.to_datetime(df_Sv["last_ping_time"]) >= start_time[snum]) &
                (pd.to_datetime(df_Sv["first_ping_time"]) <= end_time[snum])
            ]["Sv_filename"].tolist()
        )
        logger.info(
            f"Found {len(Sv_filenames)} Sv files in the specified time range: \n"
            + "".join([f"- {svf}\n" for svf in Sv_filenames])
        )

        # If no Sv files found, skip this slice
        if len(Sv_filenames) == 0:
            logger.info(f"No Sv files found for slice {snum+1}, skipping")
            continue

        # Create MVBS for this slice
        try:
            MVBS_filename = f"MVBS_{start_time[snum].strftime("%Y%m%dT%H%M%S")}.zarr"
            first_ping_time, last_ping_time = task_create_MVBS.with_options(
                task_run_name=MVBS_filename,
                name=MVBS_filename,
            )(
                start_time=start_time[snum],
                end_time=end_time[snum],
                range_bin=range_bin,
                ping_time_bin=ping_time_bin,
                path_MVBS_zarrr=path_MVBS_zarr,
                MVBS_filename=MVBS_filename,
                path_Sv_zarr=path_Sv_zarr,
                Sv_filenames=Sv_filenames,
            )

            # Add MVBS slice info to dataframe
            if MVBS_filename in df_MVBS["MVBS_filename"].values:
                logger.info(f"MVBS file {MVBS_filename} already exists, updating first and last ping times")
                idx_to_add = df_MVBS.index[df_MVBS["MVBS_filename"] == MVBS_filename]
            else:
                logger.info(f"Adding new MVBS file {MVBS_filename} to tracking dataframe")
                idx_to_add = len(df_MVBS)
            df_MVBS.loc[idx_to_add] = [MVBS_filename, first_ping_time, last_ping_time]
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during MVBS creation for slice {snum+1}: {e}")

    # Save updated MVBS info dataframe
    df_MVBS.to_csv(file_MVBS_csv, date_format="%Y-%m-%dT%H:%M:%S")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during MVBS creation out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Failed(message=error_msg)
            )
        raise Exception(error_msg)


@task(log_prints=True)
def task_create_MVBS(
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    range_bin: str,
    ping_time_bin: str,
    path_MVBS_zarrr: str,
    MVBS_filename: str,
    path_Sv_zarr: str,
    Sv_filenames: list[str],
):
    """
    Create MVBS from Sv files in the specified time range.

    Parameters
    ----------
    MVBS_filename : str
        The name of the MVBS file to create.
    start_time : pd.Timestamp
        The start time for the MVBS slice.
    end_time : pd.Timestamp
        The end time for the MVBS slice.
    """
    logger = get_run_logger()

    # Remove timezone info for slicing
    start_time = start_time.replace(tzinfo=None)
    end_time = end_time.replace(tzinfo=None)

    # Combine Sv files into a single dataset
    ds_Sv = xr.open_mfdataset(
        [Path(path_Sv_zarr) / svf for svf in Sv_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat='override',
        chunks={"channel": 1, "ping_time": 1000, "range_sample": -1},
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time-pd.to_timedelta("1nanoseconds"))
    )

    # Compute MVBS for the slice
    ds_MVBS = ep.commongrid.compute_MVBS(
        ds_Sv=ds_Sv,
        range_var="depth",
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
        reindex=False,
        fill_value=np.nan,
    )

    # Save to zarr: 1 chunk along each dimension
    logger.info(f"Saving MVBS to {MVBS_filename}")
    ds_MVBS.chunk({"channel": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_MVBS_zarrr) / MVBS_filename,  # existing file will be overwritten
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return (
        pd.to_datetime(ds_MVBS["ping_time"][0].values),
        pd.to_datetime(ds_MVBS["ping_time"][-1].values),
    )


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
    file_prediction_csv: str = ""
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
    ).astimezone(datetime.timezone.utc)  # convert to UTC

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
    df_MVBS = pd.read_csv(
        file_MVBS_csv,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    # Convert last_ping_time and first_ping_time to UTC
    if not df_MVBS.empty:
        if df_MVBS["last_ping_time"].dt.tz is None:
            df_MVBS["last_ping_time"] = df_MVBS["last_ping_time"].dt.tz_localize("UTC")
        if df_MVBS["first_ping_time"].dt.tz is None:
            df_MVBS["first_ping_time"] = df_MVBS["first_ping_time"].dt.tz_localize("UTC")
    else:
        logger.info(
            "MVBS info csv is empty, create_MVBS flow may have just started! "
            "No prediction can be made, exiting flow."
        )
        return

    if not file_prediction_csv.exists():
        df_prediction = pd.DataFrame(
            columns=[
                "prediction_filename_postfix",
                "score_filename",
                "softmax_filename",
                "prediction_filename",
                "evr_filename",
                "first_ping_time",
                "last_ping_time"
            ]
        )
        df_prediction.to_csv(file_prediction_csv)
    else:
        df_prediction = pd.read_csv(
            file_prediction_csv,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"]
        )
        # Convert last_ping_time and first_ping_time to UTC
        # only allowable when dataframe is not empty
        if len(df_prediction) != 0:
            if df_prediction["last_ping_time"].dt.tz is None:
                df_prediction["last_ping_time"] = df_prediction["last_ping_time"].dt.tz_localize("UTC")
            if df_prediction["first_ping_time"].dt.tz is None:
                df_prediction["first_ping_time"] = df_prediction["first_ping_time"].dt.tz_localize("UTC")

    # Sequentially predict over combined MVBS slices
    errors = []
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get MVBS files in the specified time range
        MVBS_filenames = sorted(
            df_MVBS[
                (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[snum]) &
                (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[snum])
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
            predict_filename_postfix=f"{start_time[snum].strftime("%Y%m%dT%H%M%S")}"

            # predict hake on the MVBS files
            (
                # used for task_compute_NASC_direct
                ds_MVBS_combine,
                da_predict_hake,
                # used for book keeping
                score_filename,
                softmax_filename,
                prediction_filename,
                evr_filename,
                first_ping_time,
                last_ping_time
            ) = task_predict_hake.with_options(
                task_run_name=f"predict_{predict_filename_postfix}",
                name=f"predict_{predict_filename_postfix}",
            )(
                predict_filename_postfix=predict_filename_postfix,
                MVBS_filenames=MVBS_filenames,
                start_time=start_time[snum],
                end_time=end_time[snum],
                model=model,
                temperature=temperature,
                softmax_threshold=softmax_threshold,
                max_depth=max_depth,
                path_MVBS_zarr=path_MVBS_zarr,
                path_prediction_zarr=path_prediction_zarr,
                path_prediction_evr=path_prediction_evr,
            )

            # Compute NASC directly from the prediction
            task_compute_NASC.with_options(
                task_run_name=f"NASC_{predict_filename_postfix}",
                name=f"NASC_{predict_filename_postfix}",                
            )(
                NASC_filename=f"NASC_{predict_filename_postfix}.zarr",
                ds_MVBS_combine=ds_MVBS_combine,
                da_predict_hake=da_predict_hake,
                path_NASC_zarr=path_NASC_zarr,  # use the same path as predictions
            )

            # Add prediction slice info to dataframe
            # Only add if NASC is also computed
            if predict_filename_postfix in df_prediction["prediction_filename_postfix"].values:
                logger.info(f"Prediction file {predict_filename_postfix} already exists, updating first and last ping times")
                idx_to_add = df_prediction.index[df_prediction["prediction_filename_postfix"] == predict_filename_postfix]
            else:
                logger.info(f"Adding new prediction file {predict_filename_postfix} to tracking dataframe")
                idx_to_add = len(df_prediction)
            df_prediction.loc[idx_to_add] = [
                predict_filename_postfix,
                score_filename,
                softmax_filename,
                prediction_filename,
                evr_filename,
                first_ping_time,
                last_ping_time,
            ]
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during prediction for slice {snum+1}: {e}")
        
        # Save updated prediction info dataframe
        df_prediction.to_csv(file_prediction_csv, date_format="%Y-%m-%dT%H:%M:%S")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during prediction out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Failed(message=error_msg)
            )
        raise Exception(error_msg)  # Stop the flow execution


@task(log_prints=True)
def task_predict_hake(
    predict_filename_postfix: str,
    MVBS_filenames: list[str],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    model: binary_hake_model,
    temperature: int = 0.5,
    softmax_threshold: float = 0.5,
    max_depth: float = 590.0,
    path_MVBS_zarr: str = "PATH_TO_MVBS_ZARR",
    path_prediction_zarr: str = "PATH_TO_PREDICTION_ZARR",
    path_prediction_evr: str = "PATH_TO_PREDICTION_EVR",
):
    """
    Predict on a single MVBS file.

    This function combines multiple MVBS files into a single dataset,
    convert it to the input tensor, and feed it into the model for prediction.

    Parameters
    ----------
    predict_filename_postfix : str
        Postfix for the prediction filename, typically a timestamp.
    MVBS_filenames : list[str]
        List of MVBS filenames to process.
    start_time : pd.Timestamp
        The start time for the MVBS computation.
    end_time : pd.Timestamp
        The end time for the MVBS computation.
    model : BinaryHakeModel
        The trained model to use for prediction.
    temperature : float
        Temperature parameter for softmax scaling in prediction.
    softmax_threshold : float
        Threshold to determine hake presence.
    max_depth : float
        Max depth to predict hake.
    """
    logger = get_run_logger()
    # Remove timezone info for slicing
    start_time = start_time.replace(tzinfo=None)
    end_time = end_time.replace(tzinfo=None)

    # Combine MVBS files into a single dataset
    ds_MVBS_combine = xr.open_mfdataset(
        [Path(path_MVBS_zarr) / mvbsf for mvbsf in MVBS_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat='override',
        chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time-pd.to_timedelta("10milliseconds")),
        depth=slice(None, max_depth)  # slice to what the model expects
    )

    # Prepare input tensor: slice depth and ensure order of coordinates
    input_tensor = get_MVBS_tensor(ds_MVBS_combine)

    # Predict using the model
    output_dict = model.forward(input_tensor, softmax_temperature=temperature)
    score_tensor = output_dict["interpolated_output"].detach()
    score_tensor_softmax = output_dict["softmax_output"].detach()

    # Assemble output DataArrays
    da_score = xr.DataArray(
        score_tensor,
        coords={
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"],
            "ping_time": ds_MVBS_combine["ping_time"],
        },
        name="score",
    )
    da_score_softmax = xr.DataArray(
        score_tensor_softmax,
        coords={
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"],
            "ping_time": ds_MVBS_combine["ping_time"],
        },
        name="softmax_score",
    )
    da_predict_hake = (
        da_score_softmax
        .sel(scatterer_class="hake")  # only need hake class
        .transpose("ping_time", "depth")  # TODO: remove once update echopype to 0.10.2
        .drop_vars("scatterer_class")
    ) > softmax_threshold
    da_predict_hake.name = "hake_prediction"

    # Save to zarr
    score_filename = f"score_{predict_filename_postfix}.zarr"
    softmax_filename = f"softmax_{predict_filename_postfix}.zarr"
    prediction_filename = f"prediction_{predict_filename_postfix}.zarr"
    da_score.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_prediction_zarr) / score_filename,
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_prediction_zarr) / softmax_filename,
        mode="w",
        consolidated=True,
    )
    da_predict_hake.chunk({"ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_prediction_zarr) / prediction_filename,
        mode="w",
        consolidated=True,
    )

    # Save to evr
    evr_filename = f"prediction_{predict_filename_postfix}.evr"
    er.write_evr(
        Path(path_prediction_evr) / evr_filename,
        da_predict_hake,
        region_classification="hake",
    )

    return (
        ds_MVBS_combine,
        da_predict_hake,
        score_filename,
        softmax_filename,
        prediction_filename,
        evr_filename,
        # Need to enforce UTC as df_prediction for which these values will be
        # put into is already read in as UTC (if it already exists before this
        # flow).
        pd.to_datetime(ds_MVBS_combine["ping_time"][0].values, utc=True),
        pd.to_datetime(ds_MVBS_combine["ping_time"][-1].values, utc=True),
    )


@task(log_prints=True)
def task_compute_NASC(
    NASC_filename: str,
    ds_MVBS_combine: xr.Dataset,
    da_predict_hake: xr.DataArray,
    path_NASC_zarr: str = "PATH_TO_SAVE_NASC_ZARR",
):
    logger = get_run_logger()

    # Apply mask based on threshold
    ds_MVBS_combine_masked = ep.mask.apply_mask(
        source_ds=ds_MVBS_combine,
        mask=da_predict_hake,
        var_name="Sv",
        fill_value=np.nan,
    )

    # Compute NASC from MVBS and hake prediction
    ds_NASC = ep.commongrid.compute_NASC(
        ds_Sv=ds_MVBS_combine_masked,
        range_bin="10m",
        dist_bin="0.5nmi"
    )

    # Save to zarr
    logger.info(f"Saving NASC to zarr: {NASC_filename}")
    ds_NASC.to_zarr(
        store=Path(path_NASC_zarr) / NASC_filename,
        mode="w",
        consolidated=True,
    )
