"""Flows that simulate realtime arrival of source data."""

from __future__ import annotations

import datetime
import re
from pathlib import Path

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config
from prefect import flow, runtime
from prefect.variables import Variable

from echodataflow.operations.operations_storage import (
    S3CopyResult,
    S3CopySettings,
    S3CopyWorkItem,
)
from echodataflow.tasks.tasks_storage import task_copy_s3_file


def _var_key(prefix: str) -> str:
    deployment_id = runtime.deployment.id or "no_deployment"
    return f"{prefix}_{deployment_id}"


def _iter_s3_keys(s3_client, s3_bucket: str, prefix: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


@flow(log_prints=True)
def flow_copy_raw(
    time_offset_seconds: float = 0.0,
    path_raw_list: str = "",
    path_copy: str = "",
    s3_bucket: str = "noaa-wcsd-pds",
    exclude_before: str | None = None,
    exclude_after: str | None = None,
    endpoint_url: str = "https://sdsc.osn.xsede.org",
) -> list[S3CopyResult]:
    """Copy raw files whose timestamps simulate new realtime arrivals."""
    print("Copy raw files to simulate data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")

    df_raw = pd.read_csv(path_raw_list)
    # Accept legacy naive values and offset-qualified values in the same file
    df_raw["timestamp"] = pd.to_datetime(
        df_raw["timestamp"], format="mixed", utc=True
    )

    # Set flow execution time to current time - time_offset_seconds
    flow_time_curr = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        seconds=time_offset_seconds
    )
    print(f"Simulated flow run start time: {flow_time_curr}")

    # Get previous flow execution time from Prefect variable, if it exists
    flow_time_prev = Variable.get(_var_key(prefix="prev_start_time"), default=None)
    flow_time_prev = pd.to_datetime(flow_time_prev, utc=True) if flow_time_prev else None
    print(f"Previous run start time: {flow_time_prev}")

    # Find the last files that would have been generated
    # between the previous and current flow execution times
    idx_wanted = df_raw["timestamp"] < flow_time_curr

    if exclude_before is not None:
        exclude_before_datetime = pd.to_datetime(exclude_before, utc=True)
        idx_wanted &= df_raw["timestamp"] >= exclude_before_datetime

    if exclude_after is not None:
        exclude_after_datetime = pd.to_datetime(exclude_after, utc=True)
        idx_wanted &= df_raw["timestamp"] < exclude_after_datetime

    if flow_time_prev is not None:
        idx_wanted &= df_raw["timestamp"] > flow_time_prev
    df_raw = df_raw[idx_wanted]

    if df_raw.empty:
        print("No new files generated since the last flow execution. Skipping file copy.")

        Variable.set(
            _var_key(prefix="prev_start_time"),
            flow_time_curr.isoformat(),
            overwrite=True,
        )

        return []

    # Setting up task to download
    settings = S3CopySettings(
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
    )

    results: list[S3CopyResult] = []
    for s3_path_value in df_raw["s3_path"]:
        s3_path = str(s3_path_value)
        filename = Path(s3_path).name
        print(f"Copying {filename} to {path_copy}")
        result = task_copy_s3_file.with_options(
            task_run_name=filename,
            name=filename,
            retries=3,
        )(
            S3CopyWorkItem(
                s3_path=s3_path,
                local_path=str(Path(path_copy) / filename),
            ),
            settings,
        )
        results.append(result)

    # Store the current flow execution time in a Prefect variable for future reference
    Variable.set(
        _var_key(prefix="prev_start_time"),
        flow_time_curr.isoformat(),
        overwrite=True,
    )
    return results


@flow(log_prints=True)
def flow_copy_trawl(
    path_copy: str = "",
    s3_bucket: str = "agr230002-bucket01",
    s3_prefix: str = "prefect_sh2506_test/trawl",
    trawl_folders: list[str] = [
        "CatchPercentages",
        "LengthFreq",
        "NetConfig",
        "Specimens",
    ],
    start_trawl_num: int = 1,
    trawl_num_step: int = 1,
    endpoint_url: str = "https://sdsc.osn.xsede.org",
) -> list[S3CopyResult]:
    """
    Copy trawl files for the next trawl number from OSN S3 into local folders.

    Each run increments an internal trawl number state (001, 002, ...), then
    downloads files matching that number from each folder under s3_prefix.
    Missing folders for the current trawl number are skipped.
    """
    print("Copy trawl files to simulate trawl data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")

    # Determine next trawl number from Prefect variable state
    trawl_num_prev = Variable.get(_var_key(prefix="prev_trawl_num"), default=None)
    trawl_num_curr = (
        start_trawl_num if trawl_num_prev is None else int(trawl_num_prev) + trawl_num_step
    )
    trawl_num_str = f"{trawl_num_curr:03d}"
    print(f"Previous trawl number: {trawl_num_prev}")
    print(f"Current trawl number: {trawl_num_str}")

    # Configure anonymous access to OSN endpoint
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        config=Config(signature_version=UNSIGNED),
    )

    # Match files where trawl number appears as an isolated token, e.g. _001_ or 001_
    path_copy_local = Path(path_copy)
    path_copy_local.mkdir(parents=True, exist_ok=True)
    trawl_num_pattern = re.compile(rf"(^|_){trawl_num_str}(_|\b)")
    settings = S3CopySettings(
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
    )
    results: list[S3CopyResult] = []

    for folder in trawl_folders:
        folder_prefix = f"{s3_prefix.rstrip('/')}/{folder}/"
        matching_keys = []

        for key in _iter_s3_keys(s3, s3_bucket, folder_prefix):
            filename = Path(key).name
            if not filename.lower().endswith(".xlsx"):
                continue
            if filename.startswith("~$"):
                continue
            if trawl_num_pattern.search(filename) is None:
                continue
            matching_keys.append(key)

        if len(matching_keys) == 0:
            print(f"No matching files for trawl {trawl_num_str} in {folder}; skipping.")
            continue

        matching_keys = sorted(matching_keys)
        print(
            f"Matching files for trawl {trawl_num_str} in {folder}:\n"
            + "".join([f"- {key}\n" for key in matching_keys])
        )
        folder_dest = path_copy_local / folder
        folder_dest.mkdir(parents=True, exist_ok=True)

        for key in matching_keys:
            filename = Path(key).name
            local_path = folder_dest / filename
            print(f"Copying {key} to {local_path}")
            result = task_copy_s3_file.with_options(
                task_run_name=filename,
                name=filename,
                retries=3,
            )(
                S3CopyWorkItem(
                    s3_path=key,
                    local_path=str(local_path),
                ),
                settings,
            )
            results.append(result)

    # Store the current trawl number for next flow execution
    Variable.set(
        _var_key(prefix="prev_trawl_num"),
        str(trawl_num_curr),
        overwrite=True,
    )
    print(f"Flow complete. Downloaded {len(results)} files for trawl {trawl_num_str}.")
    return results


@flow(log_prints=True)
def flow_simulate_transects(
    path_transect_csv: str,
    survey_start: str,
    transect_duration_minutes: int = 60,
    start_transect_num: int = 1,
    max_transects: int = 20,
) -> None:
    """Simulate realtime arrival of completed transect rows."""

    path_transect = Path(path_transect_csv)
    path_transect.parent.mkdir(parents=True, exist_ok=True)

    survey_start_time = pd.to_datetime(survey_start, utc=True)

    transect_state_key = _var_key(prefix="transect_state")
    state = Variable.get(transect_state_key, default=None)

    if state is None:
        transect_num_curr = start_transect_num
        action = "open"
    else:
        transect_num_str, action = str(state).split(":", maxsplit=1)
        transect_num_curr = int(transect_num_str)

    if transect_num_curr > max_transects:
        print("All simulated transects have been generated.")
        return

    transect_num = f"{transect_num_curr:03d}"

    transect_offset = (transect_num_curr - start_transect_num) * transect_duration_minutes
    transect_start = survey_start_time + pd.Timedelta(minutes=transect_offset)

    transect_end = transect_start + pd.Timedelta(minutes=transect_duration_minutes)

    if path_transect.exists():
        df = pd.read_csv(path_transect, dtype="string")
    else:
        df = pd.DataFrame(
            columns=[
                "transectPart",
                "transectNumber",
                "transectStart",
                "transectEnd",
            ],
            dtype="string",
        )

    # ---------------------------------------------
    # OPEN transect
    # ---------------------------------------------
    if action == "open":
        row = pd.DataFrame(
            [
                {
                    "transectPart": transect_num,
                    "transectNumber": transect_num,
                    "transectStart": transect_start.isoformat(),
                    "transectEnd": pd.NA,
                }
            ],
            dtype="string",
        )

        df = pd.concat([df, row], ignore_index=True)

        df.to_csv(path_transect, index=False)

        Variable.set(transect_state_key, f"{transect_num_curr}:close", overwrite=True)

        print(f"Opened simulated transect {transect_num}: {transect_start}")

        return

    # ---------------------------------------------
    # CLOSE transect
    # ---------------------------------------------
    idx = df["transectPart"] == transect_num

    if not idx.any():
        message = f"Cannot close transect {transect_num}: transect not found in CSV."
        raise ValueError(message)

    df.loc[idx, "transectEnd"] = transect_end.isoformat()

    df.to_csv(path_transect, index=False)

    print(f"Closed simulated transect {transect_num}: {transect_end}")

    Variable.set(transect_state_key, f"{transect_num_curr + 1}:open", overwrite=True)
