from pathlib import Path
import datetime
import asyncio

from yaml import safe_load

import numpy as np
import pandas as pd
import numpy as np

from flows_acoustics import (
    flow_raw2Sv, flow_create_MVBS, flow_predict_hake
)
from helpers import flow_file_upload


# Load workflow config params
with open(Path(__file__).parent / "config_ship_mass.yaml", "r") as file:
    config = safe_load(file)


# Run flow_raw2Sv for all available raw files
print("Run flow_raw2Sv for all available raw files...")
config["raw2Sv"]["new_file_num_limit"] = -1  # include all new files
flow_raw2Sv(**config["raw2Sv"])


# Helper function for flow_create_MVBS and flow_predict_hake
def get_start_end_times(file_path):
    """Get start and end times from csv."""
    df = pd.read_csv(
        file_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    if df["last_ping_time"].dt.tz is None:
        df["last_ping_time"] = df["last_ping_time"].dt.tz_localize("UTC")
    if df["first_ping_time"].dt.tz is None:
        df["first_ping_time"] = df["first_ping_time"].dt.tz_localize("UTC")
    return df["first_ping_time"].iloc[0], df["last_ping_time"].iloc[-1]


# Run flow_create_MVBS for all possible slices
print("Run flow_create_MVBS for all possible slices...")

if config["create_MVBS"]["num_slices"] == -1:
    # Get total number of slices
    time_start, time_end = get_start_end_times(
        Path(config["create_MVBS"]["path_main"]) / config["create_MVBS"]["file_Sv_csv"]
    )
    total_mins = (time_end - time_start).total_seconds() / 60
    num_slices = int(np.ceil(total_mins / config["create_MVBS"]["slice_mins"]))
    config["create_MVBS"]["num_slices"] = num_slices

    # Get time offset in seconds
    curr_time_offset = (
        datetime.datetime.now(datetime.timezone.utc)
        - time_end.astimezone(datetime.timezone.utc)
    )
    config["create_MVBS"]["time_offset_seconds"] = curr_time_offset.total_seconds()

    # Print computation info
    print(f"Sv files from {time_start} to {time_end}")
    print(f"Total mins: {total_mins:.2f}")
    print(f"Number of slices to process: {num_slices}")
    print(f"Time offset (s): {config['create_MVBS']['time_offset_seconds']:.2f}")

try:
    asyncio.run(flow_create_MVBS(**config["create_MVBS"]))
except Exception as e:
    print(f"Error in flow_create_MVBS: {e}")


# Run flow_predict_hake for all possible slices
print("Run flow_predict_hake for all possible slices...")

if config["predict_hake"]["num_slices"] == -1:
    # Get total number of slices
    time_start, time_end = get_start_end_times(
        Path(config["predict_hake"]["path_main"]) / config["predict_hake"]["file_MVBS_csv"]
    )
    total_mins = (time_end - time_start).total_seconds() / 60
    num_slices = int(np.ceil(total_mins / config["predict_hake"]["slice_mins"]))
    # num_slices = 10
    config["predict_hake"]["num_slices"] = num_slices

    # Get time offset in seconds
    curr_time_offset = (
        datetime.datetime.now(datetime.timezone.utc)
        - time_end.astimezone(datetime.timezone.utc)
    )
    config["predict_hake"]["time_offset_seconds"] = curr_time_offset.total_seconds()

    # Print computation info
    print(f"MVBS files from {time_start} to {time_end}")
    print(f"Total mins: {total_mins:.2f}")
    print(f"Number of slices to process: {num_slices}")
    print(f"Time offset (s): {config['predict_hake']['time_offset_seconds']:.2f}")

try:
    asyncio.run(flow_predict_hake(**config["predict_hake"]))
except Exception as e:
    print(f"Error in flow_predict_hake: {e}")


# Run file_upload_acoustics to upload all created acoustic files
flow_file_upload(**config["file_upload_acoustics"])
