import asyncio
import datetime
import re
import warnings
from importlib.resources import files
from pathlib import Path

import boto3
import dask_image.ndfilters
import echopype as ep
import numpy as np
import pandas as pd
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config
from prefect import flow, get_client, get_run_logger, runtime, task
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
from prefect.states import Cancelled, Failed
from prefect.variables import Variable
from prefect_dask import DaskTaskRunner
from scipy.signal import convolve2d

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.flows.flows_acoustics import flow_raw2Sv
from echodataflow.utils.utils import (
    extract_datetime_from_filename,
    get_slice_start_end_times,
    round_up_mins,
)

@task(name="dilate_7x7")
def dilate_7x7(da: xr.DataArray) -> xr.DataArray:
    """Applies a 7x7 max filter (dilation) safely across Dask chunks."""
    dilated_dask_array = dask_image.ndfilters.maximum_filter(
        da.data, 
        size=(1, 7, 7) 
    )
    return xr.DataArray(dilated_dask_array, dims=da.dims, coords=da.coords)


@task(log_prints=True)
def task_compute_NASC(
        NASC_filename: str,
        ds_Sv_masked: xr.Dataset,
        path_NASC_zarr: str = "PATH_TO_SAVE_NASC_ZARR",
    ):
    logger = get_run_logger()


    ds_NASC = ep.commongrid.compute_NASC(
        ds_Sv=ds_Sv_masked,
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


@task(
    log_prints=True,
    tags=["acoustic_processing"],
)
def task_process_acoustic(
    raw_path: str,
    encode_mode: str = "complex",
    waveform_mode: str = "CW",
    sonar_model: str = "EK80",
    path_output_zarr: str = "",
    path_output_csv: str = "",
):
    """
    Process raw sonar data using the CPS notebook logic: computes Sv, aligns geometry,
    removes background noise, detects seafloor (exports CSV), calculates variance, 
    and applies boolean masking (exports to Zarr).
    """
    print(f"Loading and processing {raw_path}...")
    
    zarr_chunks = {"channel": 1, "ping_time": 1000, "range_sample": -1}

    ed = ep.open_raw(
        raw_file=str(raw_path),
        sonar_model=sonar_model,
        use_swap=True,
        # storage_options={"anon": True}, # Uncomment if fetching straight from S3
    )

    # 1. Compute Sv and Angles
    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode=waveform_mode,
        encode_mode=encode_mode,
    )
    ds_Sv = ep.consolidate.add_splitbeam_angle(
        ds_Sv, ed, waveform_mode=waveform_mode, encode_mode=encode_mode, to_disk=False
    )
    ds_Sv = ep.consolidate.add_depth(ds=ds_Sv, echodata=ed)

    # 2. Align Data & Resample
    target_channel = "WBT 987763-15 ES38-7_ES"
    chunked_ds = ds_Sv.chunk({"channel": 1, "ping_time": 1000, "range_sample": -1})
    
    aligned_ds_Sv = ep.commongrid.resample_to_geometry(
        chunked_ds, target_variable="Sv", target_channel=target_channel
    )
    aligned_ds_Sv["sound_absorption"] = ds_Sv["sound_absorption"]
    aligned_ds_Sv = ep.consolidate.add_depth(aligned_ds_Sv)

    aligned_ds_Sv["angle_athwartship"] = ep.commongrid.resample_to_geometry(
        chunked_ds, target_variable="angle_athwartship", target_channel=target_channel
    )["angle_athwartship"]
        
    aligned_ds_Sv["angle_alongship"] = ep.commongrid.resample_to_geometry(
        chunked_ds, target_variable="angle_alongship", target_channel=target_channel
    )["angle_alongship"]
    
    ds_Sv[["Sv", "echo_range", "angle_athwartship", "angle_alongship"]] = aligned_ds_Sv[["Sv", "echo_range", "angle_athwartship", "angle_alongship"]]
    ds_Sv = ep.consolidate.add_depth(ds_Sv)

    # 3. Clean Background Noise
    ds_Sv = ep.clean.remove_background_noise(
        ds_Sv, ping_num=20, range_sample_num=5, SNR_threshold="5.0dB"
    )

    # 4. Detect Seafloor
    q = 0.95
    ch_sel = ds_Sv.sel(channel=target_channel)
    angle_alonghsip_threshold = ch_sel["angle_alongship"].rolling(ping_time=7, range_sample=7).mean().quantile([q]).values[0]
    angle_athwartship_threshold = ch_sel["angle_athwartship"].rolling(ping_time=7, range_sample=7).mean().quantile([q]).values[0]

    blackwell_depth = ep.mask.detect_seafloor(
        ds=ds_Sv,
        method="blackwell",
        params={
            "channel": target_channel,
            "var_name": "Sv",
            "threshold": (-58, angle_alonghsip_threshold, angle_athwartship_threshold),
            "offset": 0.2, "r0": 25, "r1": 750, "wtheta": 7, "wphi": 7,
        }
    )
    
    # Dump Seafloor line to CSV
    df = pd.DataFrame({
        "time": blackwell_depth["ping_time"].values,
        "depth": blackwell_depth.values,
    })
    df = df[df["depth"] > -0.2]
    out_csv = Path(path_output_csv) / f"{Path(raw_path).stem}_bottom_line.csv"
    df.to_csv(out_csv, index=False)

    # 5. Compute Variance, Smoothing, & Classification Mask
    ds_Sv["Sv_smoothed"] = ds_Sv["Sv_corrected"].rolling(ping_time=3, range_sample=11).mean() 
    ds_Sv["variance"] = (10 ** (ds_Sv["Sv_corrected"] / 10) - 10 ** (ds_Sv["Sv_smoothed"] / 10)) ** 2
    ds_Sv["variance_smoothed"] = ds_Sv["variance"].rolling(ping_time=3, range_sample=11).mean()
    ds_Sv["variance_smoothed"] = 10 * np.log10(ds_Sv["variance_smoothed"] ** 0.5)
    
    # Applying Dilation (using .fn to call the task's python function directly inside this task)
    ds_Sv["variance_smoothed"] = dilate_7x7.fn(ds_Sv["variance_smoothed"])
    
    sd_200 = ds_Sv["variance_smoothed"].sel(channel="WBT 987771-15 ES200-7C_ES")
    sd_120 = ds_Sv["variance_smoothed"].sel(channel="WBT 987753-15 ES120-7C_ES")

    # Final Boolean target mask
    mask_sd = (sd_200 > -65) & (sd_120 > -65)

    ds_Sv["Sv_smoothed"] = ds_Sv["Sv_corrected"].rolling(
    ping_time=3,
    range_sample=11
    ).mean() 

    channel_38 = "WBT 987763-15 ES38-7_ES"
    ds_Sv["Sv_dilated"] = dilate_7x7.fn(ds_Sv["Sv_smoothed"])
    
    differencing = ds_Sv["Sv_dilated"] - ds_Sv["Sv_dilated"].sel(channel=channel_38)
    
    # 2. Extract the specific channels ONCE
    diff_200 = differencing.sel(channel="WBT 987771-15 ES200-7C_ES")
    diff_120 = differencing.sel(channel="WBT 987753-15 ES120-7C_ES")
    diff_70  = differencing.sel(channel="WBT 987766-15 ES70-7C_ES")

# 3. Apply the boolean logic using the extracted variables
    mask_frequency_response = (
        ((diff_200 > -13.51) & (diff_200 < 12.53)) &
        ((diff_120 > -13.50) & (diff_120 < 9.37))  &
        ((diff_70 > -13.85)  & (diff_70 < 9.89)) 
    )

    final_mask = mask_frequency_response & mask_sd

    ds_Sv["Sv"] = ds_Sv['Sv'].where(final_mask)

    ds_Sv = ep.consolidate.add_location(ds = ds_Sv, echodata=ed, datagram_type = 'MRU1')


    ds_Sv = ds_Sv.chunk(zarr_chunks)
    

    # 6. Save Mask Output to Zarr
    ds_nasc = task_compute_NASC.fn(
        NASC_filename=f"{Path(raw_path).stem}_nasc.zarr",
        ds_Sv_masked=ds_Sv,
        path_NASC_zarr=path_output_zarr
    )


    out_zarr = Path(path_output_zarr) / f"{Path(raw_path).stem}_mask.zarr"
    ds_Sv.to_zarr(
        store=out_zarr,
        mode="w",
        consolidated=True,
    )
    
    return (
        out_zarr.name, 
        pd.to_datetime(ds_Sv["ping_time"][0].values),
        pd.to_datetime(ds_Sv["ping_time"][-1].values)
    )


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner()
)
def flow_process_acoustic_data(
    exclude_before: str | None = None,
    exclude_raw_file: list[str] = [],
    parallel: bool = False,
    encode_mode: str = "complex",
    waveform_mode: str = "CW",
    sonar_model: str = "EK80",
    filename_pattern: str = "*.raw",
    path_main: str = "processed_data",
    path_raw: str = "raw_data",
    file_Sv_csv: str = "processed_files_registry.csv",
    new_file_num_limit: int = 50,
):
    print(f"Starting flow_process_acoustic_data with parameters:\n")
    errors = []
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
    path_main_obj = Path(path_main)
    path_Sv_zarr = path_main_obj / "Sv_Masks_Zarr"
    path_csv_outputs = path_main_obj / "Seafloor_CSVs"
    file_Sv_csv_path = path_main_obj / file_Sv_csv
    path_raw_obj = Path(path_raw)

    # Set up folder to store converted Zarrs & CSVs
    path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    path_csv_outputs.mkdir(parents=True, exist_ok=True)
    
    path_Sv_zarr = str(path_Sv_zarr)
    path_csv_outputs = str(path_csv_outputs)

    # Load info dataframe containing tracking correspondence
    if not file_Sv_csv_path.exists():
        df_Sv = pd.DataFrame(
            columns=["raw_filename", "zarr_mask_filename", "first_ping_time", "last_ping_time"]
        )
        df_Sv.to_csv(file_Sv_csv_path)
    else:
        df_Sv = pd.read_csv(
            file_Sv_csv_path,
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
        raw_files_in_folder = set([filename.name for filename in path_raw_obj.glob(filename_pattern)])
    else:
        raw_files_in_folder = set([
            filename.name for filename in path_raw_obj.glob(filename_pattern)
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
        
    if new_files:
        print(f"Files to process: \n" + "".join([f"- {nf}\n" for nf in new_files]))

    # Bundle up parameters
    task_kwargs = dict(
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        sonar_model=sonar_model,
        path_output_zarr=path_Sv_zarr,
        path_output_csv=path_csv_outputs
    )

    if parallel:
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_process_acoustic.with_options(
                task_run_name=nf, name=nf, retries=2
            )
            future = new_processed_raw.submit(path_raw_obj / nf, **task_kwargs)
            future_all.append(future)

        results = []
        for nf, ff in zip(new_files, future_all):
            result = [nf] + list(ff.result())
            results.append(result)

    else:
        errors = []
        print("Processing raw files sequentially")
        results = []
        for nf in new_files:
            try:
                print(f"Processing {nf}")
                zarr_filename, first_ping_time, last_ping_time = task_process_acoustic.with_options(
                    task_run_name=nf, name=nf, retries=2
                )(
                    raw_path=path_raw_obj / nf, **task_kwargs
                )
                results.append([nf, zarr_filename, first_ping_time, last_ping_time])
            except Exception as e:
                errors.append(e)
                print(f"Error processing {nf}: {e}")

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "zarr_mask_filename", "first_ping_time", "last_ping_time"]
        )
        print(df_new)
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.sort_values(
            by=["first_ping_time"],
            inplace=True,
            ignore_index=True
        )
        df_Sv.to_csv(file_Sv_csv_path, date_format="%Y-%m-%dT%H:%M:%S.%f")
        print(f"Added {len(new_files)} new entries to tracking CSV")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during acoustic processing out of {len(new_files)} files"
        
        # Safely grab the flow_run_id from the Prefect runtime
        try:
            current_run_id = runtime.flow_run.id
        except AttributeError:
            current_run_id = None

        if current_run_id:
            async def set_failed_state():
                async with get_client() as client:
                    await client.set_flow_run_state(
                        flow_run_id=current_run_id,
                        state=Failed(message=error_msg)
                    )
            asyncio.run(set_failed_state())
            
        raise Exception(error_msg)


if __name__ == "__main__":
    flow_process_acoustic_data(path_raw=r"C:\Users\elias.capriles\Desktop\Raw", path_main=r"C:\Users\elias.capriles\Desktop\Raw", new_file_num_limit=10)
#"C:\Users\elias.capriles\Desktop\Raw"