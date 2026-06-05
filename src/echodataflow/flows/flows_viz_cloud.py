from pathlib import Path
import datetime
import configparser

import pandas as pd
import xarray as xr
import s3fs

from prefect import flow, get_run_logger

from echodataflow.utils.utils import round_up_mins, get_slice_start_end_times


@flow()
def flow_update_cache_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 180,
    path_cache: str = "PATH_TO_DATA_CACHE",
    path_MVBS: str = "PATH_TO_MVBS_DATA_STORE",
    cred_file: str = "PATH_TO_CREDENTIALS_FILE",
    file_MVBS_csv: str = "MVBS_files.csv",
    file_MVBS_zarr: str = "latest_MVBS.zarr",
):
    logger = get_run_logger()

    # Set end_time to current time - time_offset_seconds
    end_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=time_offset_seconds)

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=1
    )

    # Get cloud bucket
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    # Get all MVBS files in the bucket
    MVBS_all = fs.glob(f"{path_MVBS}/*.zarr")
    MVBS_all = sorted([Path(f).name for f in MVBS_all])
    # logger.info(f"All MVBS files: {MVBS_all}")

    # Load MVBS info dataframe
    with fs.open(str(Path(path_MVBS).parent / file_MVBS_csv), "r") as f:
        df_MVBS = pd.read_csv(
            f,
            parse_dates=["first_ping_time", "last_ping_time"],
            index_col=0,
        )

    # Convert last_ping_time and first_ping_time to UTC
    if not df_MVBS.empty:
        if df_MVBS["last_ping_time"].dt.tz is None:
            df_MVBS["last_ping_time"] = df_MVBS["last_ping_time"].dt.tz_localize("UTC")
        if df_MVBS["first_ping_time"].dt.tz is None:
            df_MVBS["first_ping_time"] = df_MVBS["first_ping_time"].dt.tz_localize("UTC")

    # Get MVBS files in the specified time range (only 1 slice)
    MVBS_filenames = sorted(
        df_MVBS[
            (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[0]) &
            (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[0])
        ]["MVBS_filename"].tolist()
    )
    logger.info(
        f"Found {len(MVBS_filenames)} MVBS files in the specified time range: \n"
        + "".join([f"- {mvbsf}\n" for mvbsf in MVBS_filenames])
    )

    if len(MVBS_filenames) == 0:
        logger.info("MVBS cache not updated: no MVBS files in the specified time range")
        return
    else:
        # Assmeble fs mapper for the MVBS files
        MVBS_filenames = [
            fs.get_mapper(str(Path(path_MVBS) / mvbsf)) for mvbsf in MVBS_filenames
        ]

        # Combine and prepare MVBS dataset
        ds_MVBS = xr.open_mfdataset(
            MVBS_filenames,
            parallel=True,
            coords="minimal",
            data_vars="minimal",
            compat='override',
            chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
            engine="zarr",  # use zarr engine for reading
            consolidated=False,
        )
        # TODO: echo_range:depth swap can be removed once Echoshader is fixed
        ds_MVBS["echo_range"] = ds_MVBS["depth"]
        ds_MVBS = ds_MVBS.swap_dims({"depth": "echo_range"})

        # Add actual_range to allow using holoviz
        ds_MVBS["Sv"] = ds_MVBS["Sv"].assign_attrs(
            actual_range=(float(ds_MVBS["Sv"].min().compute()),
                        float(ds_MVBS["Sv"].max().compute()))
        )

        # Remove chunk encoding to prevent saving issues
        for var in ds_MVBS.data_vars:
            if "chunks" in ds_MVBS[var].encoding:
                ds_MVBS[var].encoding.pop("chunks")
            if "preferred_chunks" in ds_MVBS[var].encoding:
                ds_MVBS[var].encoding.pop("preferred_chunks")

        # Save to cache
        logger.info(f"Saving MVBS dataset to cache: {str(Path(path_cache) / file_MVBS_zarr)}")
        ds_MVBS.chunk(
            {"channel": -1, "ping_time": -1, "echo_range": -1}
        ).to_zarr(
            Path(path_cache) / file_MVBS_zarr,  # cache is local
            mode="w",
            consolidated=True,
        )