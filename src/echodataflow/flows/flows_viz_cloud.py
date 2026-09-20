from os import path
from pathlib import Path
import datetime
import configparser

import pandas as pd
import numpy as np
import xarray as xr
import s3fs

from prefect import flow, get_run_logger

from echodataflow.utils.utils import get_slice_start_end_times
from echodataflow.utils.processing_ledger import (
    get_completed_cps_files,
    get_latest_completed_transect,
    resolve_database,
)


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
            index_col=0,
        )

    # Accept legacy naive values and offset-qualified values in the same file
    if not df_MVBS.empty:
        for column in ["first_ping_time", "last_ping_time"]:
            df_MVBS[column] = pd.to_datetime(
                df_MVBS[column], format="mixed", utc=True
            )

    # Get MVBS files in the specified time range (only 1 slice)
    MVBS_filenames = sorted(
        df_MVBS[
            (df_MVBS["last_ping_time"] >= start_time[0]) &
            (df_MVBS["first_ping_time"] <= end_time[0])
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

def _prepare_sv_for_echogram(
    ds: xr.Dataset,
    var_name: str = "Sv_masked",
) -> xr.Dataset:
    """Prepare an Sv dataset for Echoshader visualization."""

    plot_ds = xr.Dataset(
        {
            "Sv": ds["Sv"],
            "Sv_masked": ds[var_name],
        }
    )

    if "Sv_water_column" in ds:
        plot_ds["Sv_water_column"] = ds["Sv_water_column"]

    if "frequency_nominal" in ds:
        plot_ds["frequency_nominal"] = ds["frequency_nominal"]

    if "depth" in ds:
        vertical = ds["depth"]
    elif "echo_range" in ds:
        vertical = ds["echo_range"]
    else:
        vertical = ds["range_sample"]

    reduce_dims = [
        dim
        for dim in ["channel", "ping_time"]
        if dim in vertical.dims
    ]

    if reduce_dims:
        vertical = vertical.median(
            dim=reduce_dims,
            skipna=True,
        )

    vertical_values = np.asarray(vertical.values)

    # Sv datasets may have trailing range samples with no valid depth.
    # Keep the contiguous valid part used for visualization.
    finite = np.isfinite(vertical_values)

    if finite.any():
        invalid = np.flatnonzero(~finite)

        if invalid.size:
            valid_length = int(invalid[0])
        else:
            valid_length = vertical_values.size

        plot_ds = plot_ds.isel(
            range_sample=slice(0, valid_length)
        )

        vertical_values = vertical_values[:valid_length]

    if (
        vertical_values.size == 0
        or not np.isfinite(vertical_values).all()
    ):
        raise ValueError(
            "Could not construct a finite vertical coordinate "
            "for Sv visualization."
        )

    plot_ds = plot_ds.assign_coords(
        echo_range=(
            "range_sample",
            vertical_values,
        )
    )

    plot_ds = plot_ds.swap_dims(
        {"range_sample": "echo_range"}
    )

    vmin = float(
        plot_ds["Sv"].min(skipna=True).compute()
    )
    vmax = float(
        plot_ds["Sv"].max(skipna=True).compute()
    )

    plot_ds["Sv"] = plot_ds["Sv"].assign_attrs(
        actual_range=(vmin, vmax)
    )

    masked_vmin = float(
        plot_ds["Sv_masked"].min(skipna=True).compute()
    )
    masked_vmax = float(
        plot_ds["Sv_masked"].max(skipna=True).compute()
    )

    plot_ds["Sv_masked"] = plot_ds["Sv_masked"].assign_attrs(
        actual_range=(masked_vmin, masked_vmax)
    )

    for var in plot_ds.variables:
        plot_ds[var].encoding.pop("chunks", None)
        plot_ds[var].encoding.pop(
            "preferred_chunks",
            None,
        )

    return plot_ds

@flow()
def flow_update_cache_CPS(
    path_CPS: str,
    path_cache: str,
    processing_db: str = "processing.db",
    file_CPS_zarr: str = "latest_CPS.zarr",
):
    """Update visualization cache from the latest completed CPS transect."""

    path_CPS = Path(path_CPS)
    path_cache = Path(path_cache)

    path_cache.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------------------
    # Find latest completed transect from processing ledger
    # -----------------------------------------------------

    db_path = resolve_database(
        path_CPS.parent,
        processing_db,
    )

    transect = get_latest_completed_transect(
        db_path,
    )

    if transect is None:
        print(
            "CPS cache not updated: "
            "no completed transects found in processing ledger."
        )
        return

    transect_number = str(
        transect["transect_part"]
    )

    transect_start = pd.to_datetime(
        transect["start_time"],
        utc=True,
    )

    transect_end = pd.to_datetime(
        transect["end_time"],
        utc=True,
    )

    # -----------------------------------------------------
    # Find completed CPS products overlapping transect
    # -----------------------------------------------------

    cps_filenames = get_completed_cps_files(
        db_path,
        start_time=transect_start,
        end_time=transect_end,
    )

    cps_paths = [
        path_CPS / filename
        for filename in cps_filenames
        if (path_CPS / filename).exists()
    ]

    if not cps_paths:
        print(
            f"CPS cache not updated: no completed CPS products "
            f"found for transect {transect_number}."
        )
        return

    print(
        f"Building CPS visualization cache for transect "
        f"{transect_number} from {len(cps_paths)} CPS files."
    )

    # -----------------------------------------------------
    # Assemble CPS transect from per-file CPS products
    # -----------------------------------------------------

    datasets = [
        xr.open_zarr(
            path,
            consolidated=True,
        )
        for path in cps_paths
    ]

    ds_CPS = xr.concat(
        datasets,
        dim="ping_time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
    ).sortby("ping_time")

    # Remove duplicate ping times at file boundaries.
    _, unique_idx = np.unique(
        ds_CPS["ping_time"].values,
        return_index=True,
    )

    ds_CPS = ds_CPS.isel(
        ping_time=np.sort(unique_idx)
    )

    # Ledger timestamps are UTC-aware; xarray ping_time is
    # represented as timezone-naive datetime64.
    transect_start_naive = (
        transect_start.tz_convert(None)
    )

    transect_end_naive = (
        transect_end.tz_convert(None)
    )

    ds_CPS = ds_CPS.sel(
        ping_time=slice(
            transect_start_naive,
            transect_end_naive,
        )
    )

    if ds_CPS.sizes.get("ping_time", 0) == 0:
        print(
            f"CPS cache not updated: no pings remain "
            f"after slicing transect {transect_number}."
        )
        return

    # -----------------------------------------------------
    # Prepare dataset for Echoshader visualization
    # -----------------------------------------------------

    ds_CPS = _prepare_sv_for_echogram(
        ds_CPS,
        var_name="Sv_masked",
    )

    ds_CPS.attrs.update(
        {
            "transect_number": transect_number,
            "transect_start": str(
                transect_start_naive
            ),
            "transect_end": str(
                transect_end_naive
            ),
            "source_cps_files": len(
                cps_paths
            ),
        }
    )

    # -----------------------------------------------------
    # Save visualization cache
    # -----------------------------------------------------

    output_path = (
        path_cache
        / file_CPS_zarr
    )

    print(
        f"Saving transect "
        f"{transect_number} "
        f"to CPS visualization cache: "
        f"{output_path}"
    )

    ds_CPS.chunk(
        {
            "channel": 1,
            "ping_time": -1,
            "echo_range": 4000,
        }
    ).to_zarr(
        output_path,
        mode="w",
        consolidated=True,
    )