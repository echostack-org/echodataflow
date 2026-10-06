"""Reusable Prefect tasks for acoustic data processing."""

import xarray as xr
from prefect import get_run_logger, task

from echodataflow.operations.operations_acoustics import (
    CreateMVBSResult,
    CreateMVBSSettings,
    CreateMVBSWorkItem,
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
    compute_NASC_from_masked_Sv,
    create_MVBS,
    convert_raw_to_Sv,
)


@task(log_prints=True)
def task_raw2Sv(
    item: RawToSvWorkItem,
    settings: RawToSvSettings,
) -> RawToSvResult:
    """Run one raw-to-Sv operation as a Prefect task."""
    return convert_raw_to_Sv(item, settings)


@task(log_prints=True)
def task_create_MVBS(
    item: CreateMVBSWorkItem,
    settings: CreateMVBSSettings,
) -> CreateMVBSResult:
    """Create one MVBS slice as a Prefect task."""
    logger = get_run_logger()
    logger.info(f"Saving MVBS to {item.mvbs_filename}")
    return create_MVBS(item, settings)


@task(log_prints=True)
def task_compute_NASC_from_masked_Sv(
    ds_Sv_masked: xr.Dataset,
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
) -> xr.Dataset:
    """Compute NASC from a masked Sv dataset as a Prefect task."""

    return compute_NASC_from_masked_Sv(
        ds_Sv_masked=ds_Sv_masked,
        range_bin=range_bin,
        dist_bin=dist_bin,
    )