"""File-level CPS execution: I/O and processing-state transitions."""

from pathlib import Path
from time import perf_counter

import pandas as pd
import xarray as xr
from prefect import task

from echodataflow.operations.cps_helpers import _mask_above_seafloor
from echodataflow.operations.operations_cps import (
    align_cps_geometry,
    apply_cps_mask,
    build_surface_mask,
    build_water_column_mask,
    classify_cps,
    correct_background_noise,
    detect_cps_seafloor,
)
from echodataflow.utils.processing_ledger import (
    mark_sv_cps_completed,
    mark_sv_cps_failed,
    mark_sv_cps_processing,
)


@task(log_prints=True, task_run_name="process-CPS-{sv_filename}")
def _process_one_sv_cps(
    sv_path: str,
    sv_filename: str,
    db_path: str,
    path_cps_sv: str,
    path_bottom: str,
    target_frequency: float,
    min_depth: float,
    seafloor_threshold: list,
    seafloor_offset: float,
    seafloor_r0: float,
    seafloor_r1: float,
    seafloor_wtheta: int,
    seafloor_wphi: int,
    mask_mode: str,
    fallback_sv_threshold: float,
    remove_background_noise: bool = True,
    save_intermediates: bool = True,
):
    """Process one generic Sv store into one CPS-ready Sv store."""
    sv_path = Path(sv_path)
    db_path = Path(db_path)
    path_cps_sv = Path(path_cps_sv)
    path_bottom = Path(path_bottom)
    name = sv_path.name.removesuffix("_Sv.zarr")
    cps_filename = f"{name}_CPS.zarr"
    cps_path = path_cps_sv / cps_filename
    try:
        print(f"Processing CPS Sv: {sv_path.name}")
        mark_sv_cps_processing(db_path, sv_path)
        ds = xr.open_zarr(sv_path, consolidated=True)
        ds, target_channel = align_cps_geometry(ds, target_frequency, sv_path.name)
        print(f"{sv_path.name}: materializing Blackwell inputs before seafloor detection")
        stage_t0 = perf_counter()
        for var in ["Sv", "depth", "angle_alongship", "angle_athwartship"]:
            da = ds[var].sel(channel=target_channel)
            print(f"{sv_path.name}: {var} shape={da.shape}, chunks={da.chunks}, dtype={da.dtype}")
            da.load()
        print(f"{sv_path.name}: Blackwell inputs materialized in {perf_counter() - stage_t0:.2f} s")
        stage_t0 = perf_counter()
        bottom_path = None
        try:
            bottom = detect_cps_seafloor(
                ds,
                target_channel,
                seafloor_threshold,
                seafloor_offset,
                seafloor_r0,
                seafloor_r1,
                seafloor_wtheta,
                seafloor_wphi,
            )
            print(
                f"{sv_path.name}: Blackwell call itself completed in {perf_counter() - stage_t0:.2f} s"
            )
            bottom_df = pd.DataFrame({"time": bottom["ping_time"].values, "depth": bottom.values})
            bottom_df = bottom_df[bottom_df["depth"] > -0.2]
            bottom_path = path_bottom / f"{name}_bottom_line.csv"
            bottom_df.to_csv(bottom_path, index=False)
            print(f"{sv_path.name}: seafloor detection complete")
        except Exception as exc:
            print(f"{sv_path.name}: seafloor detection failed: {exc}")
        surface_mask = build_surface_mask(ds, target_channel, min_depth)
        above_seafloor_mask = xr.ones_like(surface_mask, dtype=bool)
        if bottom_path is not None:
            try:
                above_seafloor_mask = _mask_above_seafloor(ds, bottom_path, target_channel)
            except Exception as exc:
                print(f"{sv_path.name}: seafloor mask failed: {exc}")
        ds = build_water_column_mask(ds, surface_mask, above_seafloor_mask, sv_path.name)
        stage_t0 = perf_counter()
        if remove_background_noise:
            ds = correct_background_noise(ds, sv_path.name)
        else:
            print(f"{sv_path.name}: skipping noise correction and noise-residual criterion (demo)")
            ds["Sv_corrected"] = ds["Sv"]
        ds.attrs["cps_background_noise_removed"] = int(remove_background_noise)
        ds.attrs["cps_noise_residual_criterion_applied"] = int(remove_background_noise)
        print(f"{sv_path.name}: noise stage returned in {perf_counter() - stage_t0:.2f} s")
        stage_t0 = perf_counter()
        final_mask = classify_cps(
            ds,
            ds["valid_water_column"],
            mask_mode,
            fallback_sv_threshold,
            sv_path.name,
            use_noise_residual=remove_background_noise,
        )
        ds = apply_cps_mask(ds, final_mask, sv_path.name)
        print(f"{sv_path.name}: classification graph prepared in {perf_counter() - stage_t0:.2f} s")
        if not save_intermediates:
            diagnostics = [
                "variance",
                "variance_smoothed",
                "Sv_dilated",
                "mask_sd120",
                "mask_sd200",
                "mask_sd",
                "mask_frequency",
                "difference_70_38",
                "difference_120_38",
                "difference_200_38",
                "mask_frequency70",
                "mask_frequency120",
                "mask_frequency200",
                "surface_mask",
                "above_seafloor_mask",
                "angle_alongship",
                "angle_athwartship",
            ]
            ds = ds.drop_vars(diagnostics, errors="ignore")
        for variable in ds.variables:
            ds[variable].encoding.pop("chunks", None)
        ds = ds.chunk(
            {
                "channel": 1,
                "ping_time": 256,
                "range_sample": 4096,
            }
        )
        stage_t0 = perf_counter()
        ds.to_zarr(cps_path, mode="w", consolidated=True)
        print(
            f"{sv_path.name}: compute and Zarr write completed in {perf_counter() - stage_t0:.2f} s"
        )
        first_ping_time = pd.to_datetime(ds["ping_time"][0].values, utc=True)
        last_ping_time = pd.to_datetime(ds["ping_time"][-1].values, utc=True)
        mark_sv_cps_completed(db_path, sv_path, cps_filename, first_ping_time, last_ping_time)
        print(f"{sv_path.name}: CPS Sv processing complete")
    except Exception as exc:
        mark_sv_cps_failed(db_path, sv_path, str(exc))
        print(f"{sv_path.name}: CPS processing failed: {exc}")
        raise
