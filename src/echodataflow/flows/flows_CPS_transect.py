from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import xarray as xr
from prefect import flow
from prefect_dask import DaskTaskRunner

from echodataflow.flows.cps_helpers import (
    _export_nasc_to_echoview_csv,
)
from echodataflow.tasks.tasks_acoustics import (
    task_compute_NASC_from_masked_Sv,
)
from echodataflow.utils.processing_ledger import (
    get_completed_cps_files,
    get_transects_to_process,
    mark_transect_completed,
    mark_transect_failed,
    mark_transect_incomplete,
    mark_transect_processing,
    resolve_database,
)


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(),
)
def flow_process_transect_CPS(
    path_transect_csv: str,
    path_snapshot_csv: str,
    path_main: str,
    processing_db: str = "processing.db",
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    nasc_process_id: int = 1928,
    exclude_before: str | None = None,
):
    """
    Assemble fully processed per-file CPS Sv products by transect
    and compute NASC.
    """

    path_main = Path(path_main)

    path_cps_sv = path_main / "CPS_Sv"
    path_bottom = path_main / "CPS_Seafloor_CSVs"
    path_nasc = path_main / "CPS_NASC_Zarr"
    path_nasc_csv = path_main / "CPS_NASC_CSV"

    db_path = resolve_database(
        path_main,
        processing_db,
    )

    for path in [
        path_bottom,
        path_nasc,
        path_nasc_csv,
    ]:
        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    # ---------------------------------------------------------
    # Select transects requiring processing from the ledger
    # ---------------------------------------------------------

    if (
        isinstance(db_path, Path)
        and not db_path.exists()
    ):
        print(
            f"Processing ledger not found: "
            f"{db_path}"
        )
        return

    transects_to_process = get_transects_to_process(
        db_path
    )

    if not transects_to_process:
        print(
            "No transects require CPS processing."
        )
        return

    # ---------------------------------------------------------
    # Process each pending transect
    # ---------------------------------------------------------

    for transect in transects_to_process:

        transect_id = transect["transect_id"]

        start = pd.to_datetime(
            transect["start_time"],
            utc=True,
        )

        end = pd.to_datetime(
            transect["end_time"],
            utc=True,
        )

        name = (
            f"transect_"
            f"{transect['transect_part']}"
        )

        mark_transect_processing(
            db_path,
            transect_id,
        )

        transect_t0 = perf_counter()

        # -----------------------------------------------------
        # Get completed per-file CPS products
        # -----------------------------------------------------

        cps_filenames = get_completed_cps_files(
            db_path,
            start_time=start,
            end_time=end,
        )

        if not cps_filenames:
            print(
                f"No CPS-ready Sv data for {name}"
            )

            mark_transect_incomplete(
                db_path,
                transect_id,
            )

            continue

        cps_paths = [
            path_cps_sv / filename
            for filename in cps_filenames
        ]

        cps_paths = [
            path
            for path in cps_paths
            if path.exists()
        ]

        if not cps_paths:
            print(
                f"{name}: CPS Sv files are "
                f"registered but missing on disk."
            )
            continue

        # -----------------------------------------------------
        # Assemble continuous CPS-ready transect
        #
        # These stores have ALREADY undergone:
        #
        #   common geometry
        #   depth recomputation
        #   Blackwell seafloor detection
        #   surface mask
        #   seafloor mask
        #   background-noise correction
        #   CPS classification
        #   final CPS masking
        #
        # Do not recompute those here.
        # -----------------------------------------------------

        print(
            f"{name}: opening {len(cps_paths)} CPS Sv stores"
        )
        stage_t0 = perf_counter()

        datasets = [
            xr.open_zarr(
                path,
                consolidated=True,
            )
            for path in cps_paths
        ]

        print(
            f"{name}: opened CPS Sv stores "
            f"in {perf_counter() - stage_t0:.1f} s"
        )

        print(
            f"{name}: assembling continuous transect"
        )
        stage_t0 = perf_counter()

        ds = xr.concat(
            datasets,
            dim="ping_time",
            data_vars="minimal",
            coords="minimal",
            compat="override",
        ).sortby(
            "ping_time"
        )

        _, unique_idx = np.unique(
            ds["ping_time"].values,
            return_index=True,
        )

        ds = ds.isel(
            ping_time=np.sort(unique_idx)
        )

        print(
            f"{name}: transect assembly complete "
            f"in {perf_counter() - stage_t0:.1f} s"
        )

        if ds.sizes.get("ping_time", 0) == 0:
            continue

        # -----------------------------------------------------
        # Require complete CPS-ready coverage
        # -----------------------------------------------------

        expected_start = start.tz_convert(None)
        expected_end = end.tz_convert(None)

        coverage_start = pd.Timestamp(
            ds["ping_time"].values[0]
        )

        coverage_end = pd.Timestamp(
            ds["ping_time"].values[-1]
        )

        tolerance = pd.Timedelta(
            seconds=5
        )

        if (
            coverage_start
            > expected_start + tolerance
            or coverage_end
            < expected_end - tolerance
        ):
            print(
                f"{name}: incomplete CPS Sv coverage. "
                f"Available: {coverage_start} -> "
                f"{coverage_end}; "
                f"required: {expected_start} -> "
                f"{expected_end}. "
                f"Leaving transect pending."
            )
            
            mark_transect_incomplete(
                db_path,
                transect_id,
                coverage_start,
                coverage_end,
            )
            
            continue

        ds = ds.sel(
            ping_time=slice(
                expected_start,
                expected_end,
            )
        )

        print(
            f"{name}: "
            f"{len(cps_paths)} CPS Sv files, "
            f"{ds.sizes['ping_time']} pings"
        )

        # -----------------------------------------------------
        # Validate required per-file products
        # -----------------------------------------------------

        required_variables = [
            "Sv",
            "depth",
            "valid_water_column",
            "Sv_corrected",
            "final_cps_mask",
            "Sv_masked",
        ]

        missing_variables = [
            variable
            for variable in required_variables
            if variable not in ds
        ]

        if missing_variables:
            print(
                f"{name}: CPS Sv products are "
                f"missing required variables: "
                f"{missing_variables}"
            )
            continue

        # -----------------------------------------------------
        # Reconstruct transect bottom line from the per-file
        # Blackwell products.
        #
        # Blackwell is NOT recomputed here.
        # -----------------------------------------------------

        bottom_frames = []

        for cps_path in cps_paths:

            file_name = (
                cps_path.name.removesuffix(
                    "_CPS.zarr"
                )
            )

            file_bottom_path = (
                path_bottom
                / f"{file_name}_bottom_line.csv"
            )

            if not file_bottom_path.exists():
                continue

            bottom_file = pd.read_csv(
                file_bottom_path
            )

            if bottom_file.empty:
                continue

            bottom_file["time"] = pd.to_datetime(
                bottom_file["time"],
                utc=True,
                errors="coerce",
            )

            bottom_frames.append(
                bottom_file
            )

        if bottom_frames:

            bottom_df = pd.concat(
                bottom_frames,
                ignore_index=True,
            )

            bottom_df = (
                bottom_df
                .dropna(
                    subset=[
                        "time",
                        "depth",
                    ]
                )
                .drop_duplicates(
                    subset="time"
                )
                .sort_values("time")
            )

            bottom_df = bottom_df.loc[
                (
                    bottom_df["time"] >= start
                )
                & (
                    bottom_df["time"] <= end
                )
            ].copy()

            transect_bottom_path = (
                path_bottom
                / f"{name}_bottom_line.csv"
            )

            bottom_df.to_csv(
                transect_bottom_path,
                index=False,
            )

        # -----------------------------------------------------
        # Expensive CPS processing is intentionally NOT done
        # here. Each CPS_Sv file already contains Sv_corrected,
        # final_cps_mask, and Sv_masked. Transect processing is
        # limited to assembly, slicing, NASC, and export.
        # -----------------------------------------------------

        # -----------------------------------------------------
        # NASC
        # -----------------------------------------------------

        print(
            f"{name}: starting NASC calculation"
        )
        stage_t0 = perf_counter()

        # Keep only the variables needed by echopype.compute_NASC.
        #
        # Important: compute_NASC internally assumes that the first
        # dataset dimension is the channel-like dimension. After
        # concatenation, Dataset dimension ordering can differ from
        # the ordering of the individual Sv variables, so explicitly
        # transpose the NASC input to:
        #
        #   channel, ping_time, range_sample
        #
        # This preserves multi-frequency NASC while avoiding an
        # erroneous extra range_sample grouping dimension.
        ds_for_nasc = ds[
            [
                "Sv_masked",
                "depth",
                "latitude",
                "longitude",
                "frequency_nominal",
            ]
        ].transpose(
            "channel",
            "ping_time",
            "range_sample",
            missing_dims="ignore",
        )

        print(
            f"{name}: NASC dataset dimensions: "
            f"{list(ds_for_nasc.sizes.keys())}"
        )

        print(
            f"{name}: Sv_masked dims: "
            f"{ds_for_nasc['Sv_masked'].dims}, "
            f"shape: {ds_for_nasc['Sv_masked'].shape}"
        )

        print(
            f"{name}: depth dims: "
            f"{ds_for_nasc['depth'].dims}, "
            f"shape: {ds_for_nasc['depth'].shape}"
        )

        ds_nasc = (
            task_compute_NASC_from_masked_Sv(
                ds_Sv_masked=ds_for_nasc,
                range_bin=range_bin,
                dist_bin=dist_bin,
            )
        )

        print(
            f"{name}: NASC calculation complete "
            f"in {perf_counter() - stage_t0:.1f} s"
        )

        nasc_path = (
            path_nasc
            / f"{name}_nasc.zarr"
        )

        print(
            f"{name}: writing NASC Zarr -> {nasc_path}"
        )
        stage_t0 = perf_counter()

        ds_nasc.to_zarr(
            nasc_path,
            mode="w",
            consolidated=True,
        )

        print(
            f"{name}: NASC Zarr written "
            f"in {perf_counter() - stage_t0:.1f} s"
        )

        print(
            f"{name}: exporting NASC Echoview CSV"
        )
        stage_t0 = perf_counter()

        _export_nasc_to_echoview_csv(
            ds_nasc,
            path_nasc_csv
            / f"{name}_nasc.csv",
            process_id=nasc_process_id,
        )

        print(
            f"{name}: NASC CSV exported "
            f"in {perf_counter() - stage_t0:.1f} s"
        )

        print(
            f"{name}: CPS + NASC complete "
            f"in {perf_counter() - transect_t0:.1f} s total"
        )
        
        mark_transect_completed(
            db_path,
            transect_id,
            coverage_start,
            coverage_end,
        )