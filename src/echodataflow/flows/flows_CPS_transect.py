from pathlib import Path

import echopype as ep
import numpy as np
import pandas as pd
import xarray as xr
from prefect import flow
from prefect_dask import DaskTaskRunner

from echodataflow.flows.cps_helpers import (
    _dilate_7x7,
    _export_nasc_to_echoview_csv,
    _pick_channel_by_frequency,
)
from echodataflow.tasks.tasks_acoustics import (
    task_compute_NASC_from_masked_Sv,
)
from echodataflow.utils.processing_ledger import (
    get_completed_cps_files,
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
    mask_mode: str = "cps",
    fallback_sv_threshold: float = -70,
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    nasc_process_id: int = 1928,
    exclude_before: str | None = None,
):
    """
    Assemble per-file CPS-ready Sv products by transect,
    apply transect-context CPS classification, and compute NASC.
    """

    path_main = Path(path_main)

    path_transect = Path(path_transect_csv)
    path_snapshot = Path(path_snapshot_csv)

    path_cps_sv = path_main / "CPS_Sv"
    path_cps = path_main / "CPS_Masks_Zarr"
    path_bottom = path_main / "CPS_Seafloor_CSVs"
    path_nasc = path_main / "CPS_NASC_Zarr"
    path_nasc_csv = path_main / "CPS_NASC_CSV"

    db_path = resolve_database(
        path_main,
        processing_db,
    )

    for path in [
        path_cps,
        path_bottom,
        path_nasc,
        path_nasc_csv,
    ]:
        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    # ---------------------------------------------------------
    # Select eligible completed transects
    # ---------------------------------------------------------

    current = pd.read_csv(
        path_transect,
        dtype={
            "transectPart": "string",
            "transectNumber": "string",
            "transectStart": "string",
            "transectEnd": "string",
        },
    )

    eligible = current.copy()

    eligible["transectStart"] = pd.to_datetime(
        eligible["transectStart"],
        utc=True,
        errors="coerce",
    )

    eligible["transectEnd"] = pd.to_datetime(
        eligible["transectEnd"],
        utc=True,
        errors="coerce",
    )

    if exclude_before is not None:
        cutoff = pd.Timestamp(exclude_before)

        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("UTC")
        else:
            cutoff = cutoff.tz_convert("UTC")

        eligible = eligible.loc[
            eligible["transectStart"] >= cutoff
        ].copy()

    completed = eligible.dropna(
        subset=[
            "transectPart",
            "transectStart",
            "transectEnd",
        ]
    ).copy()

    pending_rows = []

    for _, transect in completed.iterrows():

        name = (
            f"transect_"
            f"{transect['transectPart']}"
        )

        cps_output = (
            path_cps
            / f"{name}_CPS.zarr"
        )

        nasc_output = (
            path_nasc
            / f"{name}_nasc.zarr"
        )

        if (
            cps_output.exists()
            and nasc_output.exists()
        ):
            continue

        pending_rows.append(transect)

    changed = pd.DataFrame(
        pending_rows,
        columns=current.columns,
    )

    if changed.empty:
        current.to_csv(
            path_snapshot,
            index=False,
        )

        print(
            "No completed transects require CPS processing."
        )
        return

    if (
        isinstance(db_path, Path)
        and not db_path.exists()
    ):
        print(
            f"Processing ledger not found: "
            f"{db_path}"
        )
        return

    # ---------------------------------------------------------
    # Process each pending transect
    # ---------------------------------------------------------

    for _, transect in changed.iterrows():

        start = pd.to_datetime(
            transect["transectStart"],
            utc=True,
        )

        end = pd.to_datetime(
            transect["transectEnd"],
            utc=True,
        )

        name = (
            f"transect_"
            f"{transect['transectPart']}"
        )

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
        #
        # Do not recompute those here.
        # -----------------------------------------------------

        datasets = [
            xr.open_zarr(
                path,
                consolidated=True,
            )
            for path in cps_paths
        ]

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

        valid_water_column = (
            ds["valid_water_column"]
        )

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
        # Background-noise correction
        #
        # Keep this at transect level because ping_num=20
        # depends on temporal neighbours across RAW boundaries.
        # -----------------------------------------------------

        try:
            ds = (
                ep.clean
                .remove_background_noise(
                    ds,
                    ping_num=20,
                    range_sample_num=5,
                    SNR_threshold="5.0dB",
                )
            )

        except Exception as exc:
            print(
                f"{name}: background-noise "
                f"removal failed: {exc}"
            )

            ds["Sv_corrected"] = (
                ds["Sv"]
            )

        sv_var = (
            "Sv_corrected"
            if "Sv_corrected" in ds
            else "Sv"
        )

        # -----------------------------------------------------
        # Restrict classifier to valid water column.
        #
        # This mask was calculated per file and has already
        # been validated against the previous transect method.
        # -----------------------------------------------------

        sv_for_cps = (
            ds[sv_var].where(
                valid_water_column
            )
        )

        # -----------------------------------------------------
        # CPS classifier
        #
        # Keep rolling + dilation at transect level for now
        # because they require neighbouring pings across file
        # boundaries.
        # -----------------------------------------------------

        try:

            ds["Sv_smoothed"] = (
                sv_for_cps
                .rolling(
                    ping_time=3,
                    range_sample=11,
                )
                .mean()
            )

            ds["variance"] = (
                10 ** (
                    sv_for_cps / 10
                )
                - 10 ** (
                    ds["Sv_smoothed"] / 10
                )
            ) ** 2

            ds["variance_smoothed"] = (
                ds["variance"]
                .rolling(
                    ping_time=3,
                    range_sample=11,
                )
                .mean()
            )

            ds["variance_smoothed"] = (
                10
                * np.log10(
                    ds[
                        "variance_smoothed"
                    ]
                    ** 0.5
                )
            )

            ds["variance_smoothed"] = (
                _dilate_7x7(
                    ds[
                        "variance_smoothed"
                    ]
                )
            )

            if (
                mask_mode == "cps"
                and ds.sizes[
                    "channel"
                ] >= 4
            ):

                ch38 = (
                    _pick_channel_by_frequency(
                        ds,
                        38000,
                    )
                )

                ch70 = (
                    _pick_channel_by_frequency(
                        ds,
                        70000,
                    )
                )

                ch120 = (
                    _pick_channel_by_frequency(
                        ds,
                        120000,
                    )
                )

                ch200 = (
                    _pick_channel_by_frequency(
                        ds,
                        200000,
                    )
                )

                # ---------------------------------------------
                # Variance criteria
                # ---------------------------------------------

                sd200 = (
                    ds[
                        "variance_smoothed"
                    ].sel(
                        channel=ch200
                    )
                )

                sd120 = (
                    ds[
                        "variance_smoothed"
                    ].sel(
                        channel=ch120
                    )
                )

                mask_sd = (
                    (sd200 > -65)
                    & (sd120 > -65)
                )

                # ---------------------------------------------
                # Frequency-response criteria
                # ---------------------------------------------

                ds["Sv_dilated"] = (
                    _dilate_7x7(
                        ds["Sv_smoothed"]
                    )
                )

                diff = (
                    ds["Sv_dilated"]
                    - ds[
                        "Sv_dilated"
                    ].sel(
                        channel=ch38
                    )
                )

                mask_frequency = (
                    (
                        diff.sel(
                            channel=ch200
                        )
                        > -13.51
                    )
                    & (
                        diff.sel(
                            channel=ch200
                        )
                        < 12.53
                    )
                    & (
                        diff.sel(
                            channel=ch120
                        )
                        > -13.50
                    )
                    & (
                        diff.sel(
                            channel=ch120
                        )
                        < 9.37
                    )
                    & (
                        diff.sel(
                            channel=ch70
                        )
                        > -13.85
                    )
                    & (
                        diff.sel(
                            channel=ch70
                        )
                        < 9.89
                    )
                )

                final_mask = (
                    mask_frequency
                    & mask_sd
                    & valid_water_column
                )

            else:

                final_mask = (
                    sv_for_cps
                    > fallback_sv_threshold
                )

        except Exception as exc:

            print(
                f"{name}: CPS classifier "
                f"failed: {exc}"
            )

            final_mask = (
                sv_for_cps
                > fallback_sv_threshold
            )

        # -----------------------------------------------------
        # Apply final CPS mask
        # -----------------------------------------------------

        ds["Sv_masked"] = (
            ds["Sv"].where(
                final_mask
            )
        )

        # -----------------------------------------------------
        # Save transect CPS product
        #
        # Important: do NOT rechunk ping_time to the complete
        # transect here. Preserve the existing per-file /
        # source Dask chunk structure.
        # -----------------------------------------------------

        cps_path = (
            path_cps
            / f"{name}_CPS.zarr"
        )

        for variable in ds.variables:
            ds[
                variable
            ].encoding.pop(
                "chunks",
                None,
            )

        ds.to_zarr(
            cps_path,
            mode="w",
            consolidated=True,
        )

        # -----------------------------------------------------
        # NASC
        # -----------------------------------------------------

        ds_nasc = (
            task_compute_NASC_from_masked_Sv(
                ds_Sv_masked=ds,
                range_bin=range_bin,
                dist_bin=dist_bin,
            )
        )

        nasc_path = (
            path_nasc
            / f"{name}_nasc.zarr"
        )

        ds_nasc.to_zarr(
            nasc_path,
            mode="w",
            consolidated=True,
        )

        _export_nasc_to_echoview_csv(
            ds_nasc,
            path_nasc_csv
            / f"{name}_nasc.csv",
            process_id=nasc_process_id,
        )

        print(
            f"{name}: CPS + NASC complete"
        )

    current.to_csv(
        path_snapshot,
        index=False,
    )
