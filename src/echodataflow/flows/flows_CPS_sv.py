from pathlib import Path

import echopype as ep
import pandas as pd
import xarray as xr
from prefect import flow
from prefect_dask import DaskTaskRunner

from echodataflow.flows.cps_helpers import (
    _mask_above_seafloor,
    _pick_channel_by_frequency,
)
from echodataflow.utils.processing_ledger import (
    get_sv_files_to_process,
    initialize_ledger,
    mark_sv_cps_completed,
    mark_sv_cps_failed,
    mark_sv_cps_processing,
    register_sv_file,
    resolve_database,
)


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(),
)
def flow_process_Sv_CPS(
    path_main: str,
    file_Sv_csv: str = "Sv_files.csv",
    processing_db: str = "processing.db",
    target_frequency: float = 70000,
    min_depth: float = 10.0,
    seafloor_threshold: list = [-40, 2.4, 1.0],
    seafloor_offset: float = 0.5,
    seafloor_r0: float = 10,
    seafloor_r1: float = 1000,
    seafloor_wtheta: int = 28,
    seafloor_wphi: int = 52,
    new_file_num_limit: int = 10,
    exclude_before: str | None = None,
):
    """Process generic Sv stores into per-file CPS-ready Sv stores."""

    path_main = Path(path_main)

    path_sv = path_main / "Sv"
    path_cps_sv = path_main / "CPS_Sv"
    path_bottom = path_main / "CPS_Seafloor_CSVs"

    for path in [
        path_cps_sv,
        path_bottom,
    ]:
        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    db_path = resolve_database(
        path_main,
        processing_db,
    )
    initialize_ledger(db_path)

    # ---------------------------------------------------------
    # Reconcile generic Sv manifest with CPS processing ledger
    # ---------------------------------------------------------

    manifest_path = path_main / file_Sv_csv

    if not manifest_path.exists():
        print(
            f"Sv manifest not found: {manifest_path}"
        )
        return

    df_sv = pd.read_csv(
        manifest_path,
    )

    if df_sv.empty:
        print("Sv manifest is empty.")
        return

    for column in [
        "first_ping_time",
        "last_ping_time",
    ]:
        if column in df_sv.columns:
            df_sv[column] = pd.to_datetime(
                df_sv[column],
                utc=True,
                errors="coerce",
            )

    if exclude_before is not None:
        exclude_before_dt = pd.to_datetime(
            exclude_before,
            utc=True,
        )

        if "last_ping_time" in df_sv.columns:
            df_sv = df_sv[
                df_sv["last_ping_time"]
                >= exclude_before_dt
            ]

    for sv_filename in df_sv[
        "Sv_filename"
    ].dropna():

        sv_path = (
            path_sv / str(sv_filename)
        )

        if not sv_path.exists():
            print(
                f"Sv store missing, skipping: "
                f"{sv_path}"
            )
            continue

        register_sv_file(
            db_path,
            sv_path,
        )

    # ---------------------------------------------------------
    # Select pending / failed Sv stores
    # ---------------------------------------------------------

    sv_files = get_sv_files_to_process(
        db_path,
        limit=new_file_num_limit,
    )

    print(
        f"Found {len(sv_files)} Sv file(s) "
        f"to process for CPS"
    )

    if not sv_files:
        return

    # ---------------------------------------------------------
    # Process one Sv store at a time
    # ---------------------------------------------------------

    for sv_path in sv_files:

        name = sv_path.name.removesuffix(
            "_Sv.zarr"
        )

        cps_filename = (
            f"{name}_CPS.zarr"
        )

        cps_path = (
            path_cps_sv
            / cps_filename
        )

        try:
            print(
                f"Processing CPS Sv: "
                f"{sv_path.name}"
            )

            mark_sv_cps_processing(
                db_path,
                sv_path,
            )

            # -------------------------------------------------
            # Open generic Sv product
            # -------------------------------------------------

            ds = xr.open_zarr(
                sv_path,
                consolidated=True,
            )

            target_channel = (
                _pick_channel_by_frequency(
                    ds,
                    target_frequency,
                )
            )

            print(
                f"Target frequency: "
                f"{target_frequency / 1000:.0f} kHz"
            )

            print(
                f"Target channel: "
                f"{target_channel}"
            )

            # -------------------------------------------------
            # Match acoustic geometry across frequencies
            #
            # resample_to_geometry requires its range core
            # dimension to be contained in a single Dask chunk.
            #
            # This is bounded here because we process one Sv
            # file at a time rather than an entire transect.
            # -------------------------------------------------

            ds_geometry = ds.chunk(
                {
                    "range_sample": -1,
                }
            )

            aligned = (
                ep.commongrid.resample_to_geometry(
                    ds_geometry,
                    target_variable="Sv",
                    target_channel=target_channel,
                )
            )

            # Preserve absorption if resample_to_geometry does
            # not carry it into the aligned dataset.
            if "sound_absorption" in ds:
                aligned[
                    "sound_absorption"
                ] = ds[
                    "sound_absorption"
                ]

            # Recompute depth on the aligned geometry.
            aligned = (
                ep.consolidate.add_depth(
                    aligned
                )
            )

            # Replace acoustic variables in the original
            # dataset with the common-geometry versions.
            ds[
                [
                    "Sv",
                    "echo_range",
                ]
            ] = aligned[
                [
                    "Sv",
                    "echo_range",
                ]
            ]

            ds = (
                ep.consolidate.add_depth(
                    ds
                )
            )

            print(
                f"{sv_path.name}: "
                f"geometry alignment complete"
            )

            # -------------------------------------------------
            # Detect seafloor with Blackwell
            # -------------------------------------------------

            bottom_path = None

            try:
                bottom = (
                    ep.mask.detect_seafloor(
                        ds=ds,
                        method="blackwell",
                        params={
                            "channel": (
                                target_channel
                            ),
                            "var_name": "Sv",
                            "threshold": (
                                seafloor_threshold
                            ),
                            "offset": (
                                seafloor_offset
                            ),
                            "r0": (
                                seafloor_r0
                            ),
                            "r1": (
                                seafloor_r1
                            ),
                            "wtheta": (
                                seafloor_wtheta
                            ),
                            "wphi": (
                                seafloor_wphi
                            ),
                        },
                    )
                )

                bottom_df = pd.DataFrame(
                    {
                        "time": (
                            bottom[
                                "ping_time"
                            ].values
                        ),
                        "depth": (
                            bottom.values
                        ),
                    }
                )

                bottom_df = (
                    bottom_df[
                        bottom_df[
                            "depth"
                        ] > -0.2
                    ]
                )

                bottom_path = (
                    path_bottom
                    / (
                        f"{name}"
                        f"_bottom_line.csv"
                    )
                )

                bottom_df.to_csv(
                    bottom_path,
                    index=False,
                )

                print(
                    f"{sv_path.name}: "
                    f"seafloor detection complete"
                )

            except Exception as exc:
                print(
                    f"{sv_path.name}: "
                    f"seafloor detection failed: "
                    f"{exc}"
                )

            # -------------------------------------------------
            # Build valid water-column mask
            #
            # 1. Exclude surface <= min_depth
            # 2. Exclude seafloor and everything below
            # -------------------------------------------------

            target_depth = (
                ds[
                    "depth"
                ].sel(
                    channel=target_channel
                )
            )

            surface_mask = (
                target_depth
                > min_depth
            )

            # If seafloor masking fails, keep everything below
            # min_depth rather than failing the entire Sv file.
            above_seafloor_mask = (
                xr.ones_like(
                    surface_mask,
                    dtype=bool,
                )
            )

            if bottom_path is not None:
                try:
                    above_seafloor_mask = (
                        _mask_above_seafloor(
                            ds,
                            bottom_path,
                            target_channel,
                        )
                    )

                except Exception as exc:
                    print(
                        f"{sv_path.name}: "
                        f"seafloor mask failed: "
                        f"{exc}"
                    )

            valid_water_column = (
                surface_mask
                & above_seafloor_mask
            )

            # Save diagnostic masks for now.
            ds[
                "surface_mask"
            ] = surface_mask

            ds[
                "above_seafloor_mask"
            ] = above_seafloor_mask

            ds[
                "valid_water_column"
            ] = valid_water_column

            ds[
                "Sv_water_column"
            ] = (
                ds[
                    "Sv"
                ].where(
                    valid_water_column
                )
            )

            print(
                f"{sv_path.name}: "
                f"water-column mask complete"
            )

            # -------------------------------------------------
            # Write per-file CPS-ready Sv product
            #
            # Do not force old transect-wide chunking here.
            # -------------------------------------------------

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

            # -------------------------------------------------
            # Record output in CPS processing ledger
            # -------------------------------------------------

            first_ping_time = (
                pd.to_datetime(
                    ds[
                        "ping_time"
                    ][0].values,
                    utc=True,
                )
            )

            last_ping_time = (
                pd.to_datetime(
                    ds[
                        "ping_time"
                    ][-1].values,
                    utc=True,
                )
            )

            mark_sv_cps_completed(
                db_path,
                sv_path,
                cps_filename,
                first_ping_time,
                last_ping_time,
            )

            print(
                f"{sv_path.name}: "
                f"CPS Sv processing complete"
            )

        except Exception as exc:

            mark_sv_cps_failed(
                db_path,
                sv_path,
                str(exc),
            )

            print(
                f"{sv_path.name}: "
                f"CPS processing failed: "
                f"{exc}"
            )

            raise
