"""Select pending Sv products and orchestrate file-level CPS tasks."""

from pathlib import Path

import pandas as pd
from prefect import flow
from prefect_dask import DaskTaskRunner

from echodataflow.tasks.tasks_cps import _process_one_sv_cps as task_process_sv_cps
from echodataflow.utils.processing_ledger import (
    get_sv_files_to_process,
    initialize_ledger,
    register_sv_file,
    resolve_database,
)


@flow(log_prints=True, task_runner=DaskTaskRunner())
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
    mask_mode: str = "cps",
    fallback_sv_threshold: float = -70,
    new_file_num_limit: int = 10,
    exclude_before: str | None = None,
    remove_background_noise: bool = True,
    save_intermediates: bool = True,
):
    """Process generic Sv stores into per-file CPS-ready Sv stores."""
    path_main = Path(path_main)
    path_sv = path_main / "Sv"
    path_cps_sv = path_main / "CPS_Sv"
    path_bottom = path_main / "CPS_Seafloor_CSVs"
    for path in [path_cps_sv, path_bottom]:
        path.mkdir(parents=True, exist_ok=True)
    db_path = resolve_database(path_main, processing_db)
    initialize_ledger(db_path)
    manifest_path = path_main / file_Sv_csv
    if not manifest_path.exists():
        print(f"Sv manifest not found: {manifest_path}")
        return
    df_sv = pd.read_csv(manifest_path)
    if df_sv.empty:
        print("Sv manifest is empty.")
        return
    for column in ["first_ping_time", "last_ping_time"]:
        if column in df_sv.columns:
            df_sv[column] = pd.to_datetime(df_sv[column], utc=True, errors="coerce")
    if exclude_before is not None:
        exclude_before_dt = pd.to_datetime(exclude_before, utc=True)
        if "last_ping_time" in df_sv.columns:
            df_sv = df_sv[df_sv["last_ping_time"] >= exclude_before_dt]
    for sv_filename in df_sv["Sv_filename"].dropna():
        sv_path = path_sv / str(sv_filename)
        if not sv_path.exists():
            print(f"Sv store missing, skipping: {sv_path}")
            continue
        register_sv_file(db_path, sv_path)
    sv_files = get_sv_files_to_process(db_path, limit=new_file_num_limit)
    print(f"Found {len(sv_files)} Sv file(s) to process for CPS")
    if not sv_files:
        return
    for sv_path in sv_files:
        task_process_sv_cps(
            sv_path=str(sv_path),
            sv_filename=sv_path.name,
            db_path=str(db_path),
            path_cps_sv=str(path_cps_sv),
            path_bottom=str(path_bottom),
            target_frequency=target_frequency,
            min_depth=min_depth,
            seafloor_threshold=seafloor_threshold,
            seafloor_offset=seafloor_offset,
            seafloor_r0=seafloor_r0,
            seafloor_r1=seafloor_r1,
            seafloor_wtheta=seafloor_wtheta,
            seafloor_wphi=seafloor_wphi,
            mask_mode=mask_mode,
            fallback_sv_threshold=fallback_sv_threshold,
            remove_background_noise=remove_background_noise,
            save_intermediates=save_intermediates,
        )
