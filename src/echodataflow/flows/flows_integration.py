from __future__ import annotations

import configparser
from pathlib import Path

import geopandas as gpd
import pandas as pd
import s3fs
from prefect import flow, get_run_logger
from prefect.futures import as_completed

from echodataflow.operations.operations_integration import (
    ReadNASCSettings,
    ReadNASCWorkItem,
    add_biological_estimates,
    aggregate_NASC_to_grid,
    assign_NASC_to_grid,
    plan_NASC_ingestion,
    read_accumulated_NASC,
    write_NASC_outputs,
)
from echodataflow.tasks.tasks_integration import task_read_NASC
from echodataflow.utils.const import GRID_PARAMS
from echodataflow.utils.grid import create_boundary_gdf, create_grid_from_bounds


@flow(log_prints=True)
def flow_ingest_NASC(
    path_vm_local: str = "LOCAL_PATH_TO_INTEGRATED_DATA",
    path_NASC_files: str = "NASC_ZARR_CLOUD_LOCATION",
    cred_file: str = "CREDENTIAL_FILE",
    file_NASC_all: str = "NASC_all.csv",
    file_stratum_mean: str = "stratum_mean.csv",
    file_NASC_all_grid: str = "NASC_all_griddify.geojson",
    num_NASC_reprocess: int = 1,
    new_file_num_limit: int = 50,
    frequency_nominal: int = 38000,
):
    """Ingest cloud NASC stores and integrate them with biological estimates."""
    logger = get_run_logger()
    if num_NASC_reprocess < 1:
        raise ValueError("num_NASC_reprocess must be at least 1")
    if new_file_num_limit < -1:
        raise ValueError("new_file_num_limit must be -1 or non-negative")

    file_NASC_all = Path(path_vm_local) / file_NASC_all
    file_stratum_mean = Path(path_vm_local) / file_stratum_mean
    file_NASC_all_grid = Path(path_vm_local) / file_NASC_all_grid

    # Wait for biological estimates from the upstream haul-ingestion flow
    if not file_stratum_mean.exists():
        logger.info(f"Upstream stratum estimates are not ready: {file_stratum_mean}")
        return

    # Read accumulated data before planning new work
    df_NASC_existing = read_accumulated_NASC(file_NASC_all)
    NASC_processed = (
        sorted(df_NASC_existing["filename"].unique()) if not df_NASC_existing.empty else []
    )
    logger.info(f"NASC files already processed: {NASC_processed}")

    # Connect to the S3-compatible NASC data store
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    NASC_available = sorted(
        Path(filename).name for filename in fs.glob(f"{path_NASC_files}/*.zarr")
    )
    logger.info(f"All NASC files: {NASC_available}")
    NASC_to_process, NASC_to_replace = plan_NASC_ingestion(
        NASC_available,
        NASC_processed,
        num_NASC_reprocess,
        new_file_num_limit,
    )
    if not NASC_to_process:
        logger.info("No new NASC files to process")
        return

    # Remove only rows for reprocessing files selected in this limited batch
    if NASC_to_replace:
        df_NASC_existing = df_NASC_existing[
            ~df_NASC_existing["filename"].isin(NASC_to_replace)
        ].copy()
    logger.info(f"NASC files to replace: {NASC_to_replace}")
    logger.info("Files to process:\n" + "".join(f"- {name}\n" for name in NASC_to_process))

    # Submit one task per store for independent retries and failure visibility
    settings = ReadNASCSettings(
        filesystem=fs,
        input_directory=path_NASC_files,
        frequency_nominal=frequency_nominal,
    )
    future_to_filename = {
        task_read_NASC.with_options(
            task_run_name=filename,
            name=filename,
            retries=3,
        ).submit(ReadNASCWorkItem(filename=filename), settings): filename
        for filename in NASC_to_process
    }

    successful_dataframes = []
    errors = []
    for future in as_completed(future_to_filename):
        filename = future_to_filename[future]
        try:
            successful_dataframes.append(future.result())
        except Exception as error:
            errors.append((filename, error))
            logger.error(f"Failed to ingest {filename}: {error}")

    # Publish successful files even when another platform's store failed
    if not successful_dataframes:
        failed = ", ".join(filename for filename, _ in errors)
        raise RuntimeError(f"All NASC files failed ingestion: {failed}")

    df_NASC_new = pd.concat(successful_dataframes, ignore_index=True)
    df_NASC_all = (
        pd.concat([df_NASC_existing, df_NASC_new], ignore_index=True)
        if not df_NASC_existing.empty
        else df_NASC_new.copy()
    )
    df_NASC_all.sort_values("ping_time", inplace=True)

    # Refresh biological estimates using the latest accumulated haul results
    df_stratum = pd.read_csv(file_stratum_mean, index_col=0)
    df_NASC_all = add_biological_estimates(df_NASC_all, df_stratum)

    # Assign each integrated observation to the configured survey grid
    _, gdf_boundary_utm, utm_num = create_boundary_gdf(
        bounds=GRID_PARAMS["bounds"],
        projection=GRID_PARAMS["projection"],
    )
    gdf_NASC = assign_NASC_to_grid(
        df_stratum,
        df_NASC_all,
        utm_num,
        gdf_boundary_utm,
    )
    write_NASC_outputs(
        df_NASC_all,
        gdf_NASC,
        file_NASC_all,
        file_NASC_all_grid,
    )

    # Mark the flow failed if any error occurred
    if errors:
        failed = ", ".join(filename for filename, _ in errors)
        raise RuntimeError(f"Failed to ingest {len(errors)} NASC files: {failed}")


@flow(log_prints=True)
def flow_update_grid(
    path_vm_local: str = "LOCAL_PATH_TO_INTEGRATED_DATA",
    file_NASC_all_grid: str = "NASC_all_griddify.geojson",
    file_stratum_mean: str = "stratum_mean.csv",
):
    """Update grid-cell estimates from integrated NASC and haul biology."""
    logger = get_run_logger()
    file_NASC_all_grid = Path(path_vm_local) / file_NASC_all_grid
    file_stratum_mean = Path(path_vm_local) / file_stratum_mean

    # Wait until both upstream integration products are available
    missing_inputs = [path for path in [file_NASC_all_grid, file_stratum_mean] if not path.exists()]
    if missing_inputs:
        logger.info(
            "Upstream grid inputs are not ready: " + ", ".join(str(path) for path in missing_inputs)
        )
        return

    # Refresh estimates on existing observations without changing their grid assignment
    gdf_NASC_all_grid = gpd.read_file(file_NASC_all_grid)
    df_stratum = pd.read_csv(file_stratum_mean, index_col=0)
    gdf_NASC_all_grid = add_biological_estimates(
        gdf_NASC_all_grid,
        df_stratum,
        reassign_strata=False,
    )

    # Create the complete survey grid and aggregate observation densities into cells
    gdf_grid_cells, _, _ = create_grid_from_bounds(
        bounds=GRID_PARAMS["bounds"],
        resolution=GRID_PARAMS["resolution"],
        projection=GRID_PARAMS["projection"],
        coastline_resolution="10m",
        area_threshold=5,
    )
    gdf_grid_cells = aggregate_NASC_to_grid(gdf_NASC_all_grid, gdf_grid_cells)
    gdf_grid_cells.to_file(Path(path_vm_local) / "grid_cells.geojson", driver="GeoJSON")
