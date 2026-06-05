from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

import s3fs
import configparser
from geopy.distance import distance
import geopandas as gpd

import echopype as ep

from echodataflow.utils.const import GRID_PARAMS
from echodataflow.utils.grid import create_boundary_gdf, create_grid_from_bounds
from echodataflow.flows.flows_biology import add_stratum

from prefect import task, flow, get_run_logger
from prefect.events import emit_event


# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


# Set up paths
data_main = "/Users/feresa/code_git/echodataflow/temp_bio"
NASC_path = "nasc_zarr"


# Create boundary GeoDataFrame with UTM projection
gdf_boundary, gdf_boundary_utm, utm_num = create_boundary_gdf(
    bounds=GRID_PARAMS["bounds"],
    projection=GRID_PARAMS["projection"]
)



@task(log_prints=True)
def task_combine_NASC_to_dataframe(
    fs: s3fs.S3FileSystem,
    path_NASC_files: str,
    NASC_filenames: list,
) -> list[pd.DataFrame, list]:
    """
    Combine multiple NASC datasets into a single DataFrame.

    Parameters
    ----------
    path_NASC_files : str
        Path to the directory containing NASC zarr files.
    NASC_filenames : list
        List of NASC zarr filenames to process.
        Not the full path, just the filenames.
    """
    df_NASC_list = []
    errors = []
    for nascf in NASC_filenames:
        try:
            print(f"Processing {nascf}...")
            mapper = fs.get_mapper(str(Path(path_NASC_files) / nascf))
            ds_NASC = xr.open_zarr(mapper, consolidated=True)
            ds_NASC = ep.consolidate.swap_dims_channel_frequency(ds_NASC)
            df_NASC = ds_NASC.sum("depth").sel(frequency_nominal=38000).to_dataframe()
            df_NASC["filename"] = nascf
            df_NASC_list.append(df_NASC)
        except Exception as e:
            errors.append(e)
            print(f"Error processing {nascf}: {e}")
            continue

    if len(df_NASC_list) == 0:
        return None, None
    else:
        return pd.concat(df_NASC_list, ignore_index=True), errors


@task(log_prints=True)
def task_griddify_NASC(
    df_stratum: pd.DataFrame,
    df_NASC: pd.DataFrame,
    utm_num: int = utm_num,
    gdf_boundary_utm: gpd.GeoDataFrame = gdf_boundary_utm,
) -> gpd.GeoDataFrame:
    """
    Generate geodataframe with grid x/y containing NASC values.
    """
    # Convert to GeoDataFrame
    gdf_NASC = gpd.GeoDataFrame(
        data=df_NASC,
        geometry=gpd.points_from_xy(df_NASC["longitude"], df_NASC["latitude"]),
        crs=GRID_PARAMS["projection"],
    ).to_crs(f"epsg:{utm_num}")

    # Extract x y coordinate for pd.cut below
    gdf_NASC["utm_x"] = gdf_NASC["geometry"].x
    gdf_NASC["utm_y"] = gdf_NASC["geometry"].y

    # Get grid step sizes and boundary x/y
    x_step = distance(nautical=GRID_PARAMS["resolution"]["x_distance"]).meters
    y_step = distance(nautical=GRID_PARAMS["resolution"]["y_distance"]).meters
    xmin, ymin, xmax, ymax = gdf_boundary_utm.total_bounds

    # Bin longitude and latitude into grids
    gdf_NASC["grid_x"] = pd.cut(
        gdf_NASC["utm_x"],
        np.arange(xmin, xmax + x_step, x_step),
        right=False,
        labels=np.arange(1, len(np.arange(xmin, xmax + x_step, x_step))),
    )

    # Bin the latitude data
    gdf_NASC["grid_y"] = pd.cut(
        gdf_NASC["utm_y"],
        np.arange(ymin, ymax + y_step, y_step),
        right=True,
        labels=np.arange(1, len(np.arange(ymin, ymax + y_step, y_step))),
    )

    # Add stratum info
    gdf_NASC = add_stratum(gdf_NASC, df_stratum)

    return gdf_NASC


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
):
    """
    Ingest NASC data from zarr files and combine into a single DataFrame.
    """
    logger = get_run_logger()

    # Ensure num_NASC_reprocess is at least 1
    if num_NASC_reprocess < 1:
        num_NASC_reprocess = 1
        raise Warning("num_NASC_reprocess must be at least 1, setting it to 1.")

    # Get NASC files already processed
    file_NASC_all = Path(path_vm_local) / file_NASC_all
    df_NASC_all = (
        pd.read_csv(
            file_NASC_all,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["ping_time"]
        )
        if file_NASC_all.exists() else pd.DataFrame()
    )
    NASC_processed = (
        sorted(df_NASC_all["filename"].unique().tolist())
        if not df_NASC_all.empty else []
    )
    logger.info(f"NASC files already processed: {NASC_processed}")

    # Reprocess the last num_NASC_reprocess files
    NASC_to_reprocess = NASC_processed[-num_NASC_reprocess:]
    NASC_processed = NASC_processed[:-num_NASC_reprocess]
    logger.info(f"NASC files to reprocess: {NASC_to_reprocess}")

    # Remove df_NASC_all entries that match files to reprocess
    if not df_NASC_all.empty:
        df_NASC_all = df_NASC_all[~df_NASC_all["filename"].isin(NASC_to_reprocess)]

    # Get cloud bucket
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    # Get all NASC files in the bucket
    NASC_all = fs.glob(f"{path_NASC_files}/*.zarr")
    NASC_all = sorted([Path(f).name for f in NASC_all])
    logger.info(f"All NASC files: {NASC_all}")

    # Determine which files to process
    NASC_to_process = sorted(list(set(NASC_all).difference(set(NASC_processed))))

    if len(NASC_to_process) == 0:
        logger.info("No new NASC files to process.")
        return
    else:

        # Limit number of new files to process
        if new_file_num_limit != -1 and len(NASC_to_process) > new_file_num_limit:
            print(
                f"More than {new_file_num_limit} new files to process. "
                f"Limiting to first {new_file_num_limit} files."
            )
            NASC_to_process = NASC_to_process[:new_file_num_limit]

        # Print list of files to process
        files_to_process = ""
        for nascf in NASC_to_process:
            files_to_process += f"- {nascf}\n"
        logger.info(f"Files to process:\n{files_to_process}")

        # Combine all unprocessed NASC datasets into a single DataFrame
        df_NASC, errors = task_combine_NASC_to_dataframe(fs, path_NASC_files, NASC_to_process)
        if df_NASC is not None:
            logger.info(f"df_NASC contains: {list(df_NASC["filename"].unique())}")

        # Append new NASC data to existing data
        df_NASC_all = pd.concat([df_NASC_all, df_NASC], ignore_index=True)
        if df_NASC_all is not None:
            logger.info(f"after run df_NASC_all contains: {list(df_NASC_all["filename"].unique())}")

        # Sort by ping_time
        df_NASC_all.sort_values(by="ping_time", inplace=True)

        # Load stratum means
        df_stratum = pd.read_csv(Path(path_vm_local) / file_stratum_mean, index_col=0)

        # Drop previous stratification to avoid merge conflicts
        df_NASC_all = df_NASC_all.drop(
            ["stratum", "sigma_bs_mean", "weight_mean"],
            axis=1, errors='ignore'
        )

        # Add stratum info
        df_NASC_all = add_stratum(df_NASC_all, df_stratum)
        df_NASC_all["stratum"] = df_NASC_all["stratum"].astype("Int64")  # convert from category to int

        # Merge on stratum
        df_NASC_all = df_NASC_all.merge(
            df_stratum[["stratum", "sigma_bs_mean", "weight_mean"]],
            on="stratum",  # merge on stratum
            how="left"
        )

        # Compute stratified number density from NASC
        df_NASC_all["number_density"] = df_NASC_all["NASC"] / df_NASC_all["sigma_bs_mean"]

        # Compute biomass density from number density
        df_NASC_all["biomass_density"] = df_NASC_all["number_density"] * df_NASC_all["weight_mean"]

        # Save to combined NASC data to csv
        df_NASC_all.to_csv(file_NASC_all)

        # Griddify the NASC data
        gdf_NASC = task_griddify_NASC(
            df_stratum=df_stratum,
            df_NASC=df_NASC_all,
            utm_num=utm_num,
            gdf_boundary_utm=gdf_boundary_utm
        )
        gdf_NASC.to_file(Path(path_vm_local) / file_NASC_all_grid, driver="GeoJSON")

        # Emit custom event when new NASC files are processed
        emit_event(
            event="nasc.ingested",
            resource={"prefect.resource.id": "ingest_NASC"}
        )

        # TODO: add error handling


@flow(log_prints=True)
def flow_update_grid(
    path_vm_local: str = "LOCAL_PATH_TO_INTEGRATED_DATA",
    file_NASC_all_grid: str = "NASC_all_griddify.geojson",
    file_stratum_mean: str = "stratum_mean.csv",
):
    """
    Update the grid with the latest NASC data and stratum means from hauls.
    """
    # Assemble full paths
    file_NASC_all_grid = Path(path_vm_local) / file_NASC_all_grid
    file_stratum_mean = Path(path_vm_local) / file_stratum_mean

    # Load griddified NASC and grid cells
    gdf_NASC = gpd.read_file(file_NASC_all_grid)

    # Load stratum means
    df_stratum = pd.read_csv(file_stratum_mean, index_col=0)

    # Create gdf_grid_cells
    gdf_grid_cells, _, _ = create_grid_from_bounds(
        bounds=GRID_PARAMS["bounds"],
        resolution=GRID_PARAMS["resolution"],
        projection=GRID_PARAMS["projection"],
        coastline_resolution="10m",
        area_threshold=5
    )
    gdf_grid_cells.set_index(["grid_x", "grid_y"], inplace=True)

    # Drop sigma_bs_mean and weight_mean to avoid merge conflicts
    gdf_NASC = gdf_NASC.drop(
        ["sigma_bs_mean", "weight_mean"],
        axis=1, errors='ignore'
    )

    # Merge on stratum
    gdf_NASC = gdf_NASC.merge(
        df_stratum[["stratum", "sigma_bs_mean", "weight_mean"]],
        on="stratum",  # merge on stratum
        how="left"
    )

    # Compute stratified number density from NASC
    gdf_NASC["number_density"] = gdf_NASC["NASC"] / gdf_NASC["sigma_bs_mean"]

    # Compute biomass density from number density
    gdf_NASC["biomass_density"] = gdf_NASC["number_density"] * gdf_NASC["weight_mean"]

    # Merge with grid cells
    gdf_grid_cells = pd.merge(
        gdf_grid_cells,
        gdf_NASC.groupby(["grid_x", "grid_y"])["NASC"].mean(),
        on=["grid_x", "grid_y"],
        how="left"
    )
    gdf_grid_cells = pd.merge(
        gdf_grid_cells,
        gdf_NASC.groupby(["grid_x", "grid_y"])["number_density"].mean(),
        on=["grid_x", "grid_y"],
        how="left"
    )
    gdf_grid_cells = pd.merge(
        gdf_grid_cells,
        gdf_NASC.groupby(["grid_x", "grid_y"])["biomass_density"].mean(),
        on=["grid_x", "grid_y"],
        how="left"
    )

    # Compute abundance and biomass
    gdf_grid_cells["abundance"] = gdf_grid_cells["number_density"] * gdf_grid_cells["area"]
    gdf_grid_cells["biomass"] = gdf_grid_cells["biomass_density"] * gdf_grid_cells["area"]

    # Save updated grid cells
    gdf_grid_cells.to_file(Path(path_vm_local) / "grid_cells.geojson", driver="GeoJSON")



# @flow(log_prints=True)
# def flow_test_trigger(trigger = True):
#     if not trigger:
#         return
#     else:
#         # Emit event when new NASC files are processed
#         emit_event(
#             event="test.processed",
#             resource={"prefect.resource.id": "test_trigger"}
#         )

# @flow(log_prints=True)
# def flow_to_be_trigger():
#     # Emit event when new NASC files are processed
#     logger = get_run_logger()
#     logger.info("This flow is triggered by an event emission.")
