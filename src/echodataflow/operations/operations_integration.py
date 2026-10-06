"""Operations for integrating acoustic and biological survey products."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import echopype as ep
import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from geopy.distance import distance

from echodataflow.operations.operations_stratification import assign_stratum
from echodataflow.utils.const import GRID_PARAMS


@dataclass(frozen=True)
class ReadNASCWorkItem:
    """One NASC store to ingest."""

    filename: str


@dataclass(frozen=True)
class ReadNASCSettings:
    """Settings shared by a batch of NASC reads."""

    filesystem: s3fs.S3FileSystem
    input_directory: str
    frequency_nominal: int = 38000


def read_NASC(item: ReadNASCWorkItem, settings: ReadNASCSettings) -> pd.DataFrame:
    """Read and normalize one NASC Zarr store."""
    mapper = settings.filesystem.get_mapper(str(Path(settings.input_directory) / item.filename))
    ds_NASC = xr.open_zarr(mapper, consolidated=True)
    ds_NASC = ep.consolidate.swap_dims_channel_frequency(ds_NASC)

    # Integrate over depth and retain the survey's primary acoustic frequency
    df_NASC = ds_NASC.sum("depth").sel(frequency_nominal=settings.frequency_nominal).to_dataframe()
    df_NASC["filename"] = item.filename
    return df_NASC


def read_accumulated_NASC(path: Path) -> pd.DataFrame:
    """Read accumulated NASC data or return an empty dataframe on the first run."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(
        path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["ping_time"],
    )


def plan_NASC_ingestion(
    available_filenames: list[str],
    processed_filenames: list[str],
    num_reprocess: int,
    num_file_limit: int,
) -> tuple[list[str], list[str]]:
    """Select files to process and processed files whose rows should be replaced."""
    if num_reprocess < 1:
        raise ValueError("num_reprocess must be at least 1")
    if num_file_limit < -1:
        raise ValueError("file_limit must be -1 or non-negative")

    available = set(available_filenames)
    reprocess_candidates = [
        filename for filename in processed_filenames[-num_reprocess:] if filename in available
    ]
    new_candidates = sorted(available.difference(processed_filenames))

    # Prioritize explicitly requested reprocessing before previously unseen files
    candidates = reprocess_candidates + [
        filename for filename in new_candidates if filename not in reprocess_candidates
    ]
    selected = candidates if num_file_limit == -1 else candidates[:num_file_limit]
    replacements = [filename for filename in reprocess_candidates if filename in selected]
    return selected, replacements


def write_NASC_outputs(
    df_NASC: pd.DataFrame,
    gdf_NASC: gpd.GeoDataFrame,
    dataframe_path: Path,
    grid_path: Path,
) -> None:
    """Stage and save integrated NASC CSV and GeoJSON outputs together."""
    transaction_id = uuid4().hex
    outputs = {
        "dataframe": dataframe_path,
        "grid": grid_path,
    }
    staged = {
        key: path.with_name(f".{path.name}.{transaction_id}.tmp") for key, path in outputs.items()
    }
    backups: dict[str, Path] = {}
    installed: list[str] = []

    try:
        # Finish both serializations before replacing either published output
        for path in outputs.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        df_NASC.to_csv(staged["dataframe"])
        gdf_NASC.to_file(staged["grid"], driver="GeoJSON")

        # Preserve existing outputs so publication can be rolled back
        for key, target in outputs.items():
            if target.exists():
                backup = target.with_name(f".{target.name}.{transaction_id}.bak")
                os.replace(target, backup)
                backups[key] = backup
            os.replace(staged[key], target)
            installed.append(key)
    except Exception:
        rollback_errors = []
        for key in reversed(installed):
            try:
                outputs[key].unlink(missing_ok=True)
            except OSError as error:
                rollback_errors.append(error)
        for key, backup in backups.items():
            try:
                os.replace(backup, outputs[key])
            except OSError as error:
                rollback_errors.append(error)
        if rollback_errors:
            raise RuntimeError(
                "Integration output update failed and could not be fully rolled back"
            ) from rollback_errors[0]
        raise
    else:
        for backup in backups.values():
            backup.unlink(missing_ok=True)
    finally:
        for temporary in staged.values():
            temporary.unlink(missing_ok=True)


def add_biological_estimates(
    df_NASC: pd.DataFrame,
    df_stratum: pd.DataFrame,
    reassign_strata: bool = True,
) -> pd.DataFrame:
    """Attach stratum estimates and derive number and biomass density."""
    # Remove old estimates so updated haul biology can be merged without conflicts
    result = df_NASC.drop(
        ["sigma_bs_mean", "weight_mean"],
        axis=1,
        errors="ignore",
    )
    if reassign_strata:
        result = result.drop("stratum", axis=1, errors="ignore")
        result = assign_stratum(result, df_stratum)
    result["stratum"] = result["stratum"].astype("Int64")
    result = result.merge(
        df_stratum[["stratum", "sigma_bs_mean", "weight_mean"]],
        on="stratum",
        how="left",
    )

    # Convert acoustic density to organism number and biomass density
    result["number_density"] = result["NASC"] / result["sigma_bs_mean"]
    result["biomass_density"] = result["number_density"] * result["weight_mean"]
    return result


def assign_NASC_to_grid(
    df_stratum: pd.DataFrame,
    df_NASC: pd.DataFrame,
    utm_num: int,
    gdf_boundary_utm: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Add projected coordinates, grid indices, and strata to NASC observations."""
    # Convert NASC positions to the grid's UTM projection
    gdf_NASC = gpd.GeoDataFrame(
        data=df_NASC,
        geometry=gpd.points_from_xy(df_NASC["longitude"], df_NASC["latitude"]),
        crs=GRID_PARAMS["projection"],
    ).to_crs(f"epsg:{utm_num}")

    # Extract projected coordinates for grid binning
    gdf_NASC["utm_x"] = gdf_NASC["geometry"].x
    gdf_NASC["utm_y"] = gdf_NASC["geometry"].y

    # Convert configured nautical-mile resolution to projected meter spacing
    x_step = distance(nautical=GRID_PARAMS["resolution"]["x_distance"]).meters
    y_step = distance(nautical=GRID_PARAMS["resolution"]["y_distance"]).meters
    xmin, ymin, xmax, ymax = gdf_boundary_utm.total_bounds

    # Bin longitude and latitude into numbered grid cells
    x_bins = np.arange(xmin, xmax + x_step, x_step)
    y_bins = np.arange(ymin, ymax + y_step, y_step)
    gdf_NASC["grid_x"] = pd.cut(
        gdf_NASC["utm_x"],
        x_bins,
        right=False,
        labels=np.arange(1, len(x_bins)),
    )
    gdf_NASC["grid_y"] = pd.cut(
        gdf_NASC["utm_y"],
        y_bins,
        right=True,
        labels=np.arange(1, len(y_bins)),
    )

    # Add biological strata using observation latitude
    return assign_stratum(gdf_NASC, df_stratum)


def aggregate_NASC_to_grid(
    gdf_NASC: gpd.GeoDataFrame,
    gdf_grid_cells: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Aggregate NASC densities and derive abundance and biomass by grid cell."""
    grid_cells = gdf_grid_cells.copy().set_index(["grid_x", "grid_y"])
    grid_means = (
        gdf_NASC.groupby(["grid_x", "grid_y"], observed=True)[
            ["NASC", "number_density", "biomass_density"]
        ]
        .mean()
        .reset_index()
    )
    grid_cells = grid_cells.merge(
        grid_means,
        on=["grid_x", "grid_y"],
        how="left",
    )

    # Scale mean densities by cell area to obtain cell totals
    grid_cells["abundance"] = grid_cells["number_density"] * grid_cells["area"]
    grid_cells["biomass"] = grid_cells["biomass_density"] * grid_cells["area"]
    return grid_cells
