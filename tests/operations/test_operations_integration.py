from pathlib import Path

import geopandas as gpd
import pandas as pd
import pandas.testing as pdt
from shapely.geometry import Point

from echodataflow.operations import operations_integration
from echodataflow.utils.const import GRID_PARAMS
from echodataflow.utils.grid import create_boundary_gdf


class FakeFileSystem:
    def get_mapper(self, path):
        return path


class FakeNASCDataset:
    def __init__(self):
        self.selection = None

    def sum(self, dimension):
        assert dimension == "depth"
        return self

    def sel(self, **selection):
        self.selection = selection
        return self

    def to_dataframe(self):
        return pd.DataFrame({"NASC": [1.0]})


def test_read_NASC_returns_dataframe_with_source_filename(monkeypatch):
    dataset = FakeNASCDataset()
    monkeypatch.setattr(
        operations_integration.xr,
        "open_zarr",
        lambda mapper, consolidated: dataset,
    )
    monkeypatch.setattr(
        operations_integration.ep.consolidate,
        "swap_dims_channel_frequency",
        lambda value: value,
    )
    settings = operations_integration.ReadNASCSettings(
        filesystem=FakeFileSystem(),
        input_directory="bucket/nasc",
        frequency_nominal=120000,
    )

    result = operations_integration.read_NASC(
        operations_integration.ReadNASCWorkItem(filename="first.zarr"),
        settings,
    )

    assert result["filename"].tolist() == ["first.zarr"]
    assert dataset.selection == {"frequency_nominal": 120000}


def test_plan_NASC_ingestion_prioritizes_reprocessing_within_limit():
    selected, replacements = operations_integration.plan_NASC_ingestion(
        available_filenames=["001.zarr", "002.zarr", "003.zarr", "004.zarr"],
        processed_filenames=["001.zarr", "002.zarr", "003.zarr"],
        num_reprocess=2,
        num_file_limit=2,
    )

    assert selected == ["002.zarr", "003.zarr"]
    assert replacements == ["002.zarr", "003.zarr"]


def test_plan_NASC_ingestion_replaces_only_selected_reprocessing_files():
    selected, replacements = operations_integration.plan_NASC_ingestion(
        available_filenames=["001.zarr", "002.zarr", "003.zarr", "004.zarr"],
        processed_filenames=["001.zarr", "002.zarr", "003.zarr"],
        num_reprocess=2,
        num_file_limit=1,
    )

    assert selected == ["002.zarr"]
    assert replacements == ["002.zarr"]


def test_integration_outputs_round_trip(tmp_path):
    dataframe_path = tmp_path / "NASC_all.csv"
    grid_path = tmp_path / "NASC_grid.geojson"
    expected = pd.DataFrame(
        {
            "ping_time": pd.to_datetime(["2026-01-01T00:00:00"]),
            "NASC": [1.0],
        }
    )
    expected_grid = gpd.GeoDataFrame(
        {"NASC": [1.0]},
        geometry=[Point(-125.0, 40.0)],
        crs="EPSG:4326",
    )

    operations_integration.write_NASC_outputs(
        expected,
        expected_grid,
        dataframe_path,
        grid_path,
    )
    result = operations_integration.read_accumulated_NASC(dataframe_path)
    result_grid = gpd.read_file(grid_path)

    pdt.assert_frame_equal(result, expected)
    assert result_grid["NASC"].tolist() == [1.0]
    assert not list(tmp_path.glob(".*.tmp"))
    assert not list(tmp_path.glob(".*.bak"))


def test_integration_outputs_restore_both_outputs_on_publish_failure(tmp_path, monkeypatch):
    dataframe_path = tmp_path / "NASC_all.csv"
    grid_path = tmp_path / "NASC_grid.geojson"
    original = pd.DataFrame({"ping_time": ["2026-01-01"], "NASC": [1.0]})
    replacement = pd.DataFrame({"ping_time": ["2026-01-02"], "NASC": [2.0]})
    original_grid = gpd.GeoDataFrame(
        {"NASC": [1.0]}, geometry=[Point(-125.0, 40.0)], crs="EPSG:4326"
    )
    replacement_grid = gpd.GeoDataFrame(
        {"NASC": [2.0]}, geometry=[Point(-124.0, 41.0)], crs="EPSG:4326"
    )
    operations_integration.write_NASC_outputs(
        original, original_grid, dataframe_path, grid_path
    )

    real_replace = operations_integration.os.replace
    failed = False

    def fail_grid_publication(source, destination):
        nonlocal failed
        source = Path(source)
        destination = Path(destination)
        if not failed and source.suffix == ".tmp" and destination == grid_path:
            failed = True
            raise OSError("publish failed")
        return real_replace(source, destination)

    monkeypatch.setattr(operations_integration.os, "replace", fail_grid_publication)

    try:
        operations_integration.write_NASC_outputs(
            replacement,
            replacement_grid,
            dataframe_path,
            grid_path,
        )
    except OSError as error:
        assert str(error) == "publish failed"
    else:
        raise AssertionError("Expected grid publication to fail")

    assert pd.read_csv(dataframe_path, index_col=0)["NASC"].tolist() == [1.0]
    assert gpd.read_file(grid_path)["NASC"].tolist() == [1.0]


def test_add_biological_estimates_replaces_previous_values():
    nasc = pd.DataFrame(
        {
            "latitude": [40.0],
            "NASC": [20.0],
            "stratum": [99],
            "sigma_bs_mean": [999.0],
            "weight_mean": [999.0],
        }
    )
    strata = pd.DataFrame(
        {
            "stratum": [1],
            "latitude_northern_limit": [45.0],
            "sigma_bs_mean": [2.0],
            "weight_mean": [3.0],
        }
    )

    result = operations_integration.add_biological_estimates(nasc, strata)

    assert result.iloc[0]["stratum"] == 1
    assert result.iloc[0]["number_density"] == 10.0
    assert result.iloc[0]["biomass_density"] == 30.0


def test_assign_NASC_to_grid_assigns_grid_coordinates_and_strata():
    _, boundary_utm, utm_num = create_boundary_gdf(
        bounds=GRID_PARAMS["bounds"],
        projection=GRID_PARAMS["projection"],
    )
    nasc = pd.DataFrame({"longitude": [-125.0], "latitude": [40.0], "NASC": [1.0]})
    strata = pd.DataFrame(
        {
            "stratum": [1, 2],
            "latitude_northern_limit": [45.0, 50.0],
        }
    )

    gridded = operations_integration.assign_NASC_to_grid(
        strata,
        nasc,
        utm_num,
        boundary_utm,
    )

    assert gridded["grid_x"].notna().all()
    assert gridded["grid_y"].notna().all()
    assert gridded["stratum"].astype(int).tolist() == [1]


def test_aggregate_NASC_to_grid_computes_cell_means_and_totals():
    observations = gpd.GeoDataFrame(
        {
            "grid_x": [1, 1],
            "grid_y": [2, 2],
            "NASC": [10.0, 30.0],
            "number_density": [2.0, 6.0],
            "biomass_density": [4.0, 12.0],
        }
    )
    cells = gpd.GeoDataFrame({"grid_x": [1], "grid_y": [2], "area": [5.0]})

    result = operations_integration.aggregate_NASC_to_grid(observations, cells)

    assert result.iloc[0]["NASC"] == 20.0
    assert result.iloc[0]["number_density"] == 4.0
    assert result.iloc[0]["biomass_density"] == 8.0
    assert result.iloc[0]["abundance"] == 20.0
    assert result.iloc[0]["biomass"] == 40.0
