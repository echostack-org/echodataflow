import geopandas as gpd
from shapely.geometry import Point, box

import pytest

from echodataflow.utils.grid import calculate_cell_areas, create_grid_cells


def test_create_grid_cells_extends_eastward_from_boundary_minimum():
    boundary = gpd.GeoDataFrame(
        geometry=[Point(0, 0), Point(20, 10)],
        crs="EPSG:32610",
    )

    cells = create_grid_cells(boundary, x_step=10, y_step=10)

    assert cells.iloc[0]["grid_x"] == 1
    assert cells.iloc[0]["grid_y"] == 1
    assert cells.iloc[0].geometry.bounds == (0.0, 0.0, 10.0, 10.0)
    assert cells.iloc[1].geometry.bounds == (10.0, 0.0, 20.0, 10.0)


def test_calculate_cell_areas_preserves_complete_outer_cells():
    boundary = gpd.GeoDataFrame(
        geometry=[Point(0, 0), Point(25, 25)],
        crs="EPSG:32610",
    )
    cells = create_grid_cells(boundary, x_step=10, y_step=10)

    result = calculate_cell_areas(cells, projection=boundary.crs)

    northern_cells = result[result["grid_y"] == result["grid_y"].max()]
    assert northern_cells.total_bounds[3] == 30.0
    assert all(cell.bounds[3] - cell.bounds[1] == 10.0 for cell in northern_cells.geometry)
    assert all(cell.bounds[2] - cell.bounds[0] == 10.0 for cell in northern_cells.geometry)
    for row in result.itertuples():
        assert row.area == pytest.approx(row.geometry.area / 1852**2)


def test_calculate_cell_areas_uses_partial_coastal_geometry():
    coastal_cell = gpd.GeoDataFrame(
        geometry=[box(0, 0, 10, 10).difference(box(5, 0, 10, 10))],
        crs="EPSG:32610",
    )

    result = calculate_cell_areas(coastal_cell, projection=coastal_cell.crs)

    assert result.iloc[0].geometry.area == 50.0
    assert result.iloc[0]["area"] == pytest.approx(50.0 / 1852**2)
