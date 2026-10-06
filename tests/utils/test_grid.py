import geopandas as gpd
from shapely.geometry import Point

from echodataflow.utils.grid import create_grid_cells


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
