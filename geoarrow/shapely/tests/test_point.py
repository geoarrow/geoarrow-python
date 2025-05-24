import numpy as np
import shapely
from numpy.typing import NDArray
from shapely.testing import assert_geometries_equal

from geoarrow.shapely import PointArray, construct_geometry_array
from geoarrow.shapely import CoordinateDimension


def points_2d() -> NDArray[np.object_]:
    return shapely.points([0, 1, 2, 3], [4, 5, 6, 7])


def points_3d() -> NDArray[np.object_]:
    return shapely.points([0, 1, 2, 3], [4, 5, 6, 7], [9, 10, 11, 12])


def test_round_trip_2d():
    shapely_geoms = points_2d()
    point_array = construct_geometry_array(shapely_geoms)

    assert isinstance(point_array, PointArray)
    assert point_array.type.coord_dimension == CoordinateDimension.XY

    new_shapely_geoms = point_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = point_array[0]
    assert scalar.as_py() == shapely_geoms[0]


def test_round_trip_3d():
    shapely_geoms = points_3d()
    point_array = construct_geometry_array(shapely_geoms)

    assert isinstance(point_array, PointArray)
    assert point_array.type.coord_dimension == CoordinateDimension.XYZ

    new_shapely_geoms = point_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = point_array[0]
    assert scalar.as_py() == shapely_geoms[0]
