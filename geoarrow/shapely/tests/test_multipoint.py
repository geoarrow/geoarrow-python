import numpy as np
import shapely
from numpy.typing import NDArray
from shapely.testing import assert_geometries_equal

from geoarrow.shapely import (
    CoordinateDimension,
    MultiPointArray,
    construct_geometry_array,
)


def multipoints_2d() -> NDArray[np.object_]:
    mp0 = shapely.MultiPoint([[0, 1], [1, 2]])
    mp1 = shapely.MultiPoint([[3, 4], [5, 6], [7, 8]])

    return np.array([mp0, mp1])


def test_round_trip_2d():
    shapely_geoms = multipoints_2d()
    multipoint_array = construct_geometry_array(shapely_geoms)

    assert isinstance(multipoint_array, MultiPointArray)
    assert multipoint_array.type.coord_dimension == CoordinateDimension.XY

    new_shapely_geoms = multipoint_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = multipoint_array[0]
    assert scalar.as_py() == shapely_geoms[0]
