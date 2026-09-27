import numpy as np
import shapely
from numpy.typing import NDArray
from shapely.testing import assert_geometries_equal

from geoarrow.shapely import (
    CoordinateDimension,
    MultiLineStringArray,
    construct_geometry_array,
)


def multilinestrings_2d() -> NDArray[np.object_]:
    ls0 = shapely.LineString([[0, 1], [1, 2], [3, 4]])
    ls1 = shapely.LineString([[3, 4], [5, 6]])

    mls0 = shapely.MultiLineString([ls0])
    mls1 = shapely.MultiLineString([ls0, ls1])

    return np.array([mls0, mls1])


def test_round_trip_2d():
    shapely_geoms = multilinestrings_2d()
    multilinestring_array = construct_geometry_array(shapely_geoms)

    assert isinstance(multilinestring_array, MultiLineStringArray)
    assert multilinestring_array.type.coord_dimension == CoordinateDimension.XY

    new_shapely_geoms = multilinestring_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = multilinestring_array[0]
    assert scalar.as_py() == shapely_geoms[0]
