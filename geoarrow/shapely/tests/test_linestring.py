import numpy as np
import shapely
from numpy.typing import NDArray
from shapely.testing import assert_geometries_equal

from geoarrow.shapely import (
    CoordinateDimension,
    LineStringArray,
    construct_geometry_array,
)


def linestrings_2d() -> NDArray[np.object_]:
    ls0 = shapely.LineString([[0, 1], [1, 2], [3, 4]])
    ls1 = shapely.LineString([[3, 4], [5, 6]])
    return np.array([ls0, ls1])


def test_round_trip_2d():
    shapely_geoms = linestrings_2d()
    line_string_array = construct_geometry_array(shapely_geoms)

    assert isinstance(line_string_array, LineStringArray)
    assert line_string_array.type.coord_dimension == CoordinateDimension.XY

    new_shapely_geoms = line_string_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = line_string_array[0]
    assert scalar.as_py() == shapely_geoms[0]
