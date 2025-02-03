import numpy as np
import shapely
from numpy.typing import NDArray
from shapely.testing import assert_geometries_equal

from geoarrow.shapely import (
    CoordinateDimension,
    PolygonArray,
    construct_geometry_array,
)


def polygons_2d() -> NDArray[np.object_]:
    p0 = shapely.box(0, 1, 5, 10)

    ext_ring = shapely.LinearRing(shapely.box(10, 20, 30, 40).exterior.coords)
    int_ring = shapely.LinearRing(shapely.box(12, 22, 28, 38).exterior.coords[::-1])
    p1 = shapely.Polygon(ext_ring, [int_ring])
    assert p1.is_valid

    return np.array([p0, p1])


def test_round_trip_2d():
    shapely_geoms = polygons_2d()
    polygon_array = construct_geometry_array(shapely_geoms)

    assert isinstance(polygon_array, PolygonArray)
    assert polygon_array.type.coord_dimension == CoordinateDimension.XY

    new_shapely_geoms = polygon_array.to_shapely()
    assert_geometries_equal(shapely_geoms, new_shapely_geoms)

    scalar = polygon_array[0]
    assert scalar.as_py() == shapely_geoms[0]
