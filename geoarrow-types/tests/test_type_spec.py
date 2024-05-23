import pytest

from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.type_spec import TypeSpec, type_spec
from geoarrow.types.crs import OGC_CRS84


def test_type_spec_class():
    pass


def test_type_spec_create():
    # From TypeSpec
    spec = TypeSpec()
    assert TypeSpec.create(spec) is spec

    # From Encoding
    assert TypeSpec.create(Encoding.WKB) == TypeSpec(encoding=Encoding.WKB)

    # From GeometryType
    assert TypeSpec.create(GeometryType.POINT) == TypeSpec(
        geometry_type=GeometryType.POINT
    )

    # From Dimensions
    assert TypeSpec.create(Dimensions.XY) == TypeSpec(dimensions=Dimensions.XY)

    # From CoordType
    assert TypeSpec.create(CoordType.INTERLEAVED) == TypeSpec(
        coord_type=CoordType.INTERLEAVED
    )

    # From EdgeType
    assert TypeSpec.create(EdgeType.PLANAR) == TypeSpec(edge_type=EdgeType.PLANAR)

    # From Crs
    assert TypeSpec.create(OGC_CRS84) == TypeSpec(crs=OGC_CRS84)

    # From unknown
    with pytest.raises(
        TypeError, match="Can't create TypeSpec from object of type NoneType"
    ):
        TypeSpec.create(None)
