import pytest

from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.type_spec import TypeSpec
from geoarrow.types.crs import OGC_CRS84


def test_type_spec_repr():
    assert repr(TypeSpec()) == "TypeSpec()"
    assert repr(TypeSpec(encoding=Encoding.WKB)) == "TypeSpec(Encoding.WKB)"


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


def test_type_spec_coalesce():
    fully_specified = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.POINT,
        Dimensions.XY,
        CoordType.SEPARATED,
        EdgeType.PLANAR,
        None,
    )

    fully_specified2 = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.LINESTRING,
        Dimensions.XYZ,
        CoordType.INTERLEAVED,
        EdgeType.SPHERICAL,
        OGC_CRS84,
    )

    # Ensure specified always trumps unspecifed
    assert TypeSpec.coalesce(fully_specified, TypeSpec()) == fully_specified
    assert TypeSpec.coalesce(TypeSpec(), fully_specified) == fully_specified

    # Ensure that if both are specified, the lefthand side wins
    assert TypeSpec.coalesce(fully_specified, fully_specified2) == fully_specified
    assert TypeSpec.coalesce(fully_specified2, fully_specified) == fully_specified2


def test_type_spec_coalesce_unspecified():
    fully_specified = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.POINT,
        Dimensions.XY,
        CoordType.SEPARATED,
        EdgeType.PLANAR,
        None,
    )

    # Ensure specified always trumps unspecifed
    assert TypeSpec.coalesce_unspecified(fully_specified, TypeSpec()) == fully_specified
    assert TypeSpec.coalesce_unspecified(TypeSpec(), fully_specified) == fully_specified

    # Ensure that arguments that are equal can be coalesced here
    assert (
        TypeSpec.coalesce_unspecified(fully_specified, fully_specified)
        == fully_specified
    )

    # Ensure that arguments can't be overspecified
    with pytest.raises(ValueError, match="Encoding is overspecified"):
        TypeSpec.coalesce_unspecified(fully_specified, Encoding.WKB)


def test_type_spec_common():
    fully_specified = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.POINT,
        Dimensions.XY,
        CoordType.SEPARATED,
        EdgeType.PLANAR,
        None,
    )
    fully_specified_z = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.POINT,
        Dimensions.XYZ,
        CoordType.SEPARATED,
        EdgeType.PLANAR,
        None,
    )

    # Ensure specified always trumps unspecifed
    assert TypeSpec.common(fully_specified, TypeSpec()) == fully_specified
    assert TypeSpec.common(TypeSpec(), fully_specified) == fully_specified

    # Make sure the common output with itself is equal to itself
    assert TypeSpec.common(fully_specified, fully_specified) == fully_specified

    # Ensure that arguments that have a common output are modified
    assert (
        TypeSpec.common(fully_specified, TypeSpec(dimensions=Dimensions.XYZ))
        == fully_specified_z
    )
