import pytest
from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    EdgeType,
)


def test_enum_create_from_input():
    # Can create enum values from an enum value, a string, or None
    assert Encoding.create(Encoding.WKB) is Encoding.WKB
    assert Encoding.create("wkb") is Encoding.WKB
    assert Encoding.create(None) is Encoding.UNSPECIFIED

    with pytest.raises(KeyError):
        Encoding.create("not a valid option")

    with pytest.raises(TypeError):
        Encoding.create(b"123")


def test_enum_default():
    assert Encoding.default(Encoding.WKB, Encoding.UNSPECIFIED) is Encoding.WKB
    assert Encoding.default(Encoding.UNSPECIFIED, Encoding.WKB) is Encoding.WKB
    assert Encoding.default(Encoding.WKB, Encoding.WKT) is Encoding.WKB


def test_enum_specified():
    assert Encoding.specified(Encoding.WKB, Encoding.WKB) is Encoding.WKB
    assert Encoding.specified(Encoding.WKB, Encoding.UNSPECIFIED) is Encoding.WKB
    assert Encoding.specified(Encoding.UNSPECIFIED, Encoding.WKB) is Encoding.WKB

    with pytest.raises(ValueError):
        Encoding.specified(Encoding.WKB, Encoding.WKT)


def test_enum_common():
    # Values equal
    assert Encoding.common(Encoding.WKB, Encoding.WKB) is Encoding.WKB

    # One value unspecified
    assert Encoding.common(Encoding.WKB, Encoding.UNSPECIFIED) is Encoding.WKB
    assert Encoding.common(Encoding.UNSPECIFIED, Encoding.WKB) is Encoding.WKB

    # Values (or reversed values) in lookup table
    assert Encoding.common(Encoding.WKB, Encoding.LARGE_WKB) is Encoding.LARGE_WKB
    assert Encoding.common(Encoding.LARGE_WKB, Encoding.WKB) is Encoding.LARGE_WKB

    # No common value
    assert EdgeType.common(EdgeType.SPHERICAL, EdgeType.PLANAR) is None


def test_encoding_serialized():
    assert Encoding.WKB.is_serialized() is True
    assert Encoding.GEOARROW.is_serialized() is False


def test_geometry_type_common():
    # Case handled by base enum
    assert (
        GeometryType.common(GeometryType.POINT, GeometryType.POINT)
        is GeometryType.POINT
    )

    # Always fall back to geometry
    assert (
        GeometryType.common(GeometryType.POINT, GeometryType.LINESTRING)
        is GeometryType.GEOMETRY
    )
