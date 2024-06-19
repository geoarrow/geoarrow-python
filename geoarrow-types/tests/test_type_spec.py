import pytest

import geoarrow.types as gt
from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.type_spec import TypeSpec
from geoarrow.types.crs import OGC_CRS84, UNSPECIFIED as UNSPECIFIED_CRS


def test_type_spec_repr():
    assert repr(TypeSpec()) == "TypeSpec()"
    assert repr(TypeSpec(encoding=Encoding.WKB)) == "TypeSpec(Encoding.WKB)"


def test_type_spec_extension_name():
    assert gt.wkb().extension_name() == "geoarrow.wkb"
    assert gt.large_wkb().extension_name() == "geoarrow.wkb"
    assert gt.wkt().extension_name() == "geoarrow.wkt"
    assert gt.large_wkt().extension_name() == "geoarrow.wkt"

    assert gt.point().extension_name() == "geoarrow.point"
    assert gt.linestring().extension_name() == "geoarrow.linestring"
    assert gt.polygon().extension_name() == "geoarrow.polygon"
    assert gt.multipoint().extension_name() == "geoarrow.multipoint"
    assert gt.multilinestring().extension_name() == "geoarrow.multilinestring"
    assert gt.multipolygon().extension_name() == "geoarrow.multipolygon"

    with pytest.raises(ValueError, match="Can't compute extension name for"):
        TypeSpec().extension_name()

    with pytest.raises(ValueError, match="Can't compute extension name for"):
        TypeSpec(encoding=Encoding.GEOARROW).extension_name()


def test_type_spec_extension_metadata():
    assert TypeSpec().with_defaults().extension_metadata() == "{}"
    assert (
        TypeSpec(edge_type=EdgeType.SPHERICAL).with_defaults().extension_metadata()
        == '{"edges": "spherical"}'
    )
    assert (
        TypeSpec(crs=gt.OGC_CRS84)
        .with_defaults()
        .extension_metadata()
        .startswith('{"crs": {')
    )

    with pytest.raises(ValueError, match="Can't compute extension_metadata"):
        TypeSpec().extension_metadata()


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

    # Ensure that with_default()/override() are mapped properly
    assert TypeSpec().with_defaults(fully_specified) == fully_specified
    assert fully_specified.with_defaults(fully_specified2) == fully_specified


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


def test_type_spec_override():
    fully_specified = TypeSpec(
        Encoding.GEOARROW,
        GeometryType.POINT,
        Dimensions.XY,
        CoordType.SEPARATED,
        EdgeType.PLANAR,
        None,
    )

    assert fully_specified.override(encoding="unspecified") == TypeSpec(
        Encoding.UNSPECIFIED, *fully_specified[1:]
    )

    assert fully_specified.override(geometry_type="unspecified") == TypeSpec(
        *fully_specified[:1], GeometryType.UNSPECIFIED, *fully_specified[2:]
    )

    assert fully_specified.override(dimensions="unspecified") == TypeSpec(
        *fully_specified[:2], Dimensions.UNSPECIFIED, *fully_specified[3:]
    )

    assert fully_specified.override(coord_type="unspecified") == TypeSpec(
        *fully_specified[:3], CoordType.UNSPECIFIED, *fully_specified[4:]
    )

    assert fully_specified.override(edge_type="unspecified") == TypeSpec(
        *fully_specified[:4], EdgeType.UNSPECIFIED, *fully_specified[5:]
    )

    assert fully_specified.override(crs=UNSPECIFIED_CRS) == TypeSpec(
        *fully_specified[:5], UNSPECIFIED_CRS
    )


def test_type_spec_helper():
    # Check positional arguments inferred
    assert gt.type_spec(Encoding.WKB) == TypeSpec(encoding=Encoding.WKB)
    assert gt.type_spec(GeometryType.POINT) == TypeSpec(
        geometry_type=GeometryType.POINT
    )
    assert gt.type_spec(Dimensions.XY) == TypeSpec(dimensions=Dimensions.XY)
    assert gt.type_spec(CoordType.INTERLEAVED) == TypeSpec(
        coord_type=CoordType.INTERLEAVED
    )
    assert gt.type_spec(EdgeType.PLANAR) == TypeSpec(edge_type=EdgeType.PLANAR)
    assert gt.type_spec(gt.OGC_CRS84) == TypeSpec(crs=gt.OGC_CRS84)

    # Check sanitized arguments by name
    assert gt.type_spec(encoding="wkb") == TypeSpec(encoding=Encoding.WKB)
    assert gt.type_spec(geometry_type="point") == TypeSpec(
        geometry_type=GeometryType.POINT
    )
    assert gt.type_spec(dimensions="xy") == TypeSpec(dimensions=Dimensions.XY)
    assert gt.type_spec(coord_type="interleaved") == TypeSpec(
        coord_type=CoordType.INTERLEAVED
    )
    assert gt.type_spec(edge_type="planar") == TypeSpec(edge_type=EdgeType.PLANAR)
    assert gt.type_spec(crs=gt.OGC_CRS84) == TypeSpec(crs=gt.OGC_CRS84)


def test_type_spec_shortcuts():
    assert gt.wkb() == TypeSpec(encoding=Encoding.WKB)
    assert gt.large_wkb() == TypeSpec(encoding=Encoding.LARGE_WKB)
    assert gt.wkt() == TypeSpec(encoding=Encoding.WKT)
    assert gt.large_wkt() == TypeSpec(encoding=Encoding.LARGE_WKT)

    assert gt.geoarrow() == TypeSpec(encoding=Encoding.GEOARROW)
    assert gt.point() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.POINT
    )
    assert gt.linestring() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.LINESTRING
    )
    assert gt.polygon() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.POLYGON
    )
    assert gt.multipoint() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.MULTIPOINT
    )
    assert gt.multilinestring() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.MULTILINESTRING
    )
    assert gt.multipolygon() == TypeSpec(
        encoding=Encoding.GEOARROW, geometry_type=GeometryType.MULTIPOLYGON
    )
