import pytest

from geoarrow.types import (
    create_geoarrow_type,
    Encoding,
    GeometryType,
    Dimensions,
    CoordType,
    EdgeType,
    OGC_CRS84,
)

from geoarrow.types.type_base import InvalidTypeError


def test_create_serialized_type_defaults():
    dt = create_geoarrow_type(Encoding.WKB)
    assert dt.encoding == Encoding.WKB
    assert dt.geometry_type == GeometryType.GEOMETRY
    assert dt.dimensions == Dimensions.UNKNOWN
    assert dt.coord_type == CoordType.UNKNOWN
    assert dt.edge_type == EdgeType.PLANAR
    assert dt.crs is None


def test_create_serialized_type_edge_type():
    dt = create_geoarrow_type(Encoding.WKB, edge_type=EdgeType.SPHERICAL)
    assert dt.encoding == Encoding.WKB
    assert dt.edge_type == EdgeType.SPHERICAL


def test_create_serialized_type_crs():
    dt = create_geoarrow_type(Encoding.WKB, crs=OGC_CRS84)
    assert dt.encoding == Encoding.WKB
    assert dt.crs is OGC_CRS84


def test_create_native_type_defaults():
    dt = create_geoarrow_type(
        Encoding.GEOARROW, GeometryType.POINT, CoordType.INTERLEAVED
    )
    assert dt.encoding == Encoding.GEOARROW
    assert dt.geometry_type == GeometryType.POINT
    assert dt.coord_type == CoordType.INTERLEAVED
    assert dt.dimensions == Dimensions.XY
    assert dt.edge_type == EdgeType.PLANAR
    assert dt.crs is None


def test_create_native_type_edge_type():
    dt = create_geoarrow_type(
        Encoding.GEOARROW,
        GeometryType.POINT,
        CoordType.INTERLEAVED,
        edge_type=EdgeType.SPHERICAL,
    )
    assert dt.encoding == Encoding.GEOARROW
    assert dt.edge_type == EdgeType.SPHERICAL


def test_create_native_type_crs():
    dt = create_geoarrow_type(
        Encoding.GEOARROW, GeometryType.POINT, CoordType.INTERLEAVED, crs=OGC_CRS84
    )
    assert dt.encoding == Encoding.GEOARROW
    assert dt.crs is OGC_CRS84


def test_create_native_type_errors():
    # If encoding is GEOARROW, geometry_type and coord_type must be specified
    with pytest.raises(InvalidTypeError):
        create_geoarrow_type(Encoding.GEOARROW)
    with pytest.raises(InvalidTypeError):
        create_geoarrow_type(Encoding.GEOARROW, geometry_type=GeometryType.POINT)
    with pytest.raises(InvalidTypeError):
        create_geoarrow_type(Encoding.GEOARROW, coord_type=CoordType.INTERLEAVED)
