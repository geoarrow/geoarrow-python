import pyarrow as pa
import pyarrow_hotfix as _  # noqa: F401

from geoarrow import types
from geoarrow.types.type_pyarrow import (
    GeometryExtensionType,
    PointType,
    LinestringType,
    PolygonType,
    MultiPointType,
    MultiLinestringType,
    MultiPolygonType,
    WkbType,
    WktType,
    extension_type,
)


def wkb() -> WkbType:
    """Well-known binary with a maximum array size of 2 GB per chunk.

    >>> import geoarrow.pyarrow as ga
    >>> ga.wkb()
    WkbType(geoarrow.wkb)
    >>> ga.wkb().storage_type
    DataType(binary)
    """
    return WkbType.__arrow_ext_deserialize__(pa.binary(), b"")


def large_wkb() -> WkbType:
    """Well-known binary using 64-bit integer offsets.

    >>> import geoarrow.pyarrow as ga
    >>> ga.large_wkb()
    WkbType(geoarrow.wkb)
    >>> ga.large_wkb().storage_type
    DataType(large_binary)
    """
    return WkbType.__arrow_ext_deserialize__(pa.large_binary(), b"")


def wkt() -> WktType:
    """Well-known text with a maximum array size of 2 GB per chunk.

    >>> import geoarrow.pyarrow as ga
    >>> ga.wkt()
    WktType(geoarrow.wkt)
    >>> ga.wkt().storage_type
    DataType(string)
    """
    return WktType.__arrow_ext_deserialize__(pa.utf8(), b"")


def large_wkt() -> WktType:
    """Well-known text using 64-bit integer offsets.

    >>> import geoarrow.pyarrow as ga
    >>> ga.large_wkt()
    WktType(geoarrow.wkt)
    >>> ga.large_wkt().storage_type
    DataType(large_string)
    """
    return WktType.__arrow_ext_deserialize__(pa.large_utf8(), b"")


def point() -> PointType:
    """Geoarrow-encoded point features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.point()
    PointType(geoarrow.point)
    >>> ga.point().storage_type
    StructType(struct<x: double, y: double>)
    """
    return extension_type(types.point())


def linestring() -> LinestringType:
    """Geoarrow-encoded line features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.linestring()
    LinestringType(geoarrow.linestring)
    >>> ga.linestring().storage_type
    ListType(list<vertices: struct<x: double, y: double>>)
    """
    return extension_type(types.linestring())


def polygon() -> PolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.polygon()
    PolygonType(geoarrow.polygon)
    >>> ga.polygon().storage_type
    ListType(list<rings: list<vertices: struct<x: double, y: double>>>)
    """
    return extension_type(types.polygon())


def multipoint() -> MultiPointType:
    """Geoarrow-encoded multipoint features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipoint()
    MultiPointType(geoarrow.multipoint)
    >>> ga.multipoint().storage_type
    ListType(list<points: struct<x: double, y: double>>)
    """
    return extension_type(types.multipoint())


def multilinestring() -> MultiLinestringType:
    """Geoarrow-encoded multilinestring features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multilinestring()
    MultiLinestringType(geoarrow.multilinestring)
    >>> ga.multilinestring().storage_type
    ListType(list<linestrings: list<vertices: struct<x: double, y: double>>>)
    """
    return extension_type(types.multilinestring())


def multipolygon() -> MultiPolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipolygon()
    MultiPolygonType(geoarrow.multipolygon)
    >>> ga.multipolygon().storage_type
    ListType(list<polygons: list<rings: list<vertices: struct<x: double, y: double>>>>)
    """
    return extension_type(types.multipolygon())


def _vector_type_common2(a, b):
    if not isinstance(a, GeometryExtensionType) or not isinstance(
        b, GeometryExtensionType
    ):
        raise ValueError(
            f"Can't compute common type between '{a}' and '{b}': non-geometry type"
        )

    if a == b:
        return a

    # This computation doesn't handle non-equal metadata (crs, edge type)
    metadata_a = a._type.extension_metadata
    metadata_b = b._type.extension_metadata
    if metadata_a != metadata_b:
        raise ValueError(
            f"Can't compute common type between '{a}' and '{b}': metadata not equal"
        )

    # TODO: There are a number of other things we can try (e.g., promote multi)
    # For now, just use wkb() if the types aren't exactly the same
    return wkb().with_metadata(metadata_a)


def geometry_type_common(types):
    """Compute common type

    From a sequence of GeoArrow types, return a type to which all can be cast
    or error if this cannot occur.

    >>> import geoarrow.pyarrow as ga
    >>> ga.geometry_type_common([ga.wkb(), ga.point()])
    WkbType(geoarrow.wkb)
    >>> ga.geometry_type_common([ga.point(), ga.point()])
    PointType(geoarrow.point)
    """
    types = list(types)

    if len(types) == 0:
        # Would be nice to have an empty type option here
        return wkb()
    elif len(types) == 1:
        return types[0]

    for i in reversed(range(len(types) - 1)):
        types[i] = _vector_type_common2(types[i], types[i + 1])

    return types[0]
