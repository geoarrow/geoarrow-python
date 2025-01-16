from typing import Iterable
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
    StructType(struct<x: double not null, y: double not null>)
    """
    return extension_type(types.point())


def linestring() -> LinestringType:
    """Geoarrow-encoded line features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.linestring()
    LinestringType(geoarrow.linestring)
    >>> ga.linestring().storage_type
    ListType(list<vertices: struct<x: double not null, y: double not null> not null>)
    """
    return extension_type(types.linestring())


def polygon() -> PolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.polygon()
    PolygonType(geoarrow.polygon)
    >>> ga.polygon().storage_type
    ListType(list<vertices: list<rings: struct<x: double not null, y: double not null> not null> not null>)
    """
    return extension_type(types.polygon())


def multipoint() -> MultiPointType:
    """Geoarrow-encoded multipoint features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipoint()
    MultiPointType(geoarrow.multipoint)
    >>> ga.multipoint().storage_type
    ListType(list<points: struct<x: double not null, y: double not null> not null>)
    """
    return extension_type(types.multipoint())


def multilinestring() -> MultiLinestringType:
    """Geoarrow-encoded multilinestring features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multilinestring()
    MultiLinestringType(geoarrow.multilinestring)
    >>> ga.multilinestring().storage_type
    ListType(list<vertices: list<linestrings: struct<x: double not null, y: double not null> not null> not null>)
    """
    return extension_type(types.multilinestring())


def multipolygon() -> MultiPolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipolygon()
    MultiPolygonType(geoarrow.multipolygon)
    >>> ga.multipolygon().storage_type
    ListType(list<vertices: list<rings: list<polygons: struct<x: double not null, y: double not null> not null> not null> not null>)
    """
    return extension_type(types.multipolygon())


def geometry_type_common(
    type_objects: Iterable[GeometryExtensionType],
) -> GeometryExtensionType:
    """Compute common type

    From a sequence of GeoArrow types, return a type to which all can be cast
    or error if this cannot occur.

    >>> import geoarrow.pyarrow as ga
    >>> ga.geometry_type_common([ga.wkb(), ga.point()])
    WkbType(geoarrow.wkb)
    >>> ga.geometry_type_common([ga.point(), ga.point()])
    PointType(geoarrow.point)
    """
    type_objects = list(type_objects)

    if len(type_objects) == 0:
        # Would be nice to have an empty type option here
        return wkb()
    elif len(type_objects) == 1:
        return type_objects[0]

    specs = [t.spec for t in type_objects]
    spec = types.TypeSpec.common(*specs).canonicalize()
    return extension_type(spec)
