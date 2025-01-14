"""
Contains pyarrow integration for the geoarrow Python bindings.

Examples
--------

>>> import geoarrow.pyarrow as ga
"""

from geoarrow.types._version import __version__, __version_tuple__  # NOQA: F401

from geoarrow.types import GeometryType, Dimensions, CoordType, EdgeType

from geoarrow.pyarrow._type import (
    GeometryExtensionType,
    WktType,
    WkbType,
    PointType,
    LinestringType,
    PolygonType,
    MultiPointType,
    MultiLinestringType,
    MultiPolygonType,
    wkb,
    large_wkb,
    wkt,
    large_wkt,
    point,
    linestring,
    polygon,
    multipoint,
    multilinestring,
    multipolygon,
    extension_type,
    geometry_type_common,
)

from geoarrow.types.type_pyarrow import (
    register_extension_types,
    unregister_extension_types,
)

from geoarrow.pyarrow._kernel import Kernel

from geoarrow.pyarrow._array import array

from geoarrow.pyarrow import _scalar

from geoarrow.pyarrow._compute import (
    parse_all,
    as_wkt,
    as_wkb,
    infer_type_common,
    as_geoarrow,
    format_wkt,
    make_point,
    unique_geometry_types,
    box,
    box_agg,
    with_coord_type,
    with_crs,
    with_dimensions,
    with_edge_type,
    with_geometry_type,
    rechunk,
    point_coords,
    to_geopandas,
)

__all__ = [
    "GeometryType",
    "Dimensions",
    "CoordType",
    "EdgeType",
    "CrsType",
    "GeometryExtensionType",
    "WktType",
    "WkbType",
    "PointType",
    "LinestringType",
    "PolygonType",
    "MultiPointType",
    "MultiLinestringType",
    "MultiPolygonType",
    "wkb",
    "large_wkb",
    "wkt",
    "large_wkt",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "extension_type",
    "geometry_type_common",
    "register_extension_types",
    "unregister_extension_types",
    "Kernel",
    "array",
    "parse_all",
    "as_wkt",
    "as_wkb",
    "infer_type_common",
    "as_geoarrow",
    "format_wkt",
    "make_point",
    "unique_geometry_types",
    "box",
    "box_agg",
    "with_coord_type",
    "with_crs",
    "with_dimensions",
    "with_edge_type",
    "with_geometry_type",
    "rechunk",
    "point_coords",
    "to_geopandas",
    "_scalar",
]

try:
    register_extension_types()
except Exception as e:
    import warnings

    warnings.warn(
        "Failed to register one or more extension types.\n"
        "If this warning appears from pytest, you may have to re-run with --import-mode=importlib.\n"
        "You may also be able to run `unregister_extension_types()` and `register_extension_types()`.\n"
        f"The original error was {e}"
    )
