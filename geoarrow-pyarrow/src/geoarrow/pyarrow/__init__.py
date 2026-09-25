"""
Contains pyarrow integration for the geoarrow Python bindings.

Examples
--------

>>> import geoarrow.pyarrow as ga
"""

from geoarrow.pyarrow import _scalar
from geoarrow.pyarrow._array import array
from geoarrow.pyarrow._compute import (
    as_geoarrow,
    as_wkb,
    as_wkt,
    box,
    box_agg,
    format_wkt,
    infer_type_common,
    make_point,
    parse_all,
    point_coords,
    rechunk,
    to_geopandas,
    unique_geometry_types,
    with_coord_type,
    with_crs,
    with_dimensions,
    with_edge_type,
    with_geometry_type,
)
from geoarrow.pyarrow._type import (
    GeometryExtensionType,
    LinestringType,
    MultiLinestringType,
    MultiPointType,
    MultiPolygonType,
    PointType,
    PolygonType,
    WkbType,
    WktType,
    extension_type,
    geometry_type_common,
    large_wkb,
    large_wkt,
    linestring,
    multilinestring,
    multipoint,
    multipolygon,
    point,
    polygon,
    wkb,
    wkb_view,
    wkt,
    wkt_view,
)
from geoarrow.types import (
    OGC_CRS84,
    CoordType,
    Dimensions,
    EdgeType,
    Encoding,
    GeometryType,
)
from geoarrow.types._version import __version__, __version_tuple__  # NOQA: F401
from geoarrow.types.type_pyarrow import (
    register_extension_types,
    unregister_extension_types,
)

__all__ = [
    "OGC_CRS84",
    "CoordType",
    "Dimensions",
    "EdgeType",
    "Encoding",
    "GeometryExtensionType",
    "GeometryType",
    "LinestringType",
    "MultiLinestringType",
    "MultiPointType",
    "MultiPolygonType",
    "PointType",
    "PolygonType",
    "WkbType",
    "WktType",
    "_scalar",
    "array",
    "as_geoarrow",
    "as_wkb",
    "as_wkt",
    "box",
    "box_agg",
    "extension_type",
    "format_wkt",
    "geometry_type_common",
    "infer_type_common",
    "large_wkb",
    "large_wkt",
    "linestring",
    "make_point",
    "multilinestring",
    "multipoint",
    "multipolygon",
    "parse_all",
    "point",
    "point_coords",
    "polygon",
    "rechunk",
    "register_extension_types",
    "to_geopandas",
    "unique_geometry_types",
    "unregister_extension_types",
    "with_coord_type",
    "with_crs",
    "with_dimensions",
    "with_edge_type",
    "with_geometry_type",
    "wkb",
    "wkb_view",
    "wkt",
    "wkt_view",
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
