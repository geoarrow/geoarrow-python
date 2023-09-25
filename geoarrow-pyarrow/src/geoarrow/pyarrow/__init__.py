"""
Contains pyarrow integration for the geoarrow Python bindings.

Examples
--------

>>> import geoarrow.pyarrow as ga
"""

from geoarrow.c.lib import GeometryType, Dimensions, CoordType, EdgeType, CrsType

from ._type import (
    VectorType,
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
    vector_type,
    vector_type_common,
    register_extension_types,
    unregister_extension_types,
)

from ._kernel import Kernel

from ._array import array

from . import _scalar

from ._compute import (
    parse_all,
    as_wkt,
    as_wkb,
    infer_type_common,
    as_geoarrow,
    format_wkt,
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
)


# Use a lazy import here to avoid requiring pyarrow.dataset
def dataset(*args, geometry_columns=None, use_row_groups=None, **kwargs):
    """Construct a GeoDataset

    This constructor is intended to mirror `pyarrow.dataset()`, adding
    geo-specific arguments. See :class:`geoarrow.pyarrow._dataset.GeoDataset` for
    details.

    >>> import geoarrow.pyarrow as ga
    >>> import pyarrow as pa
    >>> table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    >>> dataset = ga.dataset(table)
    """
    from pyarrow import dataset as _ds
    from ._dataset import GeoDataset, ParquetRowGroupGeoDataset

    parent = _ds.dataset(*args, **kwargs)

    if use_row_groups is None:
        use_row_groups = isinstance(parent, _ds.FileSystemDataset) and isinstance(
            parent.format, _ds.ParquetFileFormat
        )
    if use_row_groups:
        return ParquetRowGroupGeoDataset.create(
            parent, geometry_columns=geometry_columns
        )
    else:
        return GeoDataset(parent, geometry_columns=geometry_columns)


register_extension_types()
