import pyarrow as pa
import pyarrow.compute as pc

from geoarrow.c.lib import GeometryType, Dimensions, CoordType, EdgeType
from geoarrow.pyarrow import _type
from geoarrow.pyarrow._array import array
from geoarrow.pyarrow._kernel import Kernel


def obj_as_array_or_chunked(obj_in):
    if (
        isinstance(obj_in, pa.Array) or isinstance(obj_in, pa.ChunkedArray)
    ) and isinstance(obj_in.type, _type.GeometryExtensionType):
        return obj_in
    else:
        return array(obj_in)


def ensure_storage(obj):
    if not isinstance(obj.type, pa.ExtensionType):
        return obj

    if isinstance(obj, pa.ChunkedArray):
        return pa.chunked_array(
            [chunk.storage for chunk in obj.chunks], type=obj.type.storage_type
        )
    else:
        return obj.storage


def push_all(
    kernel_constructor, obj, args=None, is_agg=False, max_workers=None, result=True
):
    if args is None:
        args = {}

    kernel = kernel_constructor(obj.type, **args)

    if is_agg:
        kernel.push(obj)
        return kernel.finish()
    else:
        return kernel.push(obj)


def parse_all(obj):
    """Parse all features and return nothing. This is useful for
    :func:`geoarrow.pyarrow.wkb` and :func:`geoarrow.pyarrow.wkt`-encoded
    arrays to validate their contents. For other types, this is a no-op.

    >>> import geoarrow.pyarrow as ga
    >>> ga.parse_all(["POINT (0 1)"])
    >>> ga.parse_all(["POINT (0 1"])
    Traceback (most recent call last):
     ...
    geoarrow.c._lib.GeoArrowCException: GeoArrowKernel<visit_void_agg>::push_batch() failed (22): Expected ')' at byte 10
    """
    obj = obj_as_array_or_chunked(obj)

    # Non-wkb or wkt types are a no-op here since they don't need parsing
    if isinstance(obj.type, _type.WkbType) or isinstance(obj.type, _type.WktType):
        push_all(Kernel.visit_void_agg, obj, result=False)

    return None


def unique_geometry_types(obj):
    """Compute unique geometry types from ``obj`` as a struct with columns
    geometry_type and dimensions. The values of these columns correspond to the
    values of the :class:`geoarrow.GeometryType` and :class:`geoarrow.Dimensions`
    enumerators.

    >>> import geoarrow.pyarrow as ga
    >>> print(str(ga.unique_geometry_types(["POINT Z (0 1 2)", "LINESTRING (0 0, 1 3)"])))
    -- is_valid: all not null
    -- child 0 type: int32
      [
        2,
        1
      ]
    -- child 1 type: int32
      [
        1,
        2
      ]
    """
    obj = obj_as_array_or_chunked(obj)
    out_type = pa.struct([("geometry_type", pa.int32()), ("dimensions", pa.int32())])

    if not isinstance(obj.type, _type.WktType) and not isinstance(
        obj.type, _type.WkbType
    ):
        return pa.array(
            [
                {
                    "geometry_type": obj.type.geometry_type,
                    "dimensions": obj.type.dimensions,
                }
            ],
            type=out_type,
        )

    result = push_all(Kernel.unique_geometry_types_agg, obj, is_agg=True)

    # Kernel currently returns ISO code integers (e.g., 2002):
    # convert to struct(geometry_type, dimensions)
    py_geometry_types = []
    for item in result:
        item_int = item.as_py()
        if item_int >= 3000:
            dimensions = Dimensions.XYZM
            item_int -= 3000
        elif item_int >= 2000:
            dimensions = Dimensions.XYM
            item_int -= 2000
        elif item_int >= 1000:
            dimensions = Dimensions.XYZ
            item_int -= 1000
        else:
            dimensions = Dimensions.XY

        py_geometry_types.append({"geometry_type": item_int, "dimensions": dimensions})

    return pa.array(py_geometry_types, type=out_type)


def infer_type_common(obj, coord_type=None, promote_multi=False):
    """Infer a common :class:`geoarrow.pyarrow.GeometryExtensionType` for the
    geometries in ``obj``, preferring geoarrow-encoded types and falling back
    to well-known binary.

    >>> import geoarrow.pyarrow as ga
    >>> ga.infer_type_common(["POINT Z (0 1 2)", "LINESTRING (0 0, 1 3)"])
    WkbType(geoarrow.wkb)
    >>> ga.infer_type_common(["POINT Z (0 1 2)", "MULTIPOINT (0 0, 1 3)"])
    MultiPointType(geoarrow.multipoint_z)
    """
    obj = obj_as_array_or_chunked(obj)

    if not isinstance(obj.type, _type.WktType) and not isinstance(
        obj.type, _type.WkbType
    ):
        if coord_type is None:
            return obj.type
        else:
            return obj.type.with_coord_type(coord_type)

    if coord_type is None:
        coord_type = CoordType.SEPARATE

    types = unique_geometry_types(obj)
    if len(types) == 0:
        # Not ideal: we probably want a _type.empty() that keeps the CRS
        return pa.null()

    types = types.flatten()

    unique_dims = types[1].unique().to_pylist()
    has_z = any(dim in (Dimensions.XYZ, Dimensions.XYZM) for dim in unique_dims)
    has_m = any(dim in (Dimensions.XYM, Dimensions.XYZM) for dim in unique_dims)
    if has_z and has_m:
        dimensions = Dimensions.XYZM
    elif has_z:
        dimensions = Dimensions.XYZ
    elif has_m:
        dimensions = Dimensions.XYM
    else:
        dimensions = Dimensions.XY

    unique_geom_types = types[0].unique().to_pylist()
    if len(unique_geom_types) == 1:
        geometry_type = unique_geom_types[0]
    elif all(
        t in (GeometryType.POINT, GeometryType.MULTIPOINT) for t in unique_geom_types
    ):
        geometry_type = GeometryType.MULTIPOINT
    elif all(
        t in (GeometryType.LINESTRING, GeometryType.MULTILINESTRING)
        for t in unique_geom_types
    ):
        geometry_type = GeometryType.MULTILINESTRING
    elif all(
        t in (GeometryType.POLYGON, GeometryType.MULTIPOLYGON)
        for t in unique_geom_types
    ):
        geometry_type = GeometryType.MULTIPOLYGON
    else:
        return (
            _type.wkb()
            .with_edge_type(obj.type.edge_type)
            .with_crs(obj.type.crs, obj.type.crs_type)
        )

    if promote_multi and geometry_type <= GeometryType.POLYGON:
        geometry_type += 3

    return _type.extension_type(
        geometry_type,
        dimensions,
        coord_type,
        edge_type=obj.type.edge_type,
        crs=obj.type.crs,
        crs_type=obj.type.crs_type,
    )


def as_wkt(obj):
    """Encode ``obj`` as :func:`geoarrow.pyarrow.wkt`.

    >>> import geoarrow.pyarrow as ga
    >>> points = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.as_wkt(points)
    GeometryExtensionArray:WktType(geoarrow.wkt)[1]
    <POINT (0 1)>
    """
    return as_geoarrow(obj, _type.wkt())


def as_wkb(obj):
    """Encode ``obj`` as :func:`geoarrow.pyarrow.wkb`.

    >>> import geoarrow.pyarrow as ga
    >>> points = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.as_wkb(points)
    GeometryExtensionArray:WkbType(geoarrow.wkb)[1]
    <POINT (0 1)>
    """
    return as_geoarrow(obj, _type.wkb())


def as_geoarrow(obj, type=None, coord_type=None, promote_multi=False):
    """Encode ``obj`` as a geoarrow-encoded array, preferring geoarrow encodings
    and falling back to well-known binary if no common geoemtry type is found.

    >>> import geoarrow.pyarrow as ga
    >>> ga.as_geoarrow(["POINT (0 1)", "MULTIPOINT Z (0 1 2, 4 5 6)"])
    MultiPointArray:MultiPointType(geoarrow.multipoint_z)[2]
    <MULTIPOINT Z (0 1 nan)>
    <MULTIPOINT Z (0 1 2, 4 5 6)>
    """
    obj = obj_as_array_or_chunked(obj)

    if type is None:
        type = infer_type_common(
            obj, coord_type=coord_type, promote_multi=promote_multi
        )

    if obj.type.geoarrow_id == type.geoarrow_id:
        return obj

    return push_all(Kernel.as_geoarrow, obj, args={"type_id": type.geoarrow_id})


def format_wkt(obj, precision=None, max_element_size_bytes=None):
    """Format geometries in an object as well-known text with an optional cap
    on digits and element size to prevent excessive output for large features.

    >>> import geoarrow.pyarrow as ga
    >>> print(str(ga.format_wkt(ga.array(["POINT (0 1.3333333333333)"]), precision=5)))
    [
      "POINT (0 1.33333)"
    ]
    >>> print(str(ga.format_wkt(ga.array(["POINT (0 1)"]), max_element_size_bytes=3)))
    [
      "POI"
    ]
    """
    return push_all(
        Kernel.format_wkt,
        obj,
        args={
            "precision": precision,
            "max_element_size_bytes": max_element_size_bytes,
        },
    )


def _box_point_struct(storage):
    arrays = storage.flatten()
    return pa.StructArray.from_arrays(
        [arrays[0], arrays[0], arrays[1], arrays[1]],
        names=["xmin", "xmax", "ymin", "ymax"],
    )


def box(obj):
    """Compute a Cartesian 2D bounding box for each feature in ``obj`` as
    a struct(xmin, xmax, ymin, ymax) array.

    >>> import geoarrow.pyarrow as ga
    >>> ga.box(["LINESTRING (0 10, 34 -1)"]).type
    StructType(struct<xmin: double, xmax: double, ymin: double, ymax: double>)
    >>> print(str(ga.box(["LINESTRING (0 10, 34 -1)"])))
    -- is_valid: all not null
    -- child 0 type: double
      [
        0
      ]
    -- child 1 type: double
      [
        34
      ]
    -- child 2 type: double
      [
        -1
      ]
    -- child 3 type: double
      [
        10
      ]
    """

    obj = obj_as_array_or_chunked(obj)

    # Spherical edges aren't supported by this algorithm
    if obj.type.edge_type == EdgeType.SPHERICAL:
        raise TypeError("Can't compute box of type with spherical edges")

    # Optimization: a box of points is just x, x, y, y with zero-copy
    # if the coord type is struct
    if obj.type.coord_type == CoordType.SEPARATE and len(obj) > 0:
        if obj.type.geometry_type == GeometryType.POINT and isinstance(
            obj, pa.ChunkedArray
        ):
            chunks = [_box_point_struct(chunk.storage) for chunk in obj.chunks]
            return pa.chunked_array(chunks)
        elif obj.type.geometry_type == GeometryType.POINT:
            return _box_point_struct(obj.storage)

    return push_all(Kernel.box, obj)


def _box_agg_point_struct(arrays):
    out = [list(pc.min_max(array).values()) for array in arrays]
    out_dict = {
        "xmin": out[0][0].as_py(),
        "xmax": out[0][1].as_py(),
        "ymin": out[1][0].as_py(),
        "ymax": out[1][1].as_py(),
    }

    # Apparently pyarrow reorders dict keys when inferring scalar types?
    return pa.scalar(
        out_dict, pa.struct([(nm, pa.float64()) for nm in out_dict.keys()])
    )


def box_agg(obj):
    """Compute a Cartesian 2D bounding box for all features in ``obj`` as
    a scalar struct(xmin, xmax, ymin, ymax). Values that are null are currently
    ignored.

    >>> import geoarrow.pyarrow as ga
    >>> ga.box_agg(["POINT (0 10)", "POINT (34 -1)"])
    <pyarrow.StructScalar: [('xmin', 0.0), ('xmax', 34.0), ('ymin', -1.0), ('ymax', 10.0)]>
    """

    obj = obj_as_array_or_chunked(obj)

    # Spherical edges aren't supported by this algorithm
    if obj.type.edge_type == EdgeType.SPHERICAL:
        raise TypeError("Can't compute box of type with spherical edges")

    # Optimization: pyarrow's minmax kernel is fast and we can use it if we have struct
    # coords. So far, only a measurable improvement for points.
    if obj.type.coord_type == CoordType.SEPARATE and len(obj) > 0:
        if obj.type.geometry_type == GeometryType.POINT and isinstance(
            obj, pa.ChunkedArray
        ):
            chunks = [chunk.storage.flatten()[:2] for chunk in obj.chunks]
            chunked_x = pa.chunked_array([chunk[0] for chunk in chunks])
            chunked_y = pa.chunked_array([chunk[1] for chunk in chunks])
            return _box_agg_point_struct([chunked_x, chunked_y])
        elif obj.type.geometry_type == GeometryType.POINT:
            return _box_agg_point_struct(obj.storage.flatten()[:2])

    return push_all(Kernel.box_agg, obj, is_agg=True)[0]


def _rechunk_max_bytes_internal(obj, max_bytes, chunks):
    n = len(obj)
    if n == 0:
        return

    if obj.nbytes <= max_bytes or n <= 1:
        chunks.append(obj)
    else:
        _rechunk_max_bytes_internal(obj[: (n // 2)], max_bytes, chunks)
        _rechunk_max_bytes_internal(obj[(n // 2) :], max_bytes, chunks)


def rechunk(obj, max_bytes):
    """Split up chunks of ``obj`` into zero-copy slices with a maximum size of
    ``max_bytes``. This may be useful to more predictibly parallelize a
    computation for variable feature sizes.

    >>> import geoarrow.pyarrow as ga
    >>> print(str(ga.rechunk(["POINT (0 1)", "POINT (2 3)"], max_bytes=100)))
    [
      [
        "POINT (0 1)",
        "POINT (2 3)"
      ]
    ]
    >>> print(str(ga.rechunk(["POINT (0 1)", "POINT (2 3)"], max_bytes=5)))
    [
      [
        "POINT (0 1)"
      ],
      [
        "POINT (2 3)"
      ]
    ]
    """
    obj = obj_as_array_or_chunked(obj)
    chunks = []

    if isinstance(obj, pa.ChunkedArray):
        for chunk in obj.chunks:
            _rechunk_max_bytes_internal(chunk, max_bytes, chunks)
    else:
        _rechunk_max_bytes_internal(obj, max_bytes, chunks)

    return pa.chunked_array(chunks, type=obj.type)


def with_coord_type(obj, coord_type):
    """Attempt to convert ``obj`` to a geoarrow-encoded array with a
    specific :class:`CoordType`.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_coord_type(["POINT (0 1)"], ga.CoordType.INTERLEAVED)
    PointArray:PointType(interleaved geoarrow.point)[1]
    <POINT (0 1)>
    """
    return as_geoarrow(obj, coord_type=coord_type)


def with_edge_type(obj, edge_type):
    """Force a :class:`geoarrow.EdgeType` on an array.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_edge_type(["LINESTRING (0 1, 2 3)"], ga.EdgeType.SPHERICAL)
    GeometryExtensionArray:WktType(spherical geoarrow.wkt)[1]
    <LINESTRING (0 1, 2 3)>
    """
    obj = obj_as_array_or_chunked(obj)
    new_type = obj.type.with_edge_type(edge_type)
    return new_type.wrap_array(ensure_storage(obj))


def with_crs(obj, crs, crs_type=None):
    """Force a :class:`geoarrow.CrsType`/crs value on an array.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_crs(["POINT (0 1)"], "EPSG:1234")
    GeometryExtensionArray:WktType(geoarrow.wkt <EPSG:1234>)[1]
    <POINT (0 1)>
    """
    obj = obj_as_array_or_chunked(obj)
    new_type = obj.type.with_crs(crs, crs_type)
    return new_type.wrap_array(ensure_storage(obj))


def with_dimensions(obj, dimensions):
    """Attempt to convert ``obj`` to a geoarrow-encoded array with a
    specific :class:`geoarrow.Dimensions`. If dimensions need to be
    added, nonexistent values will be filled with ``nan``. If
    dimensions need to be dropped, this function will silently
    drop them. You can use :func:`geoarrow.pyarrow.unique_geometry_types`
    to efficiently detect if one or both of these will occur.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_dimensions(["POINT (0 1)"], ga.Dimensions.XYZM)
    PointArray:PointType(geoarrow.point_zm)[1]
    <POINT ZM (0 1 nan nan)>
    >>> ga.with_dimensions(["POINT ZM (0 1 2 3)"], ga.Dimensions.XY)
    PointArray:PointType(geoarrow.point)[1]
    <POINT (0 1)>
    """
    obj = as_geoarrow(obj)
    if dimensions == obj.type.dimensions:
        return obj

    new_type = obj.type.with_dimensions(dimensions)
    return as_geoarrow(obj, type=new_type)


def with_geometry_type(obj, geometry_type):
    """Attempt to convert ``obj`` to a geoarrow-encoded array with a
    specific :class:`geoarrow.GeometryType`.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_geometry_type(["POINT (0 1)"], ga.GeometryType.MULTIPOINT)
    MultiPointArray:MultiPointType(geoarrow.multipoint)[1]
    <MULTIPOINT (0 1)>
    >>> ga.with_geometry_type(["MULTIPOINT (0 1)"], ga.GeometryType.POINT)
    PointArray:PointType(geoarrow.point)[1]
    <POINT (0 1)>
    >>> ga.with_geometry_type(["LINESTRING EMPTY", "POINT (0 1)"], ga.GeometryType.POINT)
    PointArray:PointType(geoarrow.point)[2]
    <POINT (nan nan)>
    <POINT (0 1)>
    >>> ga.with_geometry_type(["MULTIPOINT (0 1, 2 3)"], ga.GeometryType.POINT)
    Traceback (most recent call last):
      ...
    geoarrow.c._lib.GeoArrowCException: GeoArrowKernel<as_geoarrow>::push_batch() failed (22): Can't convert feature with >1 coordinate to POINT
    """
    obj = as_geoarrow(obj)
    if geometry_type == obj.type.geometry_type:
        return obj

    new_type = obj.type.with_geometry_type(geometry_type)
    return as_geoarrow(obj, type=new_type)


def point_coords(obj, dimensions=None):
    """Extract point coordinates into separate arrays or chunked arrays.

    >>> import geoarrow.pyarrow as ga
    >>> x, y = ga.point_coords(["POINT (0 1)", "POINT (2 3)"])
    >>> list(x)
    [<pyarrow.DoubleScalar: 0.0>, <pyarrow.DoubleScalar: 2.0>]
    >>> list(y)
    [<pyarrow.DoubleScalar: 1.0>, <pyarrow.DoubleScalar: 3.0>]
    """
    if dimensions is None:
        target_type = _type.point()
    else:
        target_type = _type.point().with_dimensions(dimensions)

    obj = as_geoarrow(obj, target_type)
    if isinstance(obj, pa.ChunkedArray):
        flattened = [chunk.storage.flatten() for chunk in obj.chunks]
        return (pa.chunked_array(dim) for dim in zip(*flattened))
    else:
        return obj.storage.flatten()


def to_geopandas(obj):
    """Convert a geoarrow-like array into a ``geopandas.GeoSeries``.

    >>> import geoarrow.pyarrow as ga
    >>> array = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.to_geopandas(array)
    0    POINT (0.00000 1.00000)
    dtype: geometry
    """
    import pandas as pd
    import geopandas

    # Ideally we will avoid serialization via geobuffers + from_ragged_array()
    wkb_array_or_chunked = as_wkb(obj)

    # Avoids copy on convert to pandas
    wkb_pandas = pd.Series(
        wkb_array_or_chunked,
        dtype=pd.ArrowDtype(wkb_array_or_chunked.type.storage_type),
    )

    return geopandas.GeoSeries.from_wkb(wkb_pandas, crs=wkb_array_or_chunked.type.crs)
