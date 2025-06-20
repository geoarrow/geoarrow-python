import pyarrow as pa
import pyarrow.compute as pc

from geoarrow.types import (
    type_spec,
    Encoding,
    CoordType,
    Dimensions,
    EdgeType,
    GeometryType,
    TypeSpec,
)
from geoarrow.pyarrow import _type
from geoarrow.pyarrow._array import array
from geoarrow.pyarrow._kernel import Kernel, _geoarrow_c


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
                    "geometry_type": obj.type.geometry_type.value,
                    "dimensions": obj.type.dimensions.value,
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
            dimensions = Dimensions.XYZM.value
            item_int -= 3000
        elif item_int >= 2000:
            dimensions = Dimensions.XYM.value
            item_int -= 2000
        elif item_int >= 1000:
            dimensions = Dimensions.XYZ.value
            item_int -= 1000
        else:
            dimensions = Dimensions.XY.value

        py_geometry_types.append({"geometry_type": item_int, "dimensions": dimensions})

    return pa.array(py_geometry_types, type=out_type)


def infer_type_common(obj, coord_type=None, promote_multi=False, _geometry_types=None):
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
        coord_type = CoordType.SEPARATED

    if _geometry_types is None:
        types = unique_geometry_types(obj)
    else:
        types = _geometry_types

    if len(types) == 0:
        # Not ideal: we probably want a _type.empty() that keeps the CRS
        return pa.null()

    types = types.flatten()

    dims = [Dimensions(dim) for dim in types[1].to_pylist()]
    dims = Dimensions.common(*dims)

    geometry_types = [
        GeometryType(geometry_type) for geometry_type in types[0].to_pylist()
    ]
    geometry_type = GeometryType.common(*geometry_types)

    if promote_multi and geometry_type.value in (1, 2, 3):
        geometry_type = GeometryType(geometry_type.value + 3)

    if geometry_type == GeometryType.GEOMETRY:
        spec = type_spec(Encoding.WKB)
    else:
        spec = type_spec(Encoding.GEOARROW, dims, geometry_type, coord_type=coord_type)

    spec = TypeSpec.coalesce(spec, obj.type.spec).canonicalize()
    return _type.extension_type(spec)


def as_wkt(obj):
    """Encode ``obj`` as :func:`geoarrow.pyarrow.wkt`.

    >>> import geoarrow.pyarrow as ga
    >>> points = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.as_wkt(points)
    GeometryExtensionArray:WktType(geoarrow.wkt)[1]
    <POINT (0 1)>
    """
    return as_geoarrow(obj, _type.wkt())


def as_wkb(obj, strict_iso_wkb=False):
    """Encode ``obj`` as :func:`geoarrow.pyarrow.wkb`.

    >>> import geoarrow.pyarrow as ga
    >>> points = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.as_wkb(points)
    GeometryExtensionArray:WkbType(geoarrow.wkb)[1]
    <POINT (0 1)>
    """
    obj = obj_as_array_or_chunked(obj)

    # If we're generating the WKB, we know it will be ISO, so no need to check
    check_wkb = isinstance(obj.type, _type.WkbType)

    obj = as_geoarrow(obj, _type.wkb())

    if check_wkb and strict_iso_wkb and _any_ewkb(obj):
        return push_all(Kernel.as_geoarrow, obj, args={"type_id": 100001})
    else:
        return obj


def _any_ewkb(obj):
    obj = obj_as_array_or_chunked(obj)
    if not isinstance(obj.type, _type.WkbType):
        return False

    if len(obj) == 0:
        return False

    if len(obj) > 1 and _any_ewkb(obj[:1]):
        return True

    import pyarrow.compute as pc

    obj = ensure_storage(obj)
    endian = pc.binary_slice(obj, 0, 1)
    is_little_endian = pc.equal(endian, b"\x01")
    high_byte = pc.if_else(
        is_little_endian, pc.binary_slice(obj, 4, 5), pc.binary_slice(obj, 1, 2)
    )
    return not pc.all(pc.equal(high_byte, b"\x00")).as_py()


def as_geoarrow(obj, type=None, coord_type=None, promote_multi=False):
    """Encode ``obj`` as a geoarrow-encoded array, preferring geoarrow encodings
    and falling back to well-known binary if no common geoemtry type is found.

    >>> import geoarrow.pyarrow as ga
    >>> ga.as_geoarrow(["POINT (0 1)", "MULTIPOINT Z (0 1 2, 4 5 6)"])
    GeometryExtensionArray:MultiPointType(geoarrow.multipoint_z)[2]
    <MULTIPOINT Z (0 1 nan)>
    <MULTIPOINT Z (0 1 2, 4 5 6)>
    """
    obj = obj_as_array_or_chunked(obj)

    if type is None:
        type = infer_type_common(
            obj, coord_type=coord_type, promote_multi=promote_multi
        )

    if obj.type.spec == type.spec:
        return obj

    lib = _geoarrow_c()

    cschema = lib.SchemaHolder()
    type._export_to_c(cschema._addr())
    ctype = lib.CVectorType.FromExtension(cschema)

    return push_all(Kernel.as_geoarrow, obj, args={"type_id": ctype.id})


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


def make_point(x, y, z=None, m=None, crs=None):
    """Create a geoarrow-encoded point array from two or more arrays
    representing x, y, and/or z, and/or m values. In many cases, this
    is a zero-copy operation if the input arrays are already in a
    column-based format (e.g., numpy array, pandas series, or pyarrow
    Array/ChunkedArray).

    >>> import geoarrow.pyarrow as ga
    >>> ga.make_point([1, 2, 3], [4, 5, 6])
    GeometryExtensionArray:PointType(geoarrow.point)[3]
    <POINT (1 4)>
    <POINT (2 5)>
    <POINT (3 6)>
    """
    import pyarrow.compute as pc

    if z is not None and m is not None:
        dimensions = Dimensions.XYZM
        field_names = ["x", "y", "z", "m"]
    elif m is not None:
        dimensions = Dimensions.XYM
        field_names = ["x", "y", "m"]
    elif z is not None:
        dimensions = Dimensions.XYZ
        field_names = ["x", "y", "z"]
    else:
        dimensions = Dimensions.XY
        field_names = ["x", "y"]

    type = _type.extension_type(
        type_spec(Encoding.GEOARROW, GeometryType.POINT, dimensions, crs=crs)
    )
    args = [x, y] + [el for el in [z, m] if el is not None]
    args = [pa.array(el, pa.float64()) for el in args]
    storage = pc.make_struct(*args, field_names=field_names)
    return type.wrap_array(storage)


def _box_point_struct(storage):
    arrays = storage.flatten()
    box_storage = pa.StructArray.from_arrays(
        [arrays[0], arrays[1], arrays[0], arrays[1]],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    return _type.types.box().to_pyarrow().wrap_array(box_storage)


def box(obj):
    """Compute a Cartesian 2D bounding box for each feature in ``obj`` as
    a struct(xmin, xmax, ymin, ymax) array.

    >>> import geoarrow.pyarrow as ga
    >>> ga.box(["LINESTRING (0 10, 34 -1)"]).type
    BoxType(geoarrow.box)
    >>> print(str(ga.box(["LINESTRING (0 10, 34 -1)"])))
    -- is_valid: all not null
    -- child 0 type: double
      [
        0
      ]
    -- child 1 type: double
      [
        -1
      ]
    -- child 2 type: double
      [
        34
      ]
    -- child 3 type: double
      [
        10
      ]
    """

    obj = obj_as_array_or_chunked(obj)

    # Spherical edges aren't supported by this algorithm
    if obj.type.edge_type != EdgeType.PLANAR:
        raise TypeError("Can't compute box of type with non-planar edges")

    # Optimization: a box of points is just x, x, y, y with zero-copy
    # if the coord type is struct
    if (
        obj.type.coord_type == CoordType.SEPARATED
        and len(obj) > 0
        and obj.null_count == 0
    ):
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
        "ymin": out[1][0].as_py(),
        "xmax": out[0][1].as_py(),
        "ymax": out[1][1].as_py(),
    }

    # Apparently pyarrow reorders dict keys when inferring scalar types?
    storage_type = pa.struct([(nm, pa.float64()) for nm in out_dict.keys()])
    storage_array = pa.array([out_dict], storage_type)
    return _type.types.box().to_pyarrow().wrap_array(storage_array)[0]


def box_agg(obj):
    """Compute a Cartesian 2D bounding box for all features in ``obj`` as
    a scalar struct(xmin, xmax, ymin, ymax). Values that are null are currently
    ignored.

    >>> import geoarrow.pyarrow as ga
    >>> ga.box_agg(["POINT (0 10)", "POINT (34 -1)"])
    BoxScalar({'xmin': 0.0, 'ymin': -1.0, 'xmax': 34.0, 'ymax': 10.0})
    """

    obj = obj_as_array_or_chunked(obj)

    # Spherical edges aren't supported by this algorithm
    if obj.type.edge_type != EdgeType.PLANAR:
        raise TypeError("Can't compute box of type with non-planar edges")

    # Optimization: pyarrow's minmax kernel is fast and we can use it if we have struct
    # coords. So far, only a measurable improvement for points.
    if obj.type.coord_type == CoordType.SEPARATED and len(obj) > 0:
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
    GeometryExtensionArray:PointType(interleaved geoarrow.point)[1]
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


def with_crs(obj, crs):
    """Force a :class:`geoarrow.CrsType`/crs value on an array.

    >>> import geoarrow.pyarrow as ga
    >>> ga.with_crs(["POINT (0 1)"], ga.OGC_CRS84)
    GeometryExtensionArray:WktType(geoarrow.wkt <ProjJsonCrs(OGC:CRS84)>)[1]
    <POINT (0 1)>
    """
    obj = obj_as_array_or_chunked(obj)
    new_type = obj.type.with_crs(crs)
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
    GeometryExtensionArray:PointType(geoarrow.point_zm)[1]
    <POINT ZM (0 1 nan nan)>
    >>> ga.with_dimensions(["POINT ZM (0 1 2 3)"], ga.Dimensions.XY)
    GeometryExtensionArray:PointType(geoarrow.point)[1]
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
    GeometryExtensionArray:MultiPointType(geoarrow.multipoint)[1]
    <MULTIPOINT (0 1)>
    >>> ga.with_geometry_type(["MULTIPOINT (0 1)"], ga.GeometryType.POINT)
    GeometryExtensionArray:PointType(geoarrow.point)[1]
    <POINT (0 1)>
    >>> ga.with_geometry_type(["LINESTRING EMPTY", "POINT (0 1)"], ga.GeometryType.POINT)
    GeometryExtensionArray:PointType(geoarrow.point)[2]
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
    """Convert a geoarrow-like array or table into a GeoSeries/DataFrame

    These are thin wrappers around ``GeoSeries.from_arrow()`` and
    ``GeoDataFrame.from_arrow()`` where available, falling back on conversion
    through WKB if using an older version of GeoPandas or an Arrow array type
    that GeoPandas doesn't support.

    >>> import pyarrow as pa
    >>> import geoarrow.pyarrow as ga
    >>> array = ga.as_geoarrow(["POINT (0 1)"])
    >>> ga.to_geopandas(array)
    0    POINT (0 1)
    dtype: geometry
    >>> table = pa.table({"geometry": array})
    >>> ga.to_geopandas(table)
          geometry
    0  POINT (0 1)
    """
    import geopandas
    import pandas as pd

    # Heuristic to detect table-like objects
    is_table_like = (
        hasattr(obj, "schema")
        and not callable(obj.schema)
        and isinstance(obj.schema, pa.Schema)
    )

    # Attempt GeoPandas from_arrow first
    try:
        if is_table_like:
            return geopandas.GeoDataFrame.from_arrow(obj)
        else:
            return geopandas.GeoSeries.from_arrow(obj)
    except ValueError:
        pass
    except TypeError:
        pass
    except AttributeError:
        pass

    if is_table_like:
        obj = pa.table(obj)
        is_geo_column = [
            isinstance(col.type, _type.GeometryExtensionType) for col in obj.columns
        ]
        new_cols = [
            to_geopandas(col) if is_geo else col
            for is_geo, col in zip(is_geo_column, obj.columns)
        ]

        # Set the geometry column if there is exactly one geometry column
        geo_column_names = [
            name for name, is_geo in zip(obj.column_names, is_geo_column) if is_geo
        ]
        geometry = geo_column_names[0] if len(geo_column_names) == 1 else None
        return geopandas.GeoDataFrame(
            {name: col for name, col in zip(obj.column_names, new_cols)},
            geometry=geometry,
        )

    # Fall back on wkb conversion
    wkb_array_or_chunked = as_wkb(obj)

    # Avoids copy on convert to pandas
    wkb_pandas = pd.Series(
        wkb_array_or_chunked,
        dtype=pd.ArrowDtype(wkb_array_or_chunked.type.storage_type),
    )

    crs = wkb_array_or_chunked.type.crs
    if crs is not None:
        crs = crs.to_json()

    return geopandas.GeoSeries.from_wkb(wkb_pandas, crs=crs)
