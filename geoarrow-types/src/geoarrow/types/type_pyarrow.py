from contextlib import contextmanager
from typing import Optional

from geoarrow.types.type_spec import TypeSpec, type_spec
from geoarrow.types.crs import Crs
from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    CoordType,
    EdgeType,
)

import pyarrow as pa
from pyarrow import types as pa_types


class GeometryExtensionType(pa.ExtensionType):
    """Extension type base class for vector geometry types."""

    _extension_name = None
    _array_cls_from_name = None
    _scalar_cls_from_name = None

    def __init__(
        self, spec: TypeSpec, *, storage_type=None, validate_storage_type=True
    ):
        if not isinstance(spec, TypeSpec):
            raise TypeError("GeometryExtensionType must be created from a TypeSpec")

        self._spec = spec.canonicalize()

        if self._spec.extension_name() != type(self)._extension_name:
            raise ValueError(
                f"Expected BaseType with extension name "
                f'"{type(self)._extension_name}" but got "{self._spec.extension_name()}"'
            )

        if storage_type is None:
            if self._spec.encoding == Encoding.GEOARROW:
                key = (
                    self._spec.geometry_type,
                    self._spec.coord_type,
                    self._spec.dimensions,
                )
                storage_type = _NATIVE_STORAGE_TYPES[key]
            else:
                storage_type = _SERIALIZED_STORAGE_TYPES[self._spec.encoding]
        elif validate_storage_type:
            _validate_storage_type(storage_type, self._spec)

        pa.ExtensionType.__init__(self, storage_type, self._spec.extension_name())

    def __repr__(self):
        return f"{type(self).__name__}({_spec_short_repr(self.spec, self._extension_name)})"

    def __arrow_ext_serialize__(self):
        return self._spec.extension_metadata().encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return _deserialize_storage(
            storage_type, cls._extension_name, serialized.decode()
        )

    def to_pandas_dtype(self):
        # Note that returning geopandas.array.GeometryDtype() here
        # doesn't result in a GeoSeries or GeoDataFrame.
        from pandas import ArrowDtype

        return ArrowDtype(self)

    def __arrow_ext_class__(self):
        if GeometryExtensionType._array_cls_from_name:
            return GeometryExtensionType._array_cls_from_name(self.extension_name)
        else:
            return super().__arrow_ext_class__()

    def __arrow_ext_scalar_class__(self):
        if GeometryExtensionType._scalar_cls_from_name:
            return GeometryExtensionType._scalar_cls_from_name(self.extension_name)
        else:
            return super().__arrow_ext_scalar_class__()

    def from_geobuffers(self, *args, **kwargs):
        """Create an array from the appropriate number of buffers
        for this type.
        """
        raise NotImplementedError()

    def wrap_array(self, storage):
        # Often this storage has the correct type except for nullable/
        # non/nullable-ness of children. First check for the easy case
        # (exactly correct storage type).
        if storage.type == self.storage_type:
            return super().wrap_array(storage)

        # A cast won't work because pyarrow won't cast nullable to
        # non-nullable; however, we can attempt to export to C and
        # reimport against this after making sure that the storage parses
        # to the appropriate geometry type.

        # Handle ChunkedArray
        if isinstance(storage, pa.ChunkedArray):
            chunks = [self.wrap_array(chunk) for chunk in storage.chunks]
            return pa.chunked_array(chunks, self)

        _, c_array = storage.__arrow_c_array__()
        c_schema = self.storage_type.__arrow_c_schema__()
        storage = pa.Array._import_from_c_capsule(c_schema, c_array)
        return super().wrap_array(storage)

    @property
    def spec(self) -> TypeSpec:
        return self._spec

    @property
    def encoding(self) -> Encoding:
        return self._spec.encoding

    @property
    def geometry_type(self) -> GeometryType:
        """The :class:`GeometryType` of this type or ``GEOMETRY`` for
        types where this is not constant (i.e., WKT and WKB).

        >>> import geoarrow.pyarrow as ga
        >>> ga.wkb().geometry_type == ga.GeometryType.GEOMETRY
        True
        >>> ga.linestring().geometry_type == ga.GeometryType.LINESTRING
        True
        """
        return self._spec.geometry_type

    @property
    def dimensions(self) -> Dimensions:
        """The :class:`Dimensions` of this type or ``UNKNOWN`` for
        types where this is not constant (i.e., WKT and WKT).

        >>> import geoarrow.pyarrow as ga
        >>> ga.wkb().dimensions == ga.Dimensions.UNKNOWN
        True
        >>> ga.linestring().dimensions == ga.Dimensions.XY
        True
        """
        return self._spec.dimensions

    @property
    def coord_type(self) -> CoordType:
        """The :class:`CoordType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().coord_type == ga.CoordType.SEPARATED
        True
        >>> ga.linestring().with_coord_type(ga.CoordType.INTERLEAVED).coord_type
        <CoordType.INTERLEAVED: 2>
        """
        return self._spec.coord_type

    @property
    def edge_type(self) -> EdgeType:
        """The :class:`EdgeType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().edge_type == ga.EdgeType.PLANAR
        True
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL).edge_type
        <EdgeType.SPHERICAL: 2>
        """
        return self._spec.edge_type

    @property
    def crs(self) -> Optional[Crs]:
        """The coordinate reference system of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_crs(ga.OGC_CRS84).crs
        ProjJsonCrs(OGC:CRS84)
        """
        return self._spec.crs

    def with_metadata(self, metadata):
        """This type with the extension metadata (e.g., copied from some other type)
        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().with_metadata('{"edges": "spherical"}').edge_type
        <EdgeType.SPHERICAL: 2>
        """
        if isinstance(metadata, str):
            metadata = metadata.encode("UTF-8")
        return type(self).__arrow_ext_deserialize__(self.storage_type, metadata)

    def with_geometry_type(self, geometry_type):
        """Returns a new type with the specified :class:`geoarrow.GeometryType`.
        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_geometry_type(ga.GeometryType.LINESTRING)
        LinestringType(geoarrow.linestring)
        """
        spec = type_spec(Encoding.GEOARROW, geometry_type=geometry_type)
        spec = TypeSpec.coalesce(spec, self.spec).canonicalize()
        return extension_type(spec)

    def with_dimensions(self, dimensions):
        """Returns a new type with the specified :class:`geoarrow.Dimensions`.
        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_dimensions(ga.Dimensions.XYZ)
        PointType(geoarrow.point_z)
        """
        spec = type_spec(dimensions=dimensions)
        spec = TypeSpec.coalesce(spec, self.spec).canonicalize()
        return extension_type(spec)

    def with_coord_type(self, coord_type):
        """Returns a new type with the specified :class:`geoarrow.CoordType`.
        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_coord_type(ga.CoordType.INTERLEAVED)
        PointType(interleaved geoarrow.point)
        """
        spec = type_spec(coord_type=coord_type)
        spec = TypeSpec.coalesce(spec, self.spec).canonicalize()
        return extension_type(spec)

    def with_edge_type(self, edge_type):
        """Returns a new type with the specified :class:`geoarrow.EdgeType`.
        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL)
        LinestringType(spherical geoarrow.linestring)
        """
        spec = type_spec(edge_type=edge_type)
        spec = TypeSpec.coalesce(spec, self.spec).canonicalize()
        return extension_type(spec)

    def with_crs(self, crs):
        """Returns a new type with the specified coordinate reference system
        :class:`geoarrow.CrsType` combination.
        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().with_crs(ga.OGC_CRS84)
        LinestringType(geoarrow.linestring <ProjJsonCrs(OGC:CRS84)>)
        """
        spec = type_spec(crs=crs)
        spec = TypeSpec.coalesce(spec, self.spec).canonicalize()
        return extension_type(spec)


class WkbType(GeometryExtensionType):
    """Extension type whose storage is a binary or large binary array of
    well-known binary. Even though the draft specification currently mandates
    ISO well-known binary, EWKB is supported by the parser.
    """

    _extension_name = "geoarrow.wkb"


class WktType(GeometryExtensionType):
    """Extension type whose storage is a utf8 or large utf8 array of
    well-known text.
    """

    _extension_name = "geoarrow.wkt"


class GeometryUnionType(GeometryExtensionType):
    _extension_name = "geoarrow.geometry"


class GeometryCollectionUnionType(GeometryExtensionType):
    _extension_name = "geoarrow.geometrycollection"


class BoxType(GeometryExtensionType):
    """Extension type whose storage is an array of boxes stored
    as a struct with two children per dimension.
    """

    _extension_name = "geoarrow.box"

    def from_geobuffers(self, validity, xmin, ymin, *bounds):
        storage = _from_buffers_box(self.storage_type, validity, xmin, ymin, *bounds)
        return self.wrap_array(storage)


class PointType(GeometryExtensionType):
    """Extension type whose storage is an array of points stored
    as either a struct with one child per dimension or a fixed-size
    list whose single child is composed of interleaved ordinate values.
    """

    _extension_name = "geoarrow.point"

    def from_geobuffers(self, validity, x, y=None, z_or_m=None, m=None):
        storage = _from_buffers_point(self.storage_type, validity, x, y, z_or_m, m)
        return self.wrap_array(storage)


class LinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of linestrings stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.linestring"

    def from_geobuffers(self, validity, coord_offsets, x, y=None, z_or_m=None, m=None):
        storage = _from_buffers_linestring(
            self.storage_type, validity, coord_offsets, x, y, z_or_m, m
        )
        return self.wrap_array(storage)


class PolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.polygon"

    def from_geobuffers(
        self, validity, ring_offsets, coord_offsets, x, y=None, z_or_m=None, m=None
    ):
        storage = _from_buffers_polygon(
            self.storage_type, validity, ring_offsets, coord_offsets, x, y, z_or_m, m
        )
        return self.wrap_array(storage)


class MultiPointType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipoint"

    def from_geobuffers(self, validity, coord_offsets, x, y=None, z_or_m=None, m=None):
        storage = _from_buffers_linestring(
            self.storage_type, validity, coord_offsets, x, y, z_or_m, m
        )
        return self.wrap_array(storage)


class MultiLinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of multilinestrings stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multilinestring"

    def from_geobuffers(
        self,
        validity,
        linestring_offsets,
        coord_offsets,
        x,
        y=None,
        z_or_m=None,
        m=None,
    ):
        storage = _from_buffers_polygon(
            self.storage_type,
            validity,
            linestring_offsets,
            coord_offsets,
            x,
            y,
            z_or_m,
            m,
        )
        return self.wrap_array(storage)


class MultiPolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of multilinestrings stored
    as a list of a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipolygon"

    def from_geobuffers(
        self,
        validity,
        polygon_offsets,
        ring_offsets,
        coord_offsets,
        x,
        y=None,
        z_or_m=None,
        m=None,
    ):
        storage = _from_buffers_multipolygon(
            self.storage_type,
            validity,
            polygon_offsets,
            ring_offsets,
            coord_offsets,
            x,
            y,
            z_or_m,
            m,
        )
        return self.wrap_array(storage)


def extension_type(
    spec: TypeSpec, storage_type=None, validate_storage_type=True
) -> GeometryExtensionType:
    spec = type_spec(spec).with_defaults()
    extension_cls = _EXTENSION_CLASSES[spec.extension_name()]
    return extension_cls(
        spec, storage_type=storage_type, validate_storage_type=validate_storage_type
    )


def storage_type(spec: TypeSpec) -> pa.DataType:
    spec = spec.with_defaults()

    if spec.encoding == Encoding.GEOARROW:
        key = spec.geometry_type, spec.coord_type, spec.dimensions
        return _NATIVE_STORAGE_TYPES[key]
    else:
        return _SERIALIZED_STORAGE_TYPES[spec.encoding]


_extension_types_registered = False


def extension_types_registered() -> bool:
    """Check if PyArrow geometry types were registered

    Returns ``True`` if the GeoArrow extension types were registered by
    this module or ``False`` otherwise.
    """
    global _extension_types_registered

    return _extension_types_registered


@contextmanager
def registered_extension_types():
    """Context manager to perform some action with extension types registered"""
    if extension_types_registered():
        yield
        return

    register_extension_types()
    try:
        yield
    finally:
        unregister_extension_types()


@contextmanager
def unregistered_extension_types():
    """Context manager to perform some action without extension types registered"""
    if not extension_types_registered():
        yield
        return

    unregister_extension_types()
    try:
        yield
    finally:
        register_extension_types()


def register_extension_types(lazy: bool = True) -> None:
    """Register PyArrow geometry extension types

    Register the extension types in the geoarrow namespace with the pyarrow
    registry. This enables geoarrow types to be read, written, imported, and
    exported like any other Arrow type.

    Parameters
    ----------
    lazy : bool
        Skip the registration process if this function has already been called.
    """
    global _extension_types_registered

    if lazy and _extension_types_registered is True:
        return

    _extension_types_registered = None

    all_types = [
        type_spec(Encoding.WKT).to_pyarrow(),
        type_spec(Encoding.WKB).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.BOX).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.GEOMETRY).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.POINT).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.LINESTRING).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.POLYGON).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.MULTIPOINT).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.MULTILINESTRING).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.MULTIPOLYGON).to_pyarrow(),
        type_spec(Encoding.GEOARROW, GeometryType.GEOMETRYCOLLECTION).to_pyarrow(),
    ]

    n_registered = 0
    for t in all_types:
        try:
            pa.register_extension_type(t)
            n_registered += 1
        except pa.ArrowException:
            pass

    if n_registered != len(all_types):
        raise RuntimeError("Failed to register one or more extension types")

    _extension_types_registered = True


def unregister_extension_types(lazy=True):
    """Unregister extension types in the geoarrow namespace."""
    global _extension_types_registered

    if lazy and _extension_types_registered is False:
        return

    _extension_types_registered = None

    all_type_names = [
        "geoarrow.wkb",
        "geoarrow.wkt",
        "geoarrow.box",
        "geoarrow.geometry",
        "geoarrow.point",
        "geoarrow.linestring",
        "geoarrow.polygon",
        "geoarrow.multipoint",
        "geoarrow.multilinestring",
        "geoarrow.multipolygon",
        "geoarrow.geometrycollection",
    ]

    n_unregistered = 0
    for t_name in all_type_names:
        try:
            pa.unregister_extension_type(t_name)
            n_unregistered += 1
        except pa.ArrowException:
            pass

    if n_unregistered != len(all_type_names):
        raise RuntimeError("Failed to unregister one or more extension types")

    _extension_types_registered = False


def _parse_storage(storage_type):
    """Simplified pyarrow type representation

    Distill a pyarrow type into the components we need to validate it.
    This will return a list where each element is a node. All elements
    will represent a list node except for the last node (which may be
    coordinates for native types or data for serialized types).
    """
    if pa_types.is_binary(storage_type):
        return [("binary", ())]
    elif pa_types.is_large_binary(storage_type):
        return [("large_binary", ())]
    elif pa_types.is_string(storage_type):
        return [("string", ())]
    elif pa_types.is_large_string(storage_type):
        return [("large_string", ())]
    elif hasattr(pa_types, "is_binary_view") and pa_types.is_binary_view(storage_type):
        return [("binary_view", ())]
    elif hasattr(pa_types, "is_string_view") and pa_types.is_string_view(storage_type):
        return [("string_view", ())]
    elif pa_types.is_float64(storage_type):
        return [("double", ())]
    elif isinstance(storage_type, pa.ListType):
        f = storage_type.field(0)
        return [("list", (f.name,))] + _parse_storage(f.type)
    elif isinstance(storage_type, pa.DenseUnionType):
        n_fields = storage_type.num_fields
        names = tuple(str(code) for code in storage_type.type_codes)
        parsed_children = tuple(
            _parse_storage(storage_type.field(i).type)[0] for i in range(n_fields)
        )
        return [("dense_union", (names, parsed_children))]
    elif isinstance(storage_type, pa.StructType):
        n_fields = storage_type.num_fields
        names = tuple(storage_type.field(i).name for i in range(n_fields))
        parsed_children = tuple(
            _parse_storage(storage_type.field(i).type)[0] for i in range(n_fields)
        )
        return [("struct", (names, parsed_children))]
    elif isinstance(storage_type, pa.FixedSizeListType):
        f = storage_type.field(0)
        return [
            (
                "fixed_size_list",
                (f.name, storage_type.list_size, (_parse_storage(f.type)[0],)),
            ),
        ]
    else:
        raise ValueError(f"Type {storage_type} is not a valid GeoArrow type component")


def _validate_storage_type(storage_type, spec):
    """Validate a storage type against a TypeSpec

    We don't currently call any constructor with validate_storage_type=True,
    but if somebody does this it should error until implemented.
    """
    raise NotImplementedError()


def _deserialize_storage(storage_type, extension_name=None, extension_metadata=None):
    """Deserialize storage, extension name, and extension metadata

    This is implemented in such a way that it could be reused for another backend
    (i.e., only requires the "parsed" representation).
    """
    parsed = _parse_storage(storage_type)

    parsed_type_names = tuple(item[0] for item in parsed)

    if parsed_type_names not in _SPEC_FROM_TYPE_NESTING:
        raise ValueError(f"Can't guess encoding from type nesting {parsed_type_names}")

    spec = _SPEC_FROM_TYPE_NESTING[parsed_type_names]
    spec = TypeSpec.from_extension_metadata(extension_metadata).with_defaults(spec)

    # If this is a serialized type or a union, we don't need to infer any more information
    # from the storage type (because we don't currently validate union types).
    if spec.encoding.is_serialized() or spec.geometry_type in (
        GeometryType.GEOMETRY,
        GeometryType.GEOMETRYCOLLECTION,
    ):
        if extension_name is not None and spec.extension_name() != extension_name:
            raise ValueError(f"Can't interpret {storage_type} as {extension_name}")

        return extension_type(spec, storage_type, validate_storage_type=False)

    # Infer dimensions and verify coordinate types
    type_name, params = parsed[-1]
    if type_name == "struct":
        names, parsed_children = params
        n_dims = len(names)
        # We could use len(names) here, but we don't actually want to
        # infer the dimensions if the struct field names are not valid
        # (because it is typically very easy to set a struct's field names,
        # as opposed to the fixed-size list where not all implementations
        # expose that behaviour)
        n_dims_infer = -1
    else:
        names, n_dims, parsed_children = params
        n_dims_infer = n_dims

    # Make sure we catch box field names (e.g., xmin, ymin, ...)
    if names in _BOX_DIMS_FROM_NAMES:
        if spec.geometry_type != GeometryType.POINT:
            raise ValueError(
                f"Expected box names {names} in root type but got nested list"
            )
        spec = spec.override(geometry_type=GeometryType.BOX)
        dims = _BOX_DIMS_FROM_NAMES[names]
    elif names in _DIMS_FROM_NAMES:
        dims = _DIMS_FROM_NAMES[names]
        if n_dims != dims.count():
            raise ValueError(f"Expected {n_dims} dimensions but got Dimensions.{dims}")
    elif n_dims_infer == 2:
        dims = Dimensions.XY
    elif n_dims_infer == 4:
        dims = Dimensions.XYZM
    else:
        raise ValueError(f"Can't infer dimensions from coord field names {names}")

    for parsed_child in parsed_children:
        if parsed_child[0] != "double":
            raise ValueError(
                f"Expected coordinate double coordinate values but got {parsed_child[0]}"
            )

    spec = spec.with_defaults(dims)

    # Infer geometry type from extension name
    if extension_name is not None:
        spec_from_name = TypeSpec.from_extension_name(extension_name)

        # Ensure that if the spec from the name has a different geometry_type
        # value that this errors.
        spec = TypeSpec.coalesce_unspecified(spec, spec_from_name)

    # Construct the extension type
    return extension_type(spec, storage_type, validate_storage_type=False)


def _struct_fields(dims):
    return pa.struct([pa.field(c, pa.float64(), nullable=False) for c in dims])


def _interleaved_fields(dims):
    return pa.list_(pa.field(dims, pa.float64(), nullable=False), len(dims))


def _nested_field(coord, names):
    if len(names) == 1:
        return pa.field(names[-1], coord, nullable=False)
    else:
        inner_type = pa.list_(_nested_field(coord, names[:-1]))
        return pa.field(names[-1], inner_type, nullable=False)


def _nested_type(coord, names):
    if len(names) > 0:
        return pa.list_(_nested_field(coord, names))
    else:
        return coord


def _from_buffer_ordinate(x):
    mv = memoryview(x)
    if mv.format != "d":
        mv = mv.cast("d")

    return pa.array(mv, pa.float64())


def _pybuffer_offset(x):
    mv = memoryview(x)
    if mv.format != "i":
        mv = mv.cast("i")

    return len(mv), pa.py_buffer(mv)


def _from_buffers_box(type_, validity, *bounds):
    length = len(bounds[0])
    validity = pa.py_buffer(validity) if validity is not None else None
    children = [_from_buffer_ordinate(bound) for bound in bounds]
    return pa.Array.from_buffers(type_, length, buffers=[validity], children=children)


def _from_buffers_point(type_, validity, x, y=None, z_or_m=None, m=None):
    validity = pa.py_buffer(validity) if validity is not None else None
    children = [_from_buffer_ordinate(x)]
    if y is not None:
        children.append(_from_buffer_ordinate(y))
    if z_or_m is not None:
        children.append(_from_buffer_ordinate(z_or_m))
    if m is not None:
        children.append(_from_buffer_ordinate(m))

    if pa_types.is_fixed_size_list(type_):
        length = len(x) // type_.list_size
    else:
        length = len(x)

    return pa.Array.from_buffers(type_, length, buffers=[validity], children=children)


def _from_buffers_linestring(
    type_, validity, coord_offsets, x, y=None, z_or_m=None, m=None
):
    validity = pa.py_buffer(validity) if validity is not None else None
    n_offsets, coord_offsets = _pybuffer_offset(coord_offsets)
    coords = _from_buffers_point(type_.field(0).type, None, x, y, z_or_m, m)
    return pa.Array.from_buffers(
        type_,
        n_offsets - 1,
        buffers=[validity, pa.py_buffer(coord_offsets)],
        children=[coords],
    )


def _from_buffers_polygon(
    type_, validity, ring_offsets, coord_offsets, x, y=None, z_or_m=None, m=None
):
    validity = pa.py_buffer(validity) if validity is not None else None
    rings = _from_buffers_linestring(
        type_.field(0).type, None, coord_offsets, x, y, z_or_m, m
    )
    n_offsets, ring_offsets = _pybuffer_offset(ring_offsets)
    return pa.Array.from_buffers(
        type_,
        n_offsets - 1,
        buffers=[validity, pa.py_buffer(ring_offsets)],
        children=[rings],
    )


def _from_buffers_multipolygon(
    type_,
    validity,
    polygon_offsets,
    ring_offsets,
    coord_offsets,
    x,
    y=None,
    z_or_m=None,
    m=None,
):
    validity = pa.py_buffer(validity) if validity is not None else None
    polygons = _from_buffers_polygon(
        type_.field(0).type, None, ring_offsets, coord_offsets, x, y, z_or_m, m
    )
    n_offsets, polygon_offsets = _pybuffer_offset(polygon_offsets)
    return pa.Array.from_buffers(
        type_,
        n_offsets - 1,
        buffers=[validity, pa.py_buffer(ring_offsets)],
        children=[polygons],
    )


ALL_DIMENSIONS = [Dimensions.XY, Dimensions.XYZ, Dimensions.XYM, Dimensions.XYZM]
ALL_COORD_TYPES = [CoordType.INTERLEAVED, CoordType.SEPARATED]
ALL_GEOMETRY_TYPES = [
    GeometryType.POINT,
    GeometryType.LINESTRING,
    GeometryType.POLYGON,
    GeometryType.MULTIPOINT,
    GeometryType.MULTILINESTRING,
    GeometryType.MULTIPOLYGON,
    GeometryType.GEOMETRYCOLLECTION,
]
ALL_GEOMETRY_TYPES_EXCEPT_GEOMETRYCOLLECTION = [
    GeometryType.POINT,
    GeometryType.LINESTRING,
    GeometryType.POLYGON,
    GeometryType.MULTIPOINT,
    GeometryType.MULTILINESTRING,
    GeometryType.MULTIPOLYGON,
]
_BOX_DIMS_FROM_NAMES = {
    ("xmin", "ymin", "xmax", "ymax"): Dimensions.XY,
    ("xmin", "ymin", "zmin", "xmax", "ymax", "zmax"): Dimensions.XYZ,
    ("xmin", "ymin", "mmin", "xmax", "ymax", "mmax"): Dimensions.XYM,
    ("xmin", "ymin", "zmin", "mmin", "xmax", "ymax", "zmax", "mmax"): Dimensions.XYZM,
}
_BOX_NAMES_FROM_DIMS = {v: k for k, v in _BOX_DIMS_FROM_NAMES.items()}


def _generate_storage_types():
    coord_storage = {
        (CoordType.SEPARATED, Dimensions.XY): _struct_fields("xy"),
        (CoordType.SEPARATED, Dimensions.XYZ): _struct_fields("xyz"),
        (CoordType.SEPARATED, Dimensions.XYM): _struct_fields("xym"),
        (CoordType.SEPARATED, Dimensions.XYZM): _struct_fields("xyzm"),
        (CoordType.INTERLEAVED, Dimensions.XY): _interleaved_fields("xy"),
        (CoordType.INTERLEAVED, Dimensions.XYZ): _interleaved_fields("xyz"),
        (CoordType.INTERLEAVED, Dimensions.XYM): _interleaved_fields("xym"),
        (CoordType.INTERLEAVED, Dimensions.XYZM): _interleaved_fields("xyzm"),
    }

    field_names = {
        GeometryType.POINT: [],
        GeometryType.LINESTRING: ["vertices"],
        GeometryType.POLYGON: ["rings", "vertices"],
        GeometryType.MULTIPOINT: ["points"],
        GeometryType.MULTILINESTRING: ["linestrings", "vertices"],
        GeometryType.MULTIPOLYGON: ["polygons", "rings", "vertices"],
    }

    all_storage_types = {}
    for geometry_type in ALL_GEOMETRY_TYPES_EXCEPT_GEOMETRYCOLLECTION:
        for coord_type in ALL_COORD_TYPES:
            for dimensions in ALL_DIMENSIONS:
                names = field_names[geometry_type]
                coord = coord_storage[(coord_type, dimensions)]
                key = geometry_type, coord_type, dimensions
                storage_type = _nested_type(coord, names)
                all_storage_types[key] = storage_type

    for dimensions in ALL_DIMENSIONS:
        storage_type = _nested_type(
            _struct_fields(_BOX_NAMES_FROM_DIMS[dimensions]), []
        )
        key = GeometryType.BOX, CoordType.SEPARATED, dimensions
        all_storage_types[key] = storage_type

    return all_storage_types


def _generate_union_storage(
    geometry_types=ALL_GEOMETRY_TYPES,
    dimensions=ALL_DIMENSIONS,
    coord_type=CoordType.SEPARATED,
):
    child_fields = []
    type_codes = []
    for dimension in dimensions:
        for geometry_type in geometry_types:
            spec = type_spec(
                encoding=Encoding.GEOARROW,
                geometry_type=geometry_type,
                dimensions=dimension,
                coord_type=coord_type,
            )

            if spec.geometry_type == GeometryType.GEOMETRYCOLLECTION:
                storage_type = _generate_union_collection_storage(
                    spec.dimensions, coord_type
                )
            else:
                storage_type = extension_type(spec).storage_type

            type_id = _UNION_TYPE_ID_FROM_SPEC[(spec.geometry_type, spec.dimensions)]
            geometry_type_lab = _UNION_GEOMETRY_TYPE_LABELS[spec.geometry_type.value]
            dimension_lab = _UNION_DIMENSION_LABELS[spec.dimensions.value]

            child_fields.append(
                pa.field(f"{geometry_type_lab}{dimension_lab}", storage_type)
            )
            type_codes.append(type_id)

    return pa.dense_union(child_fields, type_codes)


def _generate_union_collection_storage(dimensions, coord_type):
    storage_union = _generate_union_storage(
        geometry_types=ALL_GEOMETRY_TYPES_EXCEPT_GEOMETRYCOLLECTION,
        dimensions=[dimensions],
        coord_type=coord_type,
    )
    storage_union_field = pa.field("geometries", storage_union, nullable=False)
    return pa.list_(storage_union_field)


def _generate_union_type_id_mapping():
    out = {}
    for dimension in ALL_DIMENSIONS:
        for geometry_type in ALL_GEOMETRY_TYPES:
            type_id = (dimension.value - 1) * 10 + geometry_type.value
            out[type_id] = (geometry_type, dimension)
    return out


def _add_union_types_to_native_storage_types():
    global _NATIVE_STORAGE_TYPES

    for coord_type in ALL_COORD_TYPES:
        for dimension in ALL_DIMENSIONS:
            _NATIVE_STORAGE_TYPES[
                (GeometryType.GEOMETRY, coord_type, dimension)
            ] = _generate_union_storage(coord_type=coord_type, dimensions=[dimension])

        # With unknown dimensions, we reigster the massive catch-all union
        _NATIVE_STORAGE_TYPES[
            (GeometryType.GEOMETRY, coord_type, Dimensions.UNKNOWN)
        ] = _generate_union_storage(coord_type=coord_type)

    for coord_type in ALL_COORD_TYPES:
        for dimension in ALL_DIMENSIONS:
            _NATIVE_STORAGE_TYPES[
                (GeometryType.GEOMETRYCOLLECTION, coord_type, dimension)
            ] = _generate_union_collection_storage(dimension, coord_type)


# A shorter version of repr(spec) that matches what geoarrow-c used to do
# (to reduce mayhem on docstring updates).
def _spec_short_repr(spec, ext_name):
    non_planar = spec.edge_type != EdgeType.PLANAR
    interleaved = spec.coord_type == CoordType.INTERLEAVED

    if spec.dimensions == Dimensions.XYZM:
        dims = "_zm"
    elif spec.dimensions == Dimensions.XYZ:
        dims = "_z"
    elif spec.dimensions == Dimensions.XYM:
        dims = "_m"
    else:
        dims = ""

    if non_planar and interleaved:
        type_prefix = f"{spec.edge_type.name.lower()} interleaved "
    elif non_planar:
        type_prefix = f"{spec.edge_type.name.lower()} "
    elif interleaved:
        type_prefix = "interleaved "
    else:
        type_prefix = ""

    if spec.crs is not None:
        crs = f" <{repr(spec.crs)}>"
    else:
        crs = ""

    if len(crs) > 40:
        crs = crs[:36] + "...>"

    return f"{type_prefix}{ext_name}{dims}{crs}"


_EXTENSION_CLASSES = {
    "geoarrow.wkb": WkbType,
    "geoarrow.wkt": WktType,
    "geoarrow.box": BoxType,
    "geoarrow.geometry": GeometryUnionType,
    "geoarrow.point": PointType,
    "geoarrow.linestring": LinestringType,
    "geoarrow.polygon": PolygonType,
    "geoarrow.multipoint": MultiPointType,
    "geoarrow.multilinestring": MultiLinestringType,
    "geoarrow.multipolygon": MultiPolygonType,
    "geoarrow.geometrycollection": GeometryCollectionUnionType,
}


_SPEC_FROM_UNION_TYPE_ID = _generate_union_type_id_mapping()
_UNION_TYPE_ID_FROM_SPEC = {v: k for k, v in _SPEC_FROM_UNION_TYPE_ID.items()}

_UNION_GEOMETRY_TYPE_LABELS = [
    "Geometry",
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
]

_UNION_DIMENSION_LABELS = [None, "", " Z", " M", " ZM"]

_SERIALIZED_STORAGE_TYPES = {
    Encoding.WKT: pa.utf8(),
    Encoding.LARGE_WKT: pa.large_utf8(),
    Encoding.WKB: pa.binary(),
    Encoding.LARGE_WKB: pa.large_binary(),
}

if hasattr(pa, "binary_view"):
    _SERIALIZED_STORAGE_TYPES[Encoding.WKT_VIEW] = pa.string_view()
    _SERIALIZED_STORAGE_TYPES[Encoding.WKB_VIEW] = pa.binary_view()

_NATIVE_STORAGE_TYPES = _generate_storage_types()
_add_union_types_to_native_storage_types()

_SPEC_FROM_TYPE_NESTING = {
    ("binary",): Encoding.WKB,
    ("large_binary",): Encoding.LARGE_WKB,
    ("string",): Encoding.WKT,
    ("large_string",): Encoding.LARGE_WKT,
    ("binary_view",): Encoding.WKB_VIEW,
    ("string_view",): Encoding.WKT_VIEW,
    ("struct",): TypeSpec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.POINT,
        coord_type=CoordType.SEPARATED,
    ),
    ("dense_union",): TypeSpec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.GEOMETRY,
    ),
    ("list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.SEPARATED
    ),
    ("list", "list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.SEPARATED
    ),
    ("list", "list", "list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW,
        coord_type=CoordType.SEPARATED,
        geometry_type=GeometryType.MULTIPOLYGON,
    ),
    ("fixed_size_list",): TypeSpec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.POINT,
        coord_type=CoordType.INTERLEAVED,
    ),
    ("list", "fixed_size_list"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.INTERLEAVED
    ),
    ("list", "list", "fixed_size_list"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.INTERLEAVED
    ),
    ("list", "list", "list", "fixed_size_list"): TypeSpec(
        encoding=Encoding.GEOARROW,
        coord_type=CoordType.INTERLEAVED,
        geometry_type=GeometryType.MULTIPOLYGON,
    ),
    ("list", "dense_union"): TypeSpec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.GEOMETRYCOLLECTION,
    ),
}

_DIMS_FROM_NAMES = {
    "xy": Dimensions.XY,
    "xyz": Dimensions.XYZ,
    "xym": Dimensions.XYM,
    "xyzm": Dimensions.XYZM,
    ("x", "y"): Dimensions.XY,
    ("x", "y", "z"): Dimensions.XYZ,
    ("x", "y", "m"): Dimensions.XYM,
    ("x", "y", "z", "m"): Dimensions.XYZM,
}
