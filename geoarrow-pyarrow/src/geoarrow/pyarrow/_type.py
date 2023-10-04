import pyarrow as pa

from geoarrow.c import lib


class GeometryExtensionType(pa.ExtensionType):
    """Extension type base class for vector geometry types."""

    _extension_name = None

    # These are injected into the class when imported by the type and scalar
    # modules to avoid a circular import. As a result, you can't
    # use this module directly (import geoarrow.pyarrow first).
    _array_cls_from_name = None
    _scalar_cls_from_name = None

    def __init__(self, c_vector_type):
        if not isinstance(c_vector_type, lib.CVectorType):
            raise TypeError(
                "geoarrow.pyarrow.VectorType must be created from a CVectorType"
            )
        self._type = c_vector_type
        if self._type.extension_name != type(self)._extension_name:
            raise ValueError(
                f'Expected CVectorType with extension name "{type(self)._extension_name}" but got "{self._type.extension_name}"'
            )

        storage_schema = self._type.to_storage_schema()
        storage_type = pa.DataType._import_from_c(storage_schema._addr())
        pa.ExtensionType.__init__(self, storage_type, self._type.extension_name)

    def __repr__(self):
        return f"{type(self).__name__}({repr(self._type)})"

    def __arrow_ext_serialize__(self):
        return self._type.extension_metadata

    @staticmethod
    def _import_from_c(addr):
        field = pa.Field._import_from_c(addr)
        if not field.metadata or "ARROW:extension:name" not in field.metadata:
            return field.type

        schema = lib.SchemaHolder()
        field._export_to_c(schema._addr())

        c_vector_type = lib.CVectorType.FromExtension(schema)
        cls = type_cls_from_name(c_vector_type.extension_name.decode("UTF-8"))
        cls(c_vector_type)

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        schema = lib.SchemaHolder()
        storage_type._export_to_c(schema._addr())

        c_vector_type = lib.CVectorType.FromStorage(
            schema, cls._extension_name.encode("UTF-8"), serialized
        )

        return cls(c_vector_type)

    @staticmethod
    def _from_ctype(c_vector_type):
        cls = type_cls_from_name(c_vector_type.extension_name)
        schema = c_vector_type.to_schema()
        storage_type = pa.DataType._import_from_c(schema._addr())
        return cls.__arrow_ext_deserialize__(
            storage_type, c_vector_type.extension_metadata
        )

    def __arrow_ext_class__(self):
        return GeometryExtensionType._array_cls_from_name(self.extension_name)

    def __arrow_ext_scalar_class__(self):
        return GeometryExtensionType._scalar_cls_from_name(self.extension_name)

    def to_pandas_dtype(self):
        from geoarrow.pandas import GeoArrowExtensionDtype

        return GeoArrowExtensionDtype(self)

    def from_geobuffers(self, *args, **kwargs):
        """Create an array from the appropriate number of buffers
        for this type.
        """
        raise NotImplementedError()

    def _from_geobuffers_internal(self, args):
        builder = lib.CBuilder(self._type.to_schema())
        for i, buf_type, buf in args:
            if buf is None:
                continue
            if buf_type == "uint8":
                builder.set_buffer_uint8(i, buf)
            elif buf_type == "int32":
                builder.set_buffer_int32(i, buf)
            elif buf_type == "double":
                builder.set_buffer_double(i, buf)
            else:
                raise ValueError(f"Unknown type: {buf_type}")

        carray = builder.finish()
        return pa.Array._import_from_c(carray._addr(), self)

    @property
    def geoarrow_id(self):
        """A unique identifier for the memory layout of this type.

        >>> import geoarrow.pyarrow as ga
        >>> int(ga.wkb().geoarrow_id)
        100001
        """
        return self._type.id

    @property
    def geometry_type(self):
        """The :class:`geoarrow.GeometryType` of this type or ``GEOMETRY`` for
        types where this is not constant (i.e., WKT and WKB).

        >>> import geoarrow.pyarrow as ga
        >>> ga.wkb().geometry_type == ga.GeometryType.GEOMETRY
        True
        >>> ga.linestring().geometry_type == ga.GeometryType.LINESTRING
        True
        """
        return self._type.geometry_type

    @property
    def dimensions(self):
        """The :class:`geoarrow.Dimensions` of this type or ``UNKNOWN`` for
        types where this is not constant (i.e., WKT and WKT).

        >>> import geoarrow.pyarrow as ga
        >>> ga.wkb().dimensions == ga.Dimensions.UNKNOWN
        True
        >>> ga.linestring().dimensions == ga.Dimensions.XY
        True
        """
        return self._type.dimensions

    @property
    def coord_type(self):
        """The :class:`geoarrow.CoordType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().coord_type == ga.CoordType.SEPARATE
        True
        >>> ga.linestring().with_coord_type(ga.CoordType.INTERLEAVED).coord_type
        <GeoArrowCoordType.GEOARROW_COORD_TYPE_INTERLEAVED: 2>
        """
        return self._type.coord_type

    @property
    def edge_type(self):
        """The :class:`geoarrow.EdgeType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().edge_type == ga.EdgeType.PLANAR
        True
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL).edge_type
        <GeoArrowEdgeType.GEOARROW_EDGE_TYPE_SPHERICAL: 1>
        """
        return self._type.edge_type

    @property
    def crs_type(self):
        """The :class:`geoarrow.CrsType` of the :attr:`crs` value.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().crs_type == ga.CrsType.NONE
        True
        >>> ga.point().with_crs("EPSG:1234").crs_type
        <GeoArrowCrsType.GEOARROW_CRS_TYPE_UNKNOWN: 1>
        """
        return self._type.crs_type

    @property
    def crs(self):
        """The coordinate reference system of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_crs("EPSG:1234").crs
        'EPSG:1234'
        """
        return self._type.crs.decode("UTF-8")

    def with_metadata(self, metadata):
        """This type with the extension metadata (e.g., copied from some other type)

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_metadata('{"crs": "EPSG:1234"}').crs
        'EPSG:1234'
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
        ctype = self._type.with_geometry_type(geometry_type)
        return _ctype_to_extension_type(ctype)

    def with_dimensions(self, dimensions):
        """Returns a new type with the specified :class:`geoarrow.Dimensions`.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_dimensions(ga.Dimensions.XYZ)
        PointType(geoarrow.point_z)
        """
        ctype = self._type.with_dimensions(dimensions)
        return _ctype_to_extension_type(ctype)

    def with_coord_type(self, coord_type):
        """Returns a new type with the specified :class:`geoarrow.CoordType`.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_coord_type(ga.CoordType.INTERLEAVED)
        PointType(interleaved geoarrow.point)
        """
        ctype = self._type.with_coord_type(coord_type)
        return _ctype_to_extension_type(ctype)

    def with_edge_type(self, edge_type):
        """Returns a new type with the specified :class:`geoarrow.EdgeType`.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL)
        LinestringType(spherical geoarrow.linestring)
        """
        ctype = self._type.with_edge_type(edge_type)
        return _ctype_to_extension_type(ctype)

    def with_crs(self, crs, crs_type=None):
        """Returns a new type with the specified coordinate reference system
        :class:`geoarrow.CrsType` combination. The ``crs_type`` defaults to
        ``NONE`` if ``crs`` is ``None``, otherwise ``UNKNOWN``.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().with_crs("EPSG:1234")
        LinestringType(geoarrow.linestring <EPSG:1234>)
        """
        if crs_type is None and crs is None:
            ctype = self._type.with_crs(b"", lib.CrsType.NONE)
        elif crs_type is None:
            if not isinstance(crs, bytes):
                crs = crs.encode("UTF-8")
            ctype = self._type.with_crs(crs, lib.CrsType.UNKNOWN)
        else:
            if not isinstance(crs, bytes):
                crs = crs.encode("UTF-8")
            ctype = self._type.with_crs(crs, crs_type)

        return _ctype_to_extension_type(ctype)


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


class PointType(GeometryExtensionType):
    """Extension type whose storage is an array of points stored
    as either a struct with one child per dimension or a fixed-size
    list whose single child is composed of interleaved ordinate values.
    """

    _extension_name = "geoarrow.point"

    def from_geobuffers(self, validity, x, y=None, z_or_m=None, m=None):
        buffers = [
            (0, "uint8", validity),
            (1, "double", x),
            (2, "double", y),
            (3, "double", z_or_m),
            (4, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


class LinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of linestrings stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.linestring"

    def from_geobuffers(self, validity, coord_offsets, x, y=None, z_or_m=None, m=None):
        buffers = [
            (0, "uint8", validity),
            (1, "int32", coord_offsets),
            (2, "double", x),
            (3, "double", y),
            (4, "double", z_or_m),
            (5, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


class PolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.polygon"

    def from_geobuffers(
        self, validity, ring_offsets, coord_offsets, x, y=None, z_or_m=None, m=None
    ):
        buffers = [
            (0, "uint8", validity),
            (1, "int32", ring_offsets),
            (2, "int32", coord_offsets),
            (3, "double", x),
            (4, "double", y),
            (5, "double", z_or_m),
            (6, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


class MultiPointType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipoint"

    def from_geobuffers(self, validity, coord_offsets, x, y=None, z_or_m=None, m=None):
        buffers = [
            (0, "uint8", validity),
            (1, "int32", coord_offsets),
            (2, "double", x),
            (3, "double", y),
            (4, "double", z_or_m),
            (5, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


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
        buffers = [
            (0, "uint8", validity),
            (1, "int32", linestring_offsets),
            (2, "int32", coord_offsets),
            (3, "double", x),
            (4, "double", y),
            (5, "double", z_or_m),
            (6, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


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
        buffers = [
            (0, "uint8", validity),
            (1, "int32", polygon_offsets),
            (2, "int32", ring_offsets),
            (3, "int32", coord_offsets),
            (4, "double", x),
            (5, "double", y),
            (6, "double", z_or_m),
            (7, "double", m),
        ]

        return self._from_geobuffers_internal(buffers)


def type_cls_from_name(name):
    if name == "geoarrow.wkb":
        return WkbType
    elif name == "geoarrow.wkt":
        return WktType
    elif name == "geoarrow.point":
        return PointType
    elif name == "geoarrow.linestring":
        return LinestringType
    elif name == "geoarrow.polygon":
        return PolygonType
    elif name == "geoarrow.multipoint":
        return MultiPointType
    elif name == "geoarrow.multilinestring":
        return MultiLinestringType
    elif name == "geoarrow.multipolygon":
        return MultiPolygonType
    else:
        raise ValueError(f'Expected valid extension name but got "{name}"')


def _ctype_to_extension_type(ctype):
    cls = type_cls_from_name(ctype.extension_name)
    return cls(ctype)


def _make_default(geometry_type, cls):
    ctype = lib.CVectorType.Make(
        geometry_type, lib.Dimensions.XY, lib.CoordType.SEPARATE
    )
    return cls(ctype)


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
    return _make_default(lib.GeometryType.POINT, PointType)


def linestring() -> PointType:
    """Geoarrow-encoded line features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.linestring()
    LinestringType(geoarrow.linestring)
    >>> ga.linestring().storage_type
    ListType(list<vertices: struct<x: double, y: double>>)
    """
    return _make_default(lib.GeometryType.LINESTRING, LinestringType)


def polygon() -> PolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.polygon()
    PolygonType(geoarrow.polygon)
    >>> ga.polygon().storage_type
    ListType(list<rings: list<vertices: struct<x: double, y: double>>>)
    """
    return _make_default(lib.GeometryType.POLYGON, PolygonType)


def multipoint() -> MultiPointType:
    """Geoarrow-encoded multipoint features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipoint()
    MultiPointType(geoarrow.multipoint)
    >>> ga.multipoint().storage_type
    ListType(list<points: struct<x: double, y: double>>)
    """
    return _make_default(lib.GeometryType.MULTIPOINT, MultiPointType)


def multilinestring() -> MultiLinestringType:
    """Geoarrow-encoded multilinestring features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multilinestring()
    MultiLinestringType(geoarrow.multilinestring)
    >>> ga.multilinestring().storage_type
    ListType(list<linestrings: list<vertices: struct<x: double, y: double>>>)
    """
    return _make_default(lib.GeometryType.MULTILINESTRING, MultiLinestringType)


def multipolygon() -> MultiPolygonType:
    """Geoarrow-encoded polygon features.

    >>> import geoarrow.pyarrow as ga
    >>> ga.multipolygon()
    MultiPolygonType(geoarrow.multipolygon)
    >>> ga.multipolygon().storage_type
    ListType(list<polygons: list<rings: list<vertices: struct<x: double, y: double>>>>)
    """
    return _make_default(lib.GeometryType.MULTIPOLYGON, MultiPolygonType)


def extension_type(
    geometry_type,
    dimensions=lib.Dimensions.XY,
    coord_type=lib.CoordType.SEPARATE,
    edge_type=lib.EdgeType.PLANAR,
    crs=None,
    crs_type=None,
) -> GeometryExtensionType:
    """Generic vector geometry type constructor.

    >>> import geoarrow.pyarrow as ga
    >>> ga.extension_type(ga.GeometryType.POINT, crs="EPSG:1234")
    PointType(geoarrow.point <EPSG:1234>)
    """
    ctype = lib.CVectorType.Make(geometry_type, dimensions, coord_type)
    cls = type_cls_from_name(ctype.extension_name)
    return cls(ctype).with_edge_type(edge_type).with_crs(crs, crs_type)


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


_extension_types_registered = False


def register_extension_types(lazy=True):
    """Register the extension types in the geoarrow namespace with the pyarrow
    registry. This enables geoarrow types to be read, written, imported, and
    exported like any other Arrow type.
    """
    global _extension_types_registered

    if lazy and _extension_types_registered is True:
        return

    _extension_types_registered = None

    all_types = [
        wkt(),
        wkb(),
        point(),
        linestring(),
        polygon(),
        multipoint(),
        multilinestring(),
        multipolygon(),
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
        "geoarrow.point",
        "geoarrow.linestring",
        "geoarrow.polygon",
        "geoarrow.multipoint",
        "geoarrow.multilinestring",
        "geoarrow.multipolygon",
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
