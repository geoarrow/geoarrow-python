from typing import Optional

from geoarrow.types.type_spec import TypeSpec
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
            if spec.encoding == Encoding.GEOARROW:
                key = spec.geometry_type, spec.coord_type, spec.dimensions
                storage_type = _NATIVE_STORAGE_TYPES[key]
            else:
                storage_type = _SERIALIZED_STORAGE_TYPES[spec.encoding]
        elif validate_storage_type:
            _validate_storage_type(storage_type, spec)

        pa.ExtensionType.__init__(self, storage_type, self._spec.extension_name())

    def __repr__(self):
        return f"{type(self).__name__}({repr(self._spec)})"

    def __arrow_ext_serialize__(self):
        return self._spec.extension_metadata().encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return _deserialize_storage(
            storage_type, cls._extension_name, serialized.decode()
        )

    def to_pandas_dtype(self):
        from pandas import ArrowDtype

        return ArrowDtype(self)

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
        >>> ga.linestring().coord_type == ga.CoordType.SEPARATE
        True
        >>> ga.linestring().with_coord_type(ga.CoordType.INTERLEAVED).coord_type
        <GeoArrowCoordType.GEOARROW_COORD_TYPE_INTERLEAVED: 2>
        """
        return self._spec.coord_type

    @property
    def edge_type(self) -> EdgeType:
        """The :class:`EdgeType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().edge_type == ga.EdgeType.PLANAR
        True
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL).edge_type
        <GeoArrowEdgeType.GEOARROW_EDGE_TYPE_SPHERICAL: 1>
        """
        return self._spec.edge_type

    @property
    def crs(self) -> Optional[Crs]:
        """The coordinate reference system of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_crs("EPSG:1234").crs
        'EPSG:1234'
        """
        return self._spec.crs


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


class LinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of linestrings stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.linestring"


class PolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.polygon"


class MultiPointType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipoint"


class MultiLinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of multilinestrings stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multilinestring"


class MultiPolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of multilinestrings stored
    as a list of a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipolygon"


def extension_type(
    spec: TypeSpec, storage_type=None, validate_storage_type=True
) -> GeometryExtensionType:
    spec = spec.with_defaults()
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
    elif pa_types.is_float64(storage_type):
        return [("double", ())]
    elif isinstance(storage_type, pa.ListType):
        f = storage_type.field(0)
        return [("list", (f.name,))] + _parse_storage(f.type)
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
    but if somebody does this it should error until is implemented.
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

    # If this is a serialized type, we don't need to infer any more information
    # from the storage type.
    if spec.encoding.is_serialized():
        if extension_name is not None and spec.extension_name() != extension_name:
            raise ValueError(f"Can't interpret {storage_type} as {extension_name}")

        return extension_type(spec, storage_type, validate_storage_type=False)

    # Infer geometry type from extension name
    if extension_name is not None:
        spec = TypeSpec.from_extension_name(extension_name).with_defaults(spec)

    # Infer dimensions and verify coordinate types
    type_name, params = parsed[-1]
    if type_name == "struct":
        names, parsed_children = params
        n_dims = len(names)
    else:
        names, n_dims, parsed_children = params

    if names in _DIMS_FROM_NAMES:
        dims = _DIMS_FROM_NAMES[names]
    elif n_dims == 2:
        dims = Dimensions.XY
    elif n_dims == 4:
        dims = Dimensions.XYZM
    else:
        raise ValueError(f"Can't infer dimensions from coord field names {names}")

    for parsed_child in parsed_children:
        if parsed_child[0] != "double":
            raise ValueError(
                f"Expected coordinate double coordinate values but got {parsed_child[0]}"
            )

    spec = spec.with_defaults(dims)

    # Make sure we haven't generated a spec whose extension name doesn't match
    if extension_name is not None and spec.extension_name() != extension_name:
        raise ValueError(f"Can't interpret {storage_type} as {extension_name}")

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

    all_geoemetry_types = list(field_names.keys())
    all_coord_types = [CoordType.INTERLEAVED, CoordType.SEPARATED]
    all_dimensions = [Dimensions.XY, Dimensions.XYZ, Dimensions.XYM, Dimensions.XYZM]

    all_storage_types = {}
    for geometry_type in all_geoemetry_types:
        for coord_type in all_coord_types:
            for dimensions in all_dimensions:
                names = field_names[geometry_type]
                coord = coord_storage[(coord_type, dimensions)]
                key = geometry_type, coord_type, dimensions
                storage_type = _nested_type(coord, names)
                all_storage_types[key] = storage_type

    return all_storage_types


_EXTENSION_CLASSES = {
    "geoarrow.wkb": WkbType,
    "geoarrow.wkt": WktType,
    "geoarrow.point": PointType,
    "geoarrow.linestring": LinestringType,
    "geoarrow.polygon": PolygonType,
    "geoarrow.multipoint": MultiPointType,
    "geoarrow.multilinestring": MultiLinestringType,
    "geoarrow.multipolygon": MultiPolygonType,
}

_SERIALIZED_STORAGE_TYPES = {
    Encoding.WKT: pa.utf8(),
    Encoding.LARGE_WKT: pa.large_utf8(),
    Encoding.WKB: pa.binary(),
    Encoding.LARGE_WKB: pa.large_binary(),
}

_NATIVE_STORAGE_TYPES = _generate_storage_types()

_SPEC_FROM_TYPE_NESTING = {
    ("binary",): Encoding.WKB,
    ("large_binary",): Encoding.LARGE_WKB,
    ("string",): Encoding.WKT,
    ("large_string",): Encoding.LARGE_WKT,
    ("struct",): TypeSpec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.POINT,
        coord_type=CoordType.SEPARATED,
    ),
    ("list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.SEPARATED
    ),
    ("list", "list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.SEPARATED
    ),
    ("list", "list", "list", "struct"): TypeSpec(
        encoding=Encoding.GEOARROW, coord_type=CoordType.SEPARATED
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
