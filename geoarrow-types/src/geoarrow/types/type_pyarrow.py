from typing import Optional

from geoarrow.types.type_base import GeoArrowType
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


def from_base_type(base_type):
    cls = _EXTENSION_CLASSES[base_type.extension_name]
    return cls(base_type)


class GeometryExtensionType(pa.ExtensionType):
    """Extension type base class for vector geometry types."""

    _extension_name = None

    def __init__(self, base_type: GeoArrowType):
        if not isinstance(base_type, GeoArrowType):
            raise TypeError(
                "GeometryExtension Type must be created from a GeoArrowType"
            )
        self._type = base_type

        if self._type.extension_name != type(self)._extension_name:
            raise ValueError(
                f'Expected BaseType with extension name "{type(self)._extension_name}" but got "{self._type.extension_name}"'
            )

        if base_type.encoding == Encoding.GEOARROW:
            key = base_type.geometry_type, base_type.coord_type, base_type.dimensions
            storage_type = _NATIVE_STORAGE_TYPES[key]
        else:
            storage_type = _SERIALIZED_STORAGE_TYPES[base_type.encoding]

        pa.ExtensionType.__init__(self, storage_type, self._type.extension_name)

    def __repr__(self):
        return f"{type(self).__name__}({repr(self._type)})"

    def __arrow_ext_serialize__(self):
        return self._type.extension_metadata.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()

    def __arrow_ext_class__(self):
        return GeometryExtensionType._array_cls_from_name(self.extension_name)

    def __arrow_ext_scalar_class__(self):
        return GeometryExtensionType._scalar_cls_from_name(self.extension_name)

    def to_pandas_dtype(self):
        from pandas import ArrowDtype

        return ArrowDtype(self)

    @property
    def geometry_type(self) -> GeometryType:
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
    def dimensions(self) -> Dimensions:
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
    def coord_type(self) -> CoordType:
        """The :class:`geoarrow.CoordType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().coord_type == ga.CoordType.SEPARATE
        True
        >>> ga.linestring().with_coord_type(ga.CoordType.INTERLEAVED).coord_type
        <GeoArrowCoordType.GEOARROW_COORD_TYPE_INTERLEAVED: 2>
        """
        return self._type.coord_type

    @property
    def edge_type(self) -> EdgeType:
        """The :class:`geoarrow.EdgeType` of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.linestring().edge_type == ga.EdgeType.PLANAR
        True
        >>> ga.linestring().with_edge_type(ga.EdgeType.SPHERICAL).edge_type
        <GeoArrowEdgeType.GEOARROW_EDGE_TYPE_SPHERICAL: 1>
        """
        return self._type.edge_type

    @property
    def crs(self) -> Optional[Crs]:
        """The coordinate reference system of this type.

        >>> import geoarrow.pyarrow as ga
        >>> ga.point().with_crs("EPSG:1234").crs
        'EPSG:1234'
        """
        return self._type.crs


class WkbType(GeometryExtensionType):
    """Extension type whose storage is a binary or large binary array of
    well-known binary. Even though the draft specification currently mandates
    ISO well-known binary, EWKB is supported by the parser.
    """

    _extension_name = "geoarrow.wkb"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


class WktType(GeometryExtensionType):
    """Extension type whose storage is a utf8 or large utf8 array of
    well-known text.
    """

    _extension_name = "geoarrow.wkt"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


class PointType(GeometryExtensionType):
    """Extension type whose storage is an array of points stored
    as either a struct with one child per dimension or a fixed-size
    list whose single child is composed of interleaved ordinate values.
    """

    _extension_name = "geoarrow.point"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


class LinestringType(GeometryExtensionType):
    """Extension type whose storage is an array of linestrings stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.linestring"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


class PolygonType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.polygon"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


class MultiPointType(GeometryExtensionType):
    """Extension type whose storage is an array of polygons stored
    as a list of points as described in :class:`PointType`.
    """

    _extension_name = "geoarrow.multipoint"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        raise NotImplementedError()


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


def _struct_fields(dims):
    return pa.struct([pa.field(c, pa.float64()) for c in dims])


def _interleaved_fields(dims):
    return pa.list_(pa.field(dims, pa.float64()), len(dims))


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


def _append_fingerprint(obj, fingerprint):
    if isinstance(obj, pa.Field):
        fingerprint.append(obj.name)
        _append_fingerprint(obj.type, fingerprint)
        return

    fingerprint.append(obj.id)
    if pa_types.is_list(obj):
        _append_fingerprint(obj.value_type, fingerprint)
    elif pa_types.is_struct(obj):
        for i in range(obj.num_fields):
            _append_fingerprint(obj.field(i), fingerprint)

    return fingerprint


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
                key = geometry_type, dimensions, coord_type
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
