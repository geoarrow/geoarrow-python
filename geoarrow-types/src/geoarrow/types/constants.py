from enum import Enum
from functools import reduce


class TypeSpecEnum(Enum):
    @classmethod
    def create(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif obj is None:
            return cls.UNSPECIFIED
        elif isinstance(obj, str):
            return cls[obj.upper()]
        else:
            raise TypeError(
                f"Can't create {cls.__name__} from object of type {type(obj).__name__}"
            )

    @classmethod
    def default(cls, *args):
        return reduce(cls._defaults2, args, cls.UNSPECIFIED)

    @classmethod
    def specified(cls, *args):
        return reduce(cls._specified2, args, cls.UNSPECIFIED)

    @classmethod
    def common(cls, *args):
        return reduce(cls._common2, args, cls.UNSPECIFIED)

    @classmethod
    def _defaults2(cls, value, default):
        if value == cls.UNSPECIFIED:
            return default
        else:
            return value

    @classmethod
    def _specified2(cls, lhs, rhs):
        if lhs == rhs:
            return lhs
        elif lhs == cls.UNSPECIFIED:
            return rhs
        elif rhs == cls.UNSPECIFIED:
            return lhs
        else:
            raise ValueError(f"{cls.__name__} {lhs} and {rhs} are both specified")

    @classmethod
    def _common2(cls, lhs, rhs):
        if lhs == cls.UNSPECIFIED:
            return rhs
        elif rhs == cls.UNSPECIFIED:
            return lhs
        elif lhs == rhs:
            return lhs
        elif (lhs, rhs) in _VALUE_COMMON_HELPER:
            return _VALUE_COMMON_HELPER[(lhs, rhs)]
        elif (rhs, lhs) in _VALUE_COMMON_HELPER:
            return _VALUE_COMMON_HELPER[(rhs, lhs)]
        else:
            return None


class Encoding(TypeSpecEnum):
    """Constants for encoding type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.Encoding.GEOARROW
    <Encoding.GEOARROW: 5>
    """

    UNSPECIFIED = 0
    """Unknown or uninitialized encoding"""

    WKB = 1
    """Well-known binary encoding with a maximum of 2GB of data per array chunk"""

    LARGE_WKB = 2
    """Well-known binary encoding"""

    WKT = 3
    """Well-known text encoding with a maximum of 2GB of data per array chunk"""

    LARGE_WKT = 4
    """Well-known text encoding with 64-bit offsets"""

    GEOARROW = 5
    """GeoArrow native nested list encoding"""


class GeometryType(TypeSpecEnum):
    """Constants for geometry type. These values are the same as those used
    in well-known binary (i.e, 0-7).

    Examples
    --------

    >>> from geoarrow import types
    >>> types.GeometryType.MULTIPOINT
    <GeometryType.MULTIPOINT: 4>
    """

    UNSPECIFIED = -1
    """Unknown or unspecified geometry type"""

    GEOMETRY = 0
    """Mixed geometry type"""

    POINT = 1
    """Point geometry type"""

    LINESTRING = 2
    """Linestring geometry type"""

    POLYGON = 3
    """Polygon geometry type"""

    MULTIPOINT = 4
    """Multipoint geometry type"""

    MULTILINESTRING = 5
    """Multilinestring geometry type"""

    MULTIPOLYGON = 6
    """Multipolygon geometry type"""

    GEOMETRYCOLLECTION = 7
    """Geometry collection geometry type"""

    @classmethod
    def _common2(cls, lhs, rhs):
        out = super()._common2(lhs, rhs)
        if out is not None:
            return out
        else:
            return cls.GEOMETRY


class Dimensions(TypeSpecEnum):
    """Constants for dimensions.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.Dimensions.XYZM
    <Dimensions.XYZM: 4>
    """

    UNSPECIFIED = 0
    """Unknown or uninitialized dimensions"""

    XY = 1
    """XY dimensions"""

    XYZ = 2
    """XYZ dimensions"""

    XYM = 3
    """XYM dimensions"""

    XYZM = 4
    """XYZM dimensions"""


class CoordType(TypeSpecEnum):
    """Constants for coordinate type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.CoordType.INTERLEAVED
    <CoordType.INTERLEAVED: 2>
    """

    UNSPECIFIED = 0
    """"Unknown or uninitialized coordinate type"""

    SEPARATED = 1
    """Coordinate type composed of separate arrays for each dimension
    (i.e., a struct)
    """

    INTERLEAVED = 2
    """Coordinate type composed of a single array containing all dimensions
    (i.e., a fixed-size list)
    """


class EdgeType(TypeSpecEnum):
    """Constants for edge type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.EdgeType.SPHERICAL
    <EdgeType.SPHERICAL: 2>
    """

    UNSPECIFIED = 0
    """Unknown or ininitialized edge type"""

    PLANAR = 1
    """Edges form a Cartesian line on a plane"""

    SPHERICAL = 2
    """Edges are geodesic on a sphere"""


_VALUE_COMMON_HELPER = {
    (Encoding.WKB, Encoding.LARGE_WKB): Encoding.LARGE_WKB,
    (Encoding.WKB, Encoding.WKT): Encoding.WKB,
    (Encoding.WKB, Encoding.LARGE_WKT): Encoding.LARGE_WKB,
    (Encoding.WKB, Encoding.GEOARROW): Encoding.WKB,
    (Encoding.WKT, Encoding.LARGE_WKT): Encoding.LARGE_WKT,
    (Encoding.WKT, Encoding.LARGE_WKB): Encoding.LARGE_WKB,
    (Encoding.WKT, Encoding.GEOARROW): Encoding.WKB,
    (GeometryType.POINT, GeometryType.MULTIPOINT): GeometryType.MULTIPOINT,
    (
        GeometryType.LINESTRING,
        GeometryType.MULTILINESTRING,
    ): GeometryType.MULTILINESTRING,
    (GeometryType.POLYGON, GeometryType.MULTIPOLYGON): GeometryType.MULTIPOLYGON,
    (Dimensions.XY, Dimensions.XYZ): Dimensions.XYZ,
    (Dimensions.XY, Dimensions.XYM): Dimensions.XYM,
    (Dimensions.XY, Dimensions.XYZM): Dimensions.XYZM,
    (Dimensions.XYZ, Dimensions.XYM): Dimensions.XYZM,
    (Dimensions.XYM, Dimensions.XYZM): Dimensions.XYZM,
}
