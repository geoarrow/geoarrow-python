from enum import Enum


class Encoding(Enum):
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


class GeometryType(Enum):
    """Constants for geometry type. These values are the same as those used
    in well-known binary (i.e, 0-7).

    Examples
    --------

    >>> from geoarrow import types
    >>> types.GeometryType.MULTIPOINT
    <GeometryType.MULTIPOINT: 4>
    """

    GEOMETRY = 0
    """Unknown or uninitialized geometry type"""

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


class Dimensions(Enum):
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


class CoordType(Enum):
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


class EdgeType(Enum):
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
