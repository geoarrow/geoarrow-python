from enum import Enum


class Encoding(Enum):
    """Constants for encoding type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.Encoding.GEOARROW
    <Encoding.GEOARROW: 5>
    """

    #: Unknown or uninitialized encoding
    UNKNOWN = 0
    #: Well-known binary encoding with a maximum of 2GB of data per array chunk
    WKB = 1
    #: Well-known binary encoding
    LARGE_WKB = 2
    #: Well-known text encoding with a maximum of 2GB of data per array chunk
    WKT = 3
    #: Well-known text encoding
    LARGE_WKT = 4
    #: GeoArrow native nested list encoding
    GEOARROW = 5


class GeometryType(Enum):
    """Constants for geometry type. These values are the same as those used
    in well-known binary (i.e, 0-7).

    Examples
    --------

    >>> from geoarrow import types
    >>> types.GeometryType.MULTIPOINT
    <GeometryType.MULTIPOINT: 4>
    """

    #: Unknown or uninitialized geometry type
    GEOMETRY = 0
    #: Point geometry type
    POINT = 1
    #: Linestring geometry type
    LINESTRING = 2
    #: Polygon geometry type
    POLYGON = 3
    #: Multipoint geometry type
    MULTIPOINT = 4
    #: Multilinestring geometry type
    MULTILINESTRING = 5
    #: Multipolygon geometry type
    MULTIPOLYGON = 6
    #: Geometrycollection geometry type
    GEOMETRYCOLLECTION = 7


class Dimensions(Enum):
    """Constants for dimensions.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.Dimensions.XYZM
    <Dimensions.XYZM: 4>
    """

    #: Unknown or ininitialized dimensions
    UNKNOWN = 0
    #: XY dimensions
    XY = 1
    #: XYZ dimensions
    XYZ = 2
    #: XYM dimensions
    XYM = 3
    #: XYZM dimensions
    XYZM = 4


class CoordType(Enum):
    """Constants for coordinate type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.CoordType.INTERLEAVED
    <CoordType.INTERLEAVED: 2>
    """

    #: Unknown or uninitialized coordinate type
    UNKNOWN = 0
    #: Coordinate type composed of separate arrays for each dimension (i.e., a struct)
    SEPARATE = 1
    #: Coordinate type compose of a single array containing all dimensions
    #:(i.e., a fixed-size list)
    INTERLEAVED = 2


class EdgeType(Enum):
    """Constants for edge type.

    Examples
    --------

    >>> from geoarrow import types
    >>> types.EdgeType.SPHERICAL
    <EdgeType.SPHERICAL: 2>
    """

    #: Unknown or uninitialized edge type
    UNKNOWN = 0
    #: Edges form a Cartesian line on a plane
    PLANAR = 1
    #: Edges are geodesic on a sphere
    SPHERICAL = 2
