from typing import NamedTuple


from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.crs import Crs, UNSPECIFIED


class LayoutSpec(NamedTuple):
    encoding: Encoding = Encoding.UNKNOWN
    geometry_type: GeometryType = GeometryType.GEOMETRY
    dimensions: Dimensions = Dimensions.UNKNOWN
    coord_type: CoordType = CoordType.UNKNOWN


class TypeSpec(LayoutSpec):
    edge_type: EdgeType = EdgeType.UNKNOWN
    crs: Crs = UNSPECIFIED
