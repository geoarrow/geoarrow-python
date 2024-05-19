from typing import NamedTuple, Optional


from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.crs import Crs, CRS_UNSPECIFIED


class LayoutSpec(NamedTuple):
    encoding: Encoding = Encoding.UNSPECIFIED
    geometry_type: GeometryType = GeometryType.GEOMETRY
    dimensions: Dimensions = Dimensions.UNSPECIFIED
    coord_type: CoordType = CoordType.UNSPECIFIED


class TypeSpec(LayoutSpec):
    edge_type: EdgeType = EdgeType.UNSPECIFIED
    crs: Optional[Crs] = CRS_UNSPECIFIED
