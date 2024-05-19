from typing import NamedTuple, Optional


from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.crs import Crs, UNSPECIFIED, create as crs_create


class TypeSpec(NamedTuple):
    encoding: Encoding = Encoding.UNSPECIFIED
    geometry_type: GeometryType = GeometryType.UNSPECIFIED
    dimensions: Dimensions = Dimensions.UNSPECIFIED
    coord_type: CoordType = CoordType.UNSPECIFIED
    edge_type: EdgeType = EdgeType.UNSPECIFIED
    crs: Optional[Crs] = UNSPECIFIED


def type_spec(
    *,
    encoding=None,
    geometry_type=None,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=UNSPECIFIED,
):
    return TypeSpec(
        Encoding.create(encoding),
        GeometryType.create(geometry_type),
        Dimensions.create(dimensions),
        CoordType.create(coord_type),
        EdgeType.create(edge_type),
        crs_create(crs),
    )
