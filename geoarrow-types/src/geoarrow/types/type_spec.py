from typing import NamedTuple, Optional


from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    EdgeType,
    CoordType,
)
from geoarrow.types.crs import Crs
from geoarrow.types import crs


class TypeSpec(NamedTuple):
    encoding: Encoding = Encoding.UNSPECIFIED
    geometry_type: GeometryType = GeometryType.UNSPECIFIED
    dimensions: Dimensions = Dimensions.UNSPECIFIED
    coord_type: CoordType = CoordType.UNSPECIFIED
    edge_type: EdgeType = EdgeType.UNSPECIFIED
    crs: Optional[Crs] = crs.UNSPECIFIED

    def any_unspecified(self) -> bool:
        for cls, arg in zip(_SPEC_TYPES):
            if arg == cls.UNSPECIFIED:
                return True

        return False


def type_spec(
    *,
    encoding=None,
    geometry_type=None,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
):
    args = (encoding, geometry_type, dimensions, coord_type, edge_type, crs)
    sanitized_args = (cls.create(arg) for cls, arg in zip(_SPEC_TYPES, args))
    return TypeSpec(*sanitized_args)


_SPEC_TYPES = [Encoding, GeometryType, Dimensions, CoordType, EdgeType, crs]
