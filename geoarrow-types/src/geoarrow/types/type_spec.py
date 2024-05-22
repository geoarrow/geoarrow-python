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

    def is_partial(self) -> bool:
        if not self.encoding.is_specified():
            return True

        elif not self.encoding.is_serialized():
            return (
                not self.geometry_type.is_specified()
                or not self.dimensions.is_specified()
                or not self.coord_type.is_specified()
            )

        return not self.edge_type.is_specified() or self.crs is crs.UNSPECIFIED

    def __repr__(self) -> str:
        specified_fields = []
        for cls, item in zip(_SPEC_TYPES, self):
            if item is not cls.UNSPECIFIED:
                specified_fields.append(str(item))
        specified_fields_str = ", ".join(specified_fields)
        return f"TypeSpec({specified_fields_str})"


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


_SPEC_TYPES = (Encoding, GeometryType, Dimensions, CoordType, EdgeType, crs)
