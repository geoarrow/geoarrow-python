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

    @classmethod
    def create(cls, obj):
        if isinstance(obj, TypeSpec):
            return obj

        for name, field_cls in zip(cls._fields, _SPEC_TYPES[:-1]):
            if isinstance(obj, field_cls):
                return TypeSpec(**{name: obj})

        if hasattr(obj, "to_json_dict"):
            return TypeSpec(crs=obj)

        raise TypeError(
            f"Can't create TypeSpec from object of type {type(obj).__name__}"
        )

    @classmethod
    def coalesce(cls, *args):
        return cls._reduce_common("_coalesce2", args)

    @classmethod
    def coalesce_unspecified(cls, *args):
        return cls._reduce_common("_coalesce_unspecified2", args)

    @classmethod
    def common(cls, *args):
        return cls._reduce_common("_common2", args)

    @classmethod
    def _reduce_common(cls, reducer2_name, args):
        reducers = [getattr(cls, reducer2_name) for cls in _SPEC_TYPES]

        args = iter(args)
        out = cls.create(next(args, TypeSpec()))
        for item in args:
            item = cls.create(item)
            out = TypeSpec(
                *(reducer(a, b) for reducer, a, b in zip(reducers, out, item))
            )

        return out


def type_spec(
    *args,
    encoding=None,
    geometry_type=None,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
):
    out_args = TypeSpec.coalesce_unspecified(*args)

    kwargs = (encoding, geometry_type, dimensions, coord_type, edge_type, crs)
    sanitized_kwargs = (cls.create(arg) for cls, arg in zip(_SPEC_TYPES, kwargs))
    return TypeSpec.coalesce_unspecified(out_args, TypeSpec(*sanitized_kwargs))


_SPEC_TYPES = (Encoding, GeometryType, Dimensions, CoordType, EdgeType, crs)
