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

_UNSPECIFIED_CRS_ARG = crs.UnspecifiedCrs()


class TypeSpec(NamedTuple):
    """GeoArrow Type Specification

    This class is a parameterization of all available types in the
    GeoArrow specification, including both serialized and native encodings
    with supported metadata. A ``TypeSpec`` instance can leave some
    components unspecified such that multiple instances can be merged
    to accomodate various type constraints.

    Parameters
    ----------
    encoding : Encoding, optional
    geometry_type
    dimensions
    coord_type
    edge_type
    crs

    """

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

    def with_defaults(self, defaults):
        return TypeSpec.coalesce(self, defaults)

    def override(
        self,
        *,
        encoding=None,
        geometry_type=None,
        dimensions=None,
        coord_type=None,
        edge_type=None,
        crs=_UNSPECIFIED_CRS_ARG,
    ):
        clean = type_spec(
            encoding=encoding,
            geometry_type=geometry_type,
            dimensions=dimensions,
            coord_type=coord_type,
            edge_type=edge_type,
            crs=crs,
        )

        encoding = self.encoding if encoding is None else clean.encoding
        geometry_type = (
            self.geometry_type if geometry_type is None else clean.geometry_type
        )
        dimensions = self.dimensions if dimensions is None else clean.dimensions
        coord_type = self.coord_type if coord_type is None else clean.coord_type
        edge_type = self.edge_type if edge_type is None else clean.edge_type
        crs = self.crs if crs is _UNSPECIFIED_CRS_ARG else clean.crs

        return TypeSpec(encoding, geometry_type, dimensions, coord_type, edge_type, crs)

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
