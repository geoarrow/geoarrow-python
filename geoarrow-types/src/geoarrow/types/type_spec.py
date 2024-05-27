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
    to accomodate various type constraints. These objects are usually
    created using helper methods like :func:`type_spec`.

    Parameters
    ----------
    encoding : Encoding, optional
        Defaults to ``Encoding.UNSPECIFIED``.
    geometry_type : GeometryType, optional
        Defaults to ``GeometryType.UNSPECIFIED``.
    dimensions : Dimensions, optional
        Defaults to ``Dimensions.UNSPECIFIED``.
    coord_type : CoordType, optional
        Defaults to ``CoordType.UNSPECIFIED``.
    edge_type : EdgeType, optional
        Defaults to ``EdgeType.UNSPECIFIED``.
    crs : Crs or None, optional
        Defaults to ``geoarrow.types.crs.UNSPECIFIED``.
    """

    encoding: Encoding = Encoding.UNSPECIFIED
    geometry_type: GeometryType = GeometryType.UNSPECIFIED
    dimensions: Dimensions = Dimensions.UNSPECIFIED
    coord_type: CoordType = CoordType.UNSPECIFIED
    edge_type: EdgeType = EdgeType.UNSPECIFIED
    crs: Optional[Crs] = crs.UNSPECIFIED

    def with_defaults(self, defaults=None):
        """Apply defaults to unspecified fields

        Applies values from ``defaults`` where fields in ``self`` are
        unspecified.

        Parameters
        ----------
        defaults: TypeSpec, optional
            A TypeSpec from which values from self are replaced if the
            value in self has not yet been specified. By default this
            will fill in ``Dimensions.XY``, ``CoordType.SEPARATED``,
            ``EdgeType.PLANAR``, and ``crs=None``.
        """
        if defaults is None:
            defaults = _SPEC_SPECIFIED_DEFAULTS

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
        """Override values for specific fields

        This method allows the setting or unsetting of specific field
        values. Any value specified in an argument to ``override()`` will
        replace the value in ``self`` (even if this value is an unspecified
        sentinel).

        Parameters
        ----------
        encoding : Encoding, optional
            Defaults to ``None``.
        geometry_type : GeometryType, optional
            Defaults to ``None``.
        dimensions : Dimensions, optional
            Defaults to ``None``.
        coord_type : CoordType, optional
            Defaults to ``None``.
        edge_type : EdgeType, optional
            Defaults to ``None``.
        crs : Crs or None, optional
            Defaults a sentinel that ensures any valid Crs value
            (including ``crs.UNSPECIFIED`` and ``None`` will be replaced).
        """
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
        """Coerce an input value to a TypeSpec instance

        Converts ``obj`` to its representation as a ``TypeSpec`` if possible.
        This is used to coerce enums (e.g., ``GeometryType.POINT``) to a
        type specification with the appropriate field set (e.g.,
        ``TypeSpec(geometry_type=GeometryType.POINT)``).

        Parameters
        ----------
        obj : any
            An object with an umambiguous representation as a ``TypeSpec``.
        """
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
        """Coalesce specified values

        Given zero or more values that have an unambiguous representation as
        a ``TypeSpec``, collect the first specified value in each field.
        This is useful for applying default values where a user can specify
        preferences but where a set of reasonable defaults is appropriate.

        Parameters
        ----------
        args : list
            Zero or more objects with an unambiguous representation as a
            ``TypeSpec``.
        """
        return cls._reduce_common("_coalesce2", args)

    @classmethod
    def coalesce_unspecified(cls, *args):
        """Coalesce specified values ensuring each field is specified once

        Given zero or more values that have an unambiguous representation as
        a ``TypeSpec``, collect at most one specified value in each field.
        This is useful for combining two specifications where it is important
        that fields are not overspecified (e.g., a geometry type of point
        and linestring).

        Parameters
        ----------
        args : list
            Zero or more objects with an unambiguous representation as a
            ``TypeSpec``.
        """
        return cls._reduce_common("_coalesce_unspecified2", args)

    @classmethod
    def common(cls, *args):
        """Compute a common cast target

        Given zero or more values that have an unambiguous representation as
        a ``TypeSpec``, compute a specification to which all values have an
        unambiguous representation. This computation attempts to preserve
        ``Encoding.GEOARROW`` where possible but may fall back to
        ``Encoding.WKB`` if this is not possible (e.g., mixed geometry
        types).

        Parameters
        ----------
        args : list
            Zero or more objects with an unambiguous representation as a
            ``TypeSpec``.
        """
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
    """Create a type specification

    This helper creates a GeoArrow :class:`TypeSpec`, applying creation
    helpers to make it less verbose to create a type.

    Parameters
    ----------
    args: list
        Zero or more objects with an unambiguous representation as a
        :class:`TypeSpec`.
    encoding : any, optional
        An explicit :class:`Encoding` or string representation of one.
    geometry_type : any, optional
        An explicit :class:`GeometryType` or string representation of one.
    dimensions : any, optional
        An explicit :class:`Dimensions` or string representation of one.
    coord_type : any, optional
        An explicit :class:`CoordType` or string representation of one.
    edge_type : any, optional
        An explicit :class:`EdgeType` or string representation of one.
    crs : Crs or None, optional
        An explicit :class:`Crs`. Defaults to ``geoarrow.types.crs.UNSPECIFIED``.
    """
    out_args = TypeSpec.coalesce_unspecified(*args)

    kwargs = (encoding, geometry_type, dimensions, coord_type, edge_type, crs)
    sanitized_kwargs = (cls.create(arg) for cls, arg in zip(_SPEC_TYPES, kwargs))
    return TypeSpec.coalesce_unspecified(out_args, TypeSpec(*sanitized_kwargs))


_SPEC_TYPES = (Encoding, GeometryType, Dimensions, CoordType, EdgeType, crs)

_SPEC_SPECIFIED_DEFAULTS = TypeSpec(
    dimensions=Dimensions.XY,
    coord_type=CoordType.SEPARATED,
    edge_type=EdgeType.PLANAR,
    crs=None,
)
