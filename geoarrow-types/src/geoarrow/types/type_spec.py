import json
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

    def extension_name(self) -> str:
        """Compute the GeoArrow extension_name field

        Compute the extension_name value to use in a GeoArrow extension
        type implementation.
        """
        if self.encoding in _SERIALIZED_EXT_NAMES:
            return _SERIALIZED_EXT_NAMES[self.encoding]
        elif (
            self.encoding == Encoding.GEOARROW
            and self.geometry_type in _GEOARROW_EXT_NAMES
        ):
            return _GEOARROW_EXT_NAMES[self.geometry_type]

        raise ValueError(f"Can't compute extension name for {self}")

    def extension_metadata(self) -> str:
        """Compute the GeoArrow extension_metadata field

        Compute the extension_metadata value to use in a GeoArrow extension
        type implementation.
        """
        metadata = {}

        if self.edge_type == EdgeType.UNSPECIFIED or self.crs == crs.UNSPECIFIED:
            raise ValueError(
                f"Can't compute extension_metadata for {self}: "
                "edge_type or crs is unspecified"
            )

        if self.edge_type == EdgeType.SPHERICAL:
            metadata["edges"] = "spherical"

        if self.crs is not None:
            metadata["crs"] = self.crs.to_json_dict()

        return json.dumps(metadata)

    def __arrow_c_schema__(self):
        # We could potentially use nanoarrow or arro3 here,
        # but use pyarrow if the module is already loaded to
        # avoid requiring those as a dependency.
        return self.to_pyarrow().__arrow_c_schema__()

    def to_pyarrow(self):
        from geoarrow.types.type_pyarrow import extension_type

        return extension_type(self)

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

    def canonicalize(self):
        """Canonicalize the representation of serialized types

        If this type specification represents a serialized type, ensure
        that the dimensions are UNKNOWN, the geometry type is GEOMETRY,
        and the coord type is UNSPECIFIED. These ensure that when a type
        implementation needs to construct a concrete type that its
        components are represented consistently for serialized types.
        """
        if self.encoding.is_serialized():
            return self.override(
                geometry_type=GeometryType.GEOMETRY,
                dimensions=Dimensions.UNKNOWN,
                coord_type=CoordType.UNSPECIFIED,
            )
        else:
            return self

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

    @staticmethod
    def from_extension_name(extension_name: str):
        """Parse an extension name into a TypeSpec

        Extracts the information from a GeoArrow extension name and places
        it in a TypeSpec instance.
        """
        if extension_name in _GEOMETRY_TYPE_FROM_EXT:
            return TypeSpec(
                encoding=Encoding.GEOARROW,
                geometry_type=_GEOMETRY_TYPE_FROM_EXT[extension_name],
            )
        else:
            return TypeSpec()

    @staticmethod
    def from_extension_metadata(extension_metadata: str):
        """Parse extension metadata into a TypeSpec

        Extract the information from a serialized GeoArrow representation and
        into a TypeSpec instance.
        """
        if extension_metadata:
            metadata = json.loads(extension_metadata)
        else:
            metadata = {}

        out_edges = EdgeType.PLANAR
        out_crs = None

        if "edges" in metadata:
            out_edges = EdgeType.create(metadata["edges"])

        if "crs" in metadata and metadata["crs"] is not None:
            out_crs = crs.ProjJsonCrs(metadata["crs"])

        return TypeSpec(edge_type=out_edges, crs=out_crs)

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


def wkb(*, edge_type=None, crs=crs.UNSPECIFIED) -> TypeSpec:
    """Well-known binary encoding

    Create a :class:`TypeSpec` denoting a well-known binary type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(encoding=Encoding.WKB, edge_type=edge_type, crs=crs)


def large_wkb(*, edge_type=None, crs=crs.UNSPECIFIED) -> TypeSpec:
    """Large well-known binary encoding

    Create a :class:`TypeSpec` denoting a well-known binary type  with
    64-bit data offsets. See :func:`type_spec` for parameter definitions.
    """
    return type_spec(encoding=Encoding.LARGE_WKB, edge_type=edge_type, crs=crs)


def wkt(*, edge_type=None, crs=crs.UNSPECIFIED) -> TypeSpec:
    """Well-known text encoding

    Create a :class:`TypeSpec` denoting a well-known text type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(encoding=Encoding.WKT, edge_type=edge_type, crs=crs)


def large_wkt(*, edge_type=None, crs=crs.UNSPECIFIED) -> TypeSpec:
    """Large well-known text encoding

    Create a :class:`TypeSpec` denoting a well-known text type with
    64-bit data offsets. See :func:`type_spec` for parameter definitions.
    """
    return type_spec(encoding=Encoding.LARGE_WKT, edge_type=edge_type, crs=crs)


def geoarrow(
    *,
    geometry_type=None,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow encoding
    without a request for a particular geometry type (e.g. one that might be
    inferred from the data). See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=geometry_type,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def point(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native point encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow point
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.POINT,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def linestring(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native linestring encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow linestring
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.LINESTRING,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def polygon(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native polygon encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow polygon
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.POLYGON,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def multipoint(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native multipoint encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow multipoint
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.MULTIPOINT,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def multilinestring(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native multilinestring encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow multilinestring
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.MULTILINESTRING,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def multipolygon(
    *,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
    """GeoArrow native multipolygon encoding

    Create a :class:`TypeSpec` denoting a preference for GeoArrow multipolygon
    type without an explicit request for dimensions or coordinate type.
    See :func:`type_spec` for parameter definitions.
    """
    return type_spec(
        encoding=Encoding.GEOARROW,
        geometry_type=GeometryType.MULTIPOLYGON,
        dimensions=dimensions,
        coord_type=coord_type,
        edge_type=edge_type,
        crs=crs,
    )


def type_spec(
    *args,
    encoding=None,
    geometry_type=None,
    dimensions=None,
    coord_type=None,
    edge_type=None,
    crs=crs.UNSPECIFIED,
) -> TypeSpec:
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

_SERIALIZED_EXT_NAMES = {
    Encoding.WKB: "geoarrow.wkb",
    Encoding.LARGE_WKB: "geoarrow.wkb",
    Encoding.WKT: "geoarrow.wkt",
    Encoding.LARGE_WKT: "geoarrow.wkt",
}

_GEOARROW_EXT_NAMES = {
    GeometryType.POINT: "geoarrow.point",
    GeometryType.LINESTRING: "geoarrow.linestring",
    GeometryType.POLYGON: "geoarrow.polygon",
    GeometryType.MULTIPOINT: "geoarrow.multipoint",
    GeometryType.MULTILINESTRING: "geoarrow.multilinestring",
    GeometryType.MULTIPOLYGON: "geoarrow.multipolygon",
}

_GEOMETRY_TYPE_FROM_EXT = {v: k for k, v in _GEOARROW_EXT_NAMES.items()}
