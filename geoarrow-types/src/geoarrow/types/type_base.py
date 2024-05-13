from typing import Optional
import json
from geoarrow.types.crs import Crs
from geoarrow.types.constants import (
    Encoding,
    GeometryType,
    Dimensions,
    CoordType,
    EdgeType,
)


class InvalidTypeError(RuntimeError):
    pass


class GeoArrowType:
    def __init__(self, encoding: Encoding) -> None:
        self._encoding = Encoding(encoding)
        self._geometry_type = GeometryType.GEOMETRY
        self._dimensions = Dimensions.UNKNOWN
        self._coord_type = CoordType.UNKNOWN
        self._edge_type = EdgeType.PLANAR
        self._crs = None

    def _check_crs(self, crs):
        if crs is None or hasattr(crs, "to_json_dict"):
            return crs

        raise TypeError("crs must be None or have a to_json_dict()  method")

    @property
    def encoding(self) -> Encoding:
        return self._encoding

    @property
    def geometry_type(self) -> GeometryType:
        return self._geometry_type

    @property
    def dimensions(self) -> Dimensions:
        return self._dimensions

    @property
    def coord_type(self) -> CoordType:
        return self._coord_type

    @property
    def edge_type(self) -> EdgeType:
        return self._edge_type

    @property
    def crs(self) -> Optional[Crs]:
        return self._crs

    @property
    def extension_metadata(self) -> str:
        metadata = {}

        if self.crs is not None:
            metadata["crs"] = self.crs.to_json_dict()

        if self.edge_type == EdgeType.SPHERICAL:
            metadata["edges"] = "spherical"

        return json.dumps(metadata)

    @property
    def extension_name(self) -> str:
        raise NotImplementedError()

    def with_encoding(self, encoding: Encoding) -> "GeoArrowType":
        raise NotImplementedError()

    def with_edge_type(self, edge_type: EdgeType) -> "GeoArrowType":
        raise NotImplementedError()

    def with_crs(self, crs: Crs) -> "GeoArrowType":
        raise NotImplementedError()


class SerializedType(GeoArrowType):
    def __init__(
        self,
        encoding: Encoding,
        edge_type: EdgeType = EdgeType.PLANAR,
        crs: Optional[Crs] = None,
    ) -> None:
        super().__init__(encoding)

        if encoding not in (
            Encoding.WKT,
            Encoding.WKB,
            Encoding.LARGE_WKT,
            Encoding.LARGE_WKB,
        ):
            raise ValueError(
                "Serialized type encoding must be one of [LARGE_]WKT or [LARGE_]WKB"
            )

        self._edge_type = EdgeType(edge_type)
        self._crs = self._check_crs(crs)

    @property
    def extension_name(self) -> str:
        return _SERIALIZED_EXTENSION_NAMES[self.encoding]

    def with_encoding(self, encoding: Encoding) -> "SerializedType":
        return SerializedType(encoding, edge_type=self.edge_type, crs=self.crs)

    def with_edge_type(self, edge_type: EdgeType) -> "SerializedType":
        return SerializedType(self.encoding, edge_type, self.crs)

    def with_crs(self, crs: Crs) -> "SerializedType":
        return SerializedType(self.encoding, self.edge_type, crs)


class NativeType(GeoArrowType):
    def __init__(
        self,
        geometry_type: GeometryType,
        coord_type: CoordType,
        dimensions: Dimensions = Dimensions.XY,
        edge_type: EdgeType = EdgeType.PLANAR,
        crs: Optional[Crs] = None,
    ) -> None:
        super().__init__(Encoding.GEOARROW)

        self._geometry_type = GeometryType(geometry_type)
        self._coord_type = CoordType(coord_type)
        self._dimensions = Dimensions(dimensions)
        self._edge_type = EdgeType(edge_type)
        self._crs = self._check_crs(crs)

        if self._geometry_type in (
            GeometryType.GEOMETRY,
            GeometryType.GEOMETRYCOLLECTION,
        ):
            raise InvalidTypeError(
                "Encoding GEOARROW not implemented for geometry type "
                f"{self._geometry_type}"
            )

    @property
    def extension_name(self) -> str:
        return _NATIVE_EXTENSION_NAMES[self.geometry_type]

    def with_encoding(self, encoding: Encoding) -> SerializedType:
        return SerializedType(encoding, edge_type=self.edge_type, crs=self.crs)

    def with_geometry_type(self, geometry_type: GeometryType):
        return NativeType(
            geometry_type, self.coord_type, self.dimensions, self.edge_type, self.crs
        )

    def with_coord_type(self, coord_type: CoordType):
        return NativeType(
            self.geometry_type,
            coord_type,
            self.dimensions,
            self.edge_type,
            self.crs,
        )

    def with_dimensions(self, dimensions: Dimensions) -> "NativeType":
        return NativeType(
            self.geometry_type, self.coord_type, dimensions, self.edge_type, self.crs
        )

    def with_edge_type(self, edge_type: EdgeType) -> "SerializedType":
        return NativeType(
            self.geometry_type, self.coord_type, self.dimensions, edge_type, self.crs
        )

    def with_crs(self, crs: Crs) -> "SerializedType":
        return NativeType(
            self.geometry_type, self.coord_type, self.dimensions, self.edge_type, crs
        )


def geoarrow_type(
    encoding: Encoding,
    geometry_type: Optional[GeometryType] = None,
    coord_type: Optional[CoordType] = None,
    dimensions: Optional[Dimensions] = None,
    edge_type: EdgeType = EdgeType.PLANAR,
    crs: Optional[Crs] = None,
):
    encoding = Encoding(encoding)

    if encoding == Encoding.GEOARROW:
        # Error if required parameters were unspecified
        if geometry_type is None or coord_type is None:
            raise InvalidTypeError(
                "geometry_type and coord_type must be supplied when "
                "constructing a GEOARROW encoding"
            )

        if dimensions is None:
            dimensions = Dimensions.XY

        return NativeType(geometry_type, coord_type, dimensions, edge_type, crs)
    else:
        # Error if native-specific parameters were specified
        if (
            geometry_type is not None
            or coord_type is not None
            or dimensions is not None
        ):
            raise InvalidTypeError(
                "geometry_type, coord_type, and dimensions must be unspecified "
                "when constructing a serialized encoding"
            )

        return SerializedType(encoding, edge_type, crs)


_SERIALIZED_EXTENSION_NAMES = {
    Encoding.WKT: "geoarrow.wkt",
    Encoding.LARGE_WKT: "geoarrow.wkt",
    Encoding.WKB: "geoarrow.wkb",
    Encoding.LARGE_WKB: "geoarrow.wkb",
}

_NATIVE_EXTENSION_NAMES = {
    GeometryType.POINT: "geoarrow.point",
    GeometryType.LINESTRING: "geoarrow.linestring",
    GeometryType.POLYGON: "geoarrow.polygon",
    GeometryType.MULTIPOINT: "geoarrow.multipoint",
    GeometryType.MULTILINESTRING: "geoarrow.multilinestring",
    GeometryType.MULTIPOLYGON: "geoarrow.multipolygon",
}
