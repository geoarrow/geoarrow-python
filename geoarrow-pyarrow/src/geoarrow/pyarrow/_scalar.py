import pyarrow as pa

from geoarrow.pyarrow._type import GeometryExtensionType


class VectorScalar(pa.ExtensionScalar):
    def to_shapely(self):
        """
        Convert an array item to a shapely geometry

        >>> import geoarrow.pyarrow as ga
        >>> array = ga.array(["POINT (30 10)"])
        >>> array[0].to_shapely()
        <POINT (30 10)>
        """
        raise NotImplementedError()


class WktScalar(pa.ExtensionScalar):
    def to_shapely(self):
        from shapely import from_wkt

        return from_wkt(self.value.as_py())


class WkbScalar(pa.ExtensionScalar):
    def to_shapely(self):
        from shapely import from_wkb

        return from_wkb(self.value.as_py())


class PointScalar(VectorScalar):
    pass


class LinestringScalar(VectorScalar):
    pass


class PolygonScalar(VectorScalar):
    pass


class MultiPointScalar(VectorScalar):
    pass


class MultiLinestringScalar(VectorScalar):
    pass


class MultiPolygonScalar(VectorScalar):
    pass


def scalar_cls_from_name(name):
    if name == "geoarrow.wkb":
        return WkbScalar
    elif name == "geoarrow.wkt":
        return WktScalar
    elif name == "geoarrow.point":
        return PointScalar
    elif name == "geoarrow.linestring":
        return LinestringScalar
    elif name == "geoarrow.polygon":
        return PolygonScalar
    elif name == "geoarrow.multipoint":
        return MultiPointScalar
    elif name == "geoarrow.multilinestring":
        return MultiLinestringScalar
    elif name == "geoarrow.multipolygon":
        return MultiPolygonScalar
    else:
        raise ValueError(f'Expected valid extension name but got "{name}"')


# Inject array_cls_from_name exactly once to avoid circular import
if GeometryExtensionType._scalar_cls_from_name is None:
    GeometryExtensionType._scalar_cls_from_name = scalar_cls_from_name
