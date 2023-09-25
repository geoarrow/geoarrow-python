import pyarrow as pa

from ._type import VectorType


class VectorScalar(pa.ExtensionScalar):
    pass


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
        return VectorScalar
    elif name == "geoarrow.wkt":
        return VectorScalar
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
if VectorType._scalar_cls_from_name is None:
    VectorType._scalar_cls_from_name = scalar_cls_from_name
