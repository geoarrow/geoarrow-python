import pyarrow as pa

from geoarrow.pyarrow._type import GeometryExtensionType
from geoarrow.pyarrow._kernel import Kernel


class GeometryExtensionScalar(pa.ExtensionScalar):
    def __repr__(self):
        # Before pyarrow 13.0.0 this will fail because constructing
        # arrays from scalars
        pa_version = [int(component) for component in pa.__version__.split(".")]
        if pa_version[0] < 13:
            return super().__repr__()

        max_width = 70

        try:
            kernel = Kernel.format_wkt(self.type, max_element_size_bytes=max_width)
            array_formatted = kernel.push(self._array1())
            string_formatted = array_formatted[0].as_py()
        except:
            string_formatted = "<value failed to parse>"

        if len(string_formatted) >= max_width:
            string_formatted = string_formatted[: (max_width - 3)] + "..."

        return f"{type(self).__name__}\n<{string_formatted}>"

    def _array1(self):
        return self.type.wrap_array(pa.array([self.value]))

    @property
    def wkt(self):
        kernel = Kernel.as_wkt(self.type)
        array_wkt = kernel.push(self._array1())
        return array_wkt.storage[0].as_py()

    @property
    def wkb(self):
        kernel = Kernel.as_wkb(self.type)
        array_wkb = kernel.push(self._array1())
        return array_wkb.storage[0].as_py()

    def to_shapely(self):
        """
        Convert an array item to a shapely geometry

        >>> import geoarrow.pyarrow as ga
        >>> array = ga.array(["POINT (30 10)"])
        >>> array[0].to_shapely()
        <POINT (30 10)>
        """
        from shapely import from_wkb

        return from_wkb(self.wkb)


class WktScalar(GeometryExtensionScalar):
    @property
    def wkt(self):
        return self.value.as_py()

    def to_shapely(self):
        from shapely import from_wkt

        return from_wkt(self.value.as_py())


class WkbScalar(GeometryExtensionScalar):
    @property
    def wkb(self):
        return self.value.as_py()


class PointScalar(GeometryExtensionScalar):
    pass


class LinestringScalar(GeometryExtensionScalar):
    pass


class PolygonScalar(GeometryExtensionScalar):
    pass


class MultiPointScalar(GeometryExtensionScalar):
    pass


class MultiLinestringScalar(GeometryExtensionScalar):
    pass


class MultiPolygonScalar(GeometryExtensionScalar):
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
