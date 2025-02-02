from typing import Optional

import pyarrow as pa
import pyarrow_hotfix as _  # noqa: F401
from geoarrow.pyarrow._kernel import Kernel
from geoarrow.types.type_pyarrow import GeometryExtensionType


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
        except Exception:
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


class BoxScalar(GeometryExtensionScalar):
    @property
    def bounds(self) -> dict:
        storage = self._array1().storage
        fields = [storage.type.field(i) for i in range(storage.type.num_fields)]
        return {k.name: v[0].as_py() for k, v in zip(fields, storage.flatten())}

    @property
    def xmin(self) -> float:
        return self.bounds["xmin"]

    @property
    def ymin(self) -> float:
        return self.bounds["ymin"]

    @property
    def xmax(self) -> float:
        return self.bounds["xmax"]

    @property
    def ymax(self) -> float:
        return self.bounds["ymax"]

    @property
    def zmin(self) -> Optional[float]:
        return self.bounds["zmin"] if "zmin" in self.bounds else None

    @property
    def zmax(self) -> Optional[float]:
        return self.bounds["zmax"] if "zmax" in self.bounds else None

    @property
    def mmin(self) -> Optional[float]:
        return self.bounds["mmin"] if "mmin" in self.bounds else None

    @property
    def mmax(self) -> Optional[float]:
        return self.bounds["mmax"] if "mmax" in self.bounds else None

    def __repr__(self) -> str:
        return f"BoxScalar({self.bounds})"


def scalar_cls_from_name(name):
    if name == "geoarrow.wkb":
        return WkbScalar
    elif name == "geoarrow.wkt":
        return WktScalar
    elif name == "geoarrow.box":
        return BoxScalar
    else:
        return GeometryExtensionScalar


# Inject array_cls_from_name exactly once to avoid circular import
if GeometryExtensionType._scalar_cls_from_name is None:
    GeometryExtensionType._scalar_cls_from_name = scalar_cls_from_name
