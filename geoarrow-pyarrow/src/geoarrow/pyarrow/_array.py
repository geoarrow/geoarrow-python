import pyarrow as pa

from geoarrow.c import lib

from geoarrow.pyarrow._kernel import Kernel
from geoarrow.pyarrow._type import (
    GeometryExtensionType,
    wkb,
    wkt,
    large_wkb,
    large_wkt,
)


class GeometryExtensionArray(pa.ExtensionArray):
    def geobuffers(self):
        import numpy as np

        cschema = lib.SchemaHolder()
        self.type._export_to_c(cschema._addr())
        carray = lib.ArrayHolder()
        self._export_to_c(carray._addr())

        array_view = lib.CArrayView(carray, cschema)
        buffers = array_view.buffers()
        return [np.array(b) if b is not None else None for b in buffers]

    def __repr__(self):
        n_values_to_show = 10
        max_width = 70

        if len(self) > n_values_to_show:
            n_extra = len(self) - n_values_to_show
            value_s = "values" if n_extra != 1 else "value"
            head = self[: int(n_values_to_show / 2)]
            mid = f"...{n_extra} {value_s}..."
            tail = self[int(-n_values_to_show / 2) :]
        else:
            head = self
            mid = ""
            tail = self[:0]

        try:
            kernel = Kernel.format_wkt(self.type, max_element_size_bytes=max_width)
            head = kernel.push(head)
            tail = kernel.push(tail)
        except Exception as e:
            err = f"* 1 or more display values failed to parse\n* {str(e)}"
            type_name = type(self).__name__
            super_repr = super().__repr__()
            return f"{type_name}:{repr(self.type)}[{len(self)}]\n{err}\n{super_repr}"

        head_str = [f"<{item.as_py()}>" for item in head]
        tail_str = [f"<{item.as_py()}>" for item in tail]
        for i in range(len(head)):
            if len(head_str[i]) > max_width:
                head_str[i] = f"{head_str[i][:(max_width - 4)]}...>"
        for i in range(len(tail)):
            if len(tail_str[i]) > max_width:
                tail_str[i] = f"{tail_str[i][:(max_width - 4)]}...>"

        type_name = type(self).__name__
        head_str = "\n".join(head_str)
        tail_str = "\n".join(tail_str)
        items_str = f"{head_str}\n{mid}\n{tail_str}"

        return f"{type_name}:{repr(self.type)}[{len(self)}]\n{items_str}".strip()


class PointArray(GeometryExtensionArray):
    pass


class LinestringArray(GeometryExtensionArray):
    pass


class PolygonArray(GeometryExtensionArray):
    pass


class MultiPointArray(GeometryExtensionArray):
    pass


class MultiLinestringArray(GeometryExtensionArray):
    pass


class MultiPolygonArray(GeometryExtensionArray):
    pass


def array_cls_from_name(name):
    if name == "geoarrow.wkb":
        return GeometryExtensionArray
    elif name == "geoarrow.wkt":
        return GeometryExtensionArray
    elif name == "geoarrow.point":
        return PointArray
    elif name == "geoarrow.linestring":
        return LinestringArray
    elif name == "geoarrow.polygon":
        return PolygonArray
    elif name == "geoarrow.multipoint":
        return MultiPointArray
    elif name == "geoarrow.multilinestring":
        return MultiLinestringArray
    elif name == "geoarrow.multipolygon":
        return MultiPolygonArray
    else:
        raise ValueError(f'Expected valid extension name but got "{name}"')


# Inject array_cls_from_name exactly once to avoid circular import
if GeometryExtensionType._array_cls_from_name is None:
    GeometryExtensionType._array_cls_from_name = array_cls_from_name


def array(obj, type_=None, *args, **kwargs) -> GeometryExtensionArray:
    """Attempt to create an Array or ChunkedArray with a geoarrow extension type
    from ``obj``. This constructor attempts to perform the fewest transformations
    possible (i.e., WKB is left as WKB, WKT is left as WKT), whereas
    :func:`geoarrow.pyarrow.as_geoarrow` actively attempts a conversion to
    a geoarrow-encoding based on a common geometry type. GeoPandas objects are
    supported. This implementation relies heavily on ``pyarrow.array()`` and has
    similar behaviour.

    >>> import geoarrow.pyarrow as ga
    >>> ga.array(["POINT (0 1)"])
    GeometryExtensionArray:WktType(geoarrow.wkt)[1]
    <POINT (0 1)>
    >>> ga.as_geoarrow(["POINT (0 1)"])
    PointArray:PointType(geoarrow.point)[1]
    <POINT (0 1)>
    """
    # Convert GeoPandas to WKB
    if type(obj).__name__ == "GeoSeries":
        if obj.crs:
            try:
                type_ = wkb().with_crs(obj.crs.to_json())
            except:
                type_ = wkb().with_crs(str(obj.crs))
        else:
            type_ = wkb()
        obj = obj.to_wkb()

    # Convert obj to array if it isn't already one
    if isinstance(obj, pa.Array) or isinstance(obj, pa.ChunkedArray):
        arr = obj
    else:
        arr = pa.array(obj, *args, **kwargs)

    # Handle the case where we get to pick the type
    if type_ is None:
        if isinstance(arr.type, GeometryExtensionType):
            return arr
        elif arr.type == pa.utf8():
            return wkt().wrap_array(arr)
        elif arr.type == pa.large_utf8():
            return large_wkt().wrap_array(arr)
        elif arr.type == pa.binary():
            return wkb().wrap_array(arr)
        elif arr.type == pa.large_binary():
            return large_wkb().wrap_array(arr)
        else:
            raise TypeError(
                f"Can't create geoarrow.array from Arrow array of type {type_}"
            )

    # Handle the case where the type requested is already the correct type
    if type_ == arr.type:
        return arr

    type_is_geoarrow = isinstance(type_, GeometryExtensionType)
    type_is_wkb_or_wkt = type_.extension_name in ("geoarrow.wkt", "geoarrow.wkb")

    if type_is_geoarrow and type_is_wkb_or_wkt:
        arr = arr.cast(type_.storage_type)
        return type_.wrap_array(arr)

    # Eventually we will be able to handle more types (e.g., parse wkt or wkb
    # into a geoarrow type)
    raise TypeError(f"Can't create geoarrow.array for type {type_}")
