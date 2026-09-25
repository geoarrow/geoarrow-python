import sys

from geoarrow.pyarrow._type import GeometryExtensionType
from geoarrow.types import CoordType, Dimensions, Encoding, GeometryType
from geoarrow.types import wkb as wkb_spec
from geoarrow.types import wkt as wkt_spec

import pyarrow as pa

_lazy_lib = None


def _geoarrow_c():
    global _lazy_lib
    if _lazy_lib is None:
        try:
            import geoarrow.c

        except ImportError as e:
            raise ImportError("Requested operation requires geoarrow-c") from e

        _lazy_lib = geoarrow.c.lib

    return _lazy_lib


class Kernel:
    def __init__(self, name, type_in, **kwargs) -> None:
        if not isinstance(type_in, pa.DataType):
            raise TypeError("Expected `type_in` to inherit from pyarrow.DataType")

        self._name = str(name)
        self._kernel = _geoarrow_c().CKernel(self._name.encode("UTF-8"))
        # True for all the kernels that currently exist
        self._is_agg = self._name.endswith("_agg")

        type_in_schema = _geoarrow_c().SchemaHolder()
        type_in._export_to_c(type_in_schema._addr())

        options = Kernel._pack_options(kwargs)

        type_out_schema = self._kernel.start(type_in_schema, options)
        self._type_out = GeometryExtensionType._import_from_c(type_out_schema._addr())
        self._type_in = type_in

    def push(self, arr):
        if isinstance(arr, pa.ChunkedArray) and self._is_agg:
            for chunk_in in arr.chunks:
                self.push(chunk_in)
            return
        elif isinstance(arr, pa.ChunkedArray):
            chunks_out = []
            for chunk_in in arr.chunks:
                chunks_out.append(self.push(chunk_in))
            return pa.chunked_array(chunks_out, type=self._type_out)
        elif not isinstance(arr, pa.Array):
            raise TypeError(
                f"Expected pyarrow.Array or pyarrow.ChunkedArray but got {type(arr)}"
            )

        array_in = _geoarrow_c().ArrayHolder()
        arr._export_to_c(array_in._addr())

        if self._is_agg:
            self._kernel.push_batch_agg(array_in)
        else:
            array_out = self._kernel.push_batch(array_in)
            return pa.Array._import_from_c(array_out._addr(), self._type_out)

    def finish(self):
        if self._is_agg:
            out = self._kernel.finish_agg()
            return pa.Array._import_from_c(out._addr(), self._type_out)
        else:
            self._kernel.finish()

    @staticmethod
    def void(type_in):
        return Kernel("void", type_in)

    @staticmethod
    def void_agg(type_in):
        return Kernel("void_agg", type_in)

    @staticmethod
    def visit_void_agg(type_in):
        return Kernel("visit_void_agg", type_in)

    @staticmethod
    def as_wkt(type_in):
        return Kernel.as_geoarrow(type_in, wkt_spec().to_pyarrow())

    @staticmethod
    def as_wkb(type_in):
        return Kernel.as_geoarrow(type_in, wkb_spec().to_pyarrow())

    @staticmethod
    def format_wkt(type_in, precision=None, max_element_size_bytes=None):
        return Kernel(
            "format_wkt",
            type_in,
            precision=precision,
            max_element_size_bytes=max_element_size_bytes,
        )

    @staticmethod
    def as_geoarrow(type_in, type_out):
        return Kernel("as_geoarrow", type_in, type=_type_id(type_out))

    @staticmethod
    def unique_geometry_types_agg(type_in):
        return Kernel("unique_geometry_types_agg", type_in)

    @staticmethod
    def box(type_in):
        return Kernel("box", type_in)

    @staticmethod
    def box_agg(type_in):
        return Kernel("box_agg", type_in)

    @staticmethod
    def _pack_options(options):
        if not options:
            return b""

        options = {k: v for k, v in options.items() if v is not None}
        bytes = len(options).to_bytes(4, sys.byteorder, signed=True)
        for k, v in options.items():
            k = str(k)
            bytes += len(k).to_bytes(4, sys.byteorder, signed=True)
            bytes += k.encode("UTF-8")

            v = str(v)
            bytes += len(v).to_bytes(4, sys.byteorder, signed=True)
            bytes += v.encode("UTF-8")

        return bytes


def _type_id(type_out):
    """Previous versions used geoarrow-c's exposed CVectorType for this; however,
    the CVectorType has been deprecated in favour of geoarrow.types. This keeps
    geoarrow-pyarrow working against previous and future versions of geoarrow-c
    while freeing up geoarrow-c to remove code that was used only here."""
    spec = type_out.spec

    if spec.encoding in _SERIALIZED_TYPE_IDS:
        return _SERIALIZED_TYPE_IDS[spec.encoding]

    if spec.encoding != Encoding.GEOARROW:
        raise ValueError(f"Unsupported GeoArrow encoding: {spec.encoding}")

    try:
        return (
            _GEOMETRY_TYPE_IDS[spec.geometry_type]
            + _DIMENSION_TYPE_ID_OFFSETS[spec.dimensions]
            + _COORD_TYPE_ID_OFFSETS[spec.coord_type]
        )
    except KeyError as e:
        raise ValueError(f"Unsupported GeoArrow type: {spec}") from e


_SERIALIZED_TYPE_IDS = {
    Encoding.WKB: 100001,
    Encoding.LARGE_WKB: 100002,
    Encoding.WKT: 100003,
    Encoding.LARGE_WKT: 100004,
    Encoding.WKB_VIEW: 100005,
    Encoding.WKT_VIEW: 100006,
}

_GEOMETRY_TYPE_IDS = {
    GeometryType.POINT: 1,
    GeometryType.LINESTRING: 2,
    GeometryType.POLYGON: 3,
    GeometryType.MULTIPOINT: 4,
    GeometryType.MULTILINESTRING: 5,
    GeometryType.MULTIPOLYGON: 6,
    GeometryType.BOX: 990,
}

_DIMENSION_TYPE_ID_OFFSETS = {
    Dimensions.XY: 0,
    Dimensions.XYZ: 1000,
    Dimensions.XYM: 2000,
    Dimensions.XYZM: 3000,
}

_COORD_TYPE_ID_OFFSETS = {
    CoordType.SEPARATED: 0,
    CoordType.INTERLEAVED: 10000,
}
