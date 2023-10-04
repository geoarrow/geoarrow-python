import sys

import pyarrow as pa

from geoarrow.c import lib
from geoarrow.pyarrow._type import GeometryExtensionType


class Kernel:
    def __init__(self, name, type_in, **kwargs) -> None:
        if not isinstance(type_in, pa.DataType):
            raise TypeError("Expected `type_in` to inherit from pyarrow.DataType")

        self._name = str(name)
        self._kernel = lib.CKernel(self._name.encode("UTF-8"))
        # True for all the kernels that currently exist
        self._is_agg = self._name.endswith("_agg")

        type_in_schema = lib.SchemaHolder()
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

        array_in = lib.ArrayHolder()
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
        return Kernel.as_geoarrow(type_in, 100003)

    @staticmethod
    def as_wkb(type_in):
        return Kernel.as_geoarrow(type_in, 100001)

    @staticmethod
    def format_wkt(type_in, precision=None, max_element_size_bytes=None):
        return Kernel(
            "format_wkt",
            type_in,
            precision=precision,
            max_element_size_bytes=max_element_size_bytes,
        )

    @staticmethod
    def as_geoarrow(type_in, type_id):
        return Kernel("as_geoarrow", type_in, type=int(type_id))

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
