import re as _re
import pandas as _pd
import pyarrow as _pa
import numpy as _np
from geoarrow.c import lib
import geoarrow.pyarrow as _ga


class GeoArrowExtensionScalar(bytes):
    """Scalar type for GeoArrowExtensionArray

    This is a generic Scalar implementation for a "Geometry". It is currently implemented
    as an immutable subclass of bytes whose value is the well-known binary representation
    of the geometry.
    """

    def __new__(cls, obj, index=None):
        if isinstance(obj, GeoArrowExtensionScalar):
            bytes_value = bytes(obj)
        elif isinstance(obj, bytes):
            bytes_value = obj
        elif isinstance(obj, str):
            wkb_array = _ga.as_wkb([obj])
            bytes_value = wkb_array[0].as_py()
        elif index < 0:
            index = len(obj) + index
            wkb_array = _ga.as_wkb(obj[index : (index + 1)])
            bytes_value = wkb_array[0].as_py()
        else:
            wkb_array = _ga.as_wkb(obj[index : (index + 1)])
            bytes_value = wkb_array[0].as_py()

        # A tiny bit of a hack that should probably be moved outside of here
        if bytes_value is None:
            return None
        else:
            return super().__new__(cls, bytes_value)

    def __hash__(self) -> int:
        return super().__hash__()

    def __str__(self):
        return self.wkt

    def __repr__(self):
        wkt_array = _ga.format_wkt(
            _ga.array([self]), precision=7, max_element_size_bytes=1024
        )
        return f'GeoArrowExtensionScalar("{wkt_array[0].as_py()}")'

    def to_shapely(self):
        """The shapely representation of this feature."""
        from shapely import from_wkb

        return from_wkb(self)

    @property
    def wkt(self):
        """The well-known text representation of this feature."""
        wkt_array = _ga.format_wkt(_ga.array([self]))
        return wkt_array[0].as_py()

    @property
    def wkb(self):
        """The well-known binary representation of this feature."""
        return bytes(self)

    def __eq__(self, other):
        return isinstance(other, GeoArrowExtensionScalar) and bytes(other) == bytes(
            self
        )


class GeoArrowExtensionArray(_pd.api.extensions.ExtensionArray):
    """ExtensionArray implementation wrapping a geoarrow Array

    This ExtensionArray implementation currently wraps a ``pyarrow.Array``
    or ``pyarrow.ChunkedArray`` with an extension type. Most users will
    not instantiate this class directly.
    """

    def __init__(self, obj, type=None):
        if type is not None:
            self._dtype = GeoArrowExtensionDtype(type)
            arrow_type = _ga.GeometryExtensionType._from_ctype(self._dtype._parent)
            self._data = _ga.array(obj, arrow_type)
        else:
            self._data = _ga.array(obj)
            self._dtype = GeoArrowExtensionDtype(self._data.type)

    @classmethod
    def _from_sequence(cls, scalars, *, dtype=None, copy=False):
        wkb_array = _pa.array(iter(scalars), type=_pa.binary())
        arrow_type = dtype.pyarrow_dtype if dtype is not None else None
        pa_array = _ga.as_geoarrow(wkb_array, arrow_type)
        return GeoArrowExtensionArray(pa_array, type=dtype)

    @classmethod
    def _from_sequence_of_strings(cls, strings, *, dtype=None, copy=False):
        return GeoArrowExtensionArray(strings, dtype)

    def __getitem__(self, item):
        if isinstance(item, int):
            if self._data.is_valid()[item]:
                return GeoArrowExtensionScalar(self._data, item)
            else:
                return None
        elif isinstance(item, slice):
            return GeoArrowExtensionArray(self._data[item])
        elif isinstance(item, list):
            return self.take(item)
        elif hasattr(item, "dtype") and item.dtype.kind == "i":
            return self.take(item)
        elif hasattr(item, "dtype") and item.dtype.kind == "b":
            if len(item) != len(self):
                raise IndexError(
                    f"Boolean index has wrong length: {len(item)} instead of {len(self)}"
                )
            return GeoArrowExtensionArray(
                self._data.filter(_pa.array(item, _pa.bool_()))
            )
        elif isinstance(item, tuple) and item[0] is Ellipsis:
            return self[item[1]]
        elif isinstance(item, tuple) and item[1] is Ellipsis:
            return self[item[0]]
        else:
            raise IndexError(
                "only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices"
            )

    def __contains__(self, item: object):
        for scalar in self:
            if scalar == item:
                return True

        return False

    def __len__(self):
        return len(self._data)

    def __eq__(self, other):
        if isinstance(other, GeoArrowExtensionScalar):
            array = _pa.array(item == other for item in self)
        else:
            array = _pa.array(
                item == other_item for item, other_item in zip(self, other)
            )

        return array.to_numpy(zero_copy_only=False)

    @property
    def dtype(self):
        return self._dtype

    @property
    def nbytes(self):
        return self._data.nbytes

    def take(self, indices, allow_fill=False, fill_value=None):
        if allow_fill or fill_value is not None:
            objecified = _np.array(list(self), dtype=object)
            # fill_value=None results in nans instead of Nones
            result = _pd.api.extensions.take(
                objecified, indices, allow_fill=allow_fill, fill_value=False
            )
            result = [item if item is not False else fill_value for item in result]
            return GeoArrowExtensionArray._from_sequence(result, dtype=self.dtype)

        if len(self) == 0 and len(indices) != 0:
            raise IndexError("cannot do a non-empty take from an empty axes")

        if isinstance(indices, list):
            indices = _pd.array(indices, dtype=_pd.Int32Dtype())

        if _np.any(indices < 0):
            indices = indices.copy()
            indices[indices < 0] = len(self) + indices[indices < 0]

        indices = _pa.array(indices)
        if indices.null_count > 0:
            raise ValueError(
                "Cannot index with an integer indexer containing NA values"
            )
        return GeoArrowExtensionArray(self._data.take(indices), self._dtype)

    def isna(self):
        out = self._data.is_null()
        if isinstance(out, _pa.ChunkedArray):
            return out.to_numpy()
        else:
            return out.to_numpy(zero_copy_only=False)

    def copy(self):
        return GeoArrowExtensionArray._from_sequence(self, dtype=self._dtype)

    @classmethod
    def _concat_same_type(cls, to_concat):
        items = list(to_concat)
        if len(items) == 0:
            return GeoArrowExtensionArray([], _ga.wkb())
        if len(items) == 1:
            return items[0]

        types = [item._data.type for item in to_concat]
        common_type = _ga.geometry_type_common(types)

        chunks = []
        for item in to_concat:
            data = item._data
            if isinstance(data, _pa.ChunkedArray):
                for chunk in data.chunks:
                    chunks.append(chunk)
            else:
                chunks.append(_pa.array(item))

        if all(type == common_type for type in types):
            return GeoArrowExtensionArray(_pa.chunked_array(chunks, common_type))
        else:
            chunks = [_ga.as_geoarrow(chunk, type=common_type) for chunk in chunks]
            return GeoArrowExtensionArray(_pa.chunked_array(chunks, common_type))

    def __arrow_array__(self, type=None):
        if type is None or type == self._data.type:
            return self._data

        raise ValueError(
            f"Can't export GeoArrowExtensionArray with type {str(self.dtype)} as {str(type)}"
        )

    def to_numpy(self, dtype=None, copy=False, na_value=None):
        if dtype is not None and dtype is not object and dtype != "O":
            raise TypeError("to_numpy() with dtype != None not supported")
        if na_value is not None:
            raise TypeError("to_numpy() with na_value != None not supported")

        return _np.array(list(self), dtype=object)

    def __array__(self, dtype=None):
        return self.to_numpy(dtype=dtype)


@_pd.api.extensions.register_extension_dtype
class GeoArrowExtensionDtype(_pd.api.extensions.ExtensionDtype):
    """ExtensionDtype implementation wrapping a geoarrow type

    The dtype object for geoarrow-encoded arrays that are converted
    to pandas. Use the ``pyarrow_dtype`` property to return the underlying
    :class:`geoarrow.pyarrow.GeometryExtensionType` (e.g., to query the
    ``crs`` or ``dimensions``).
    """

    _match = _re.compile(
        r"^geoarrow."
        r"(?P<type>wkt|wkb|point|linestring|polygon|multipoint|multilinestring|multipolygon)"
        r"(?P<dims>\[z\]|\[m\]|\[zm\])?"
        r"(?P<coord_type>\[interleaved\])?"
        r"(?P<metadata>.*)$"
    )

    def __init__(self, parent):
        if isinstance(parent, _ga.GeometryExtensionType):
            self._parent = parent._type
        elif isinstance(parent, lib.CVectorType):
            self._parent = parent
        elif isinstance(parent, GeoArrowExtensionDtype):
            self._parent = parent._parent
        else:
            raise TypeError(
                "`geoarrow_type` must inherit from geoarrow.pyarrow.VectorType, "
                "geoarrow.CVectorType, or geoarrow.pandas.GeoArrowExtensionDtype"
            )

    @property
    def pyarrow_dtype(self):
        return _ga.GeometryExtensionType._from_ctype(self._parent)

    @property
    def type(self):
        return GeoArrowExtensionScalar

    @classmethod
    def construct_array_type(cls):
        return GeoArrowExtensionArray

    @classmethod
    def construct_from_string(cls, string):
        if not isinstance(string, str):
            raise TypeError(
                f"'construct_from_string' expects a string, got {type(string)}"
            )

        matched = cls._match.match(string)
        if not matched:
            raise TypeError(
                f"Cannot construct a 'GeoArrowExtensionDtype' from '{string}'"
            )

        params = matched.groupdict()

        if params["dims"] == "[z]":
            dims = _ga.Dimensions.XYZ
        elif params["dims"] == "[m]":
            dims = _ga.Dimensions.XYM
        elif params["dims"] == "[zm]":
            dims = _ga.Dimensions.XYZM
        elif params["type"] in ("wkt", "wkb"):
            dims = _ga.Dimensions.UNKNOWN
        else:
            dims = _ga.Dimensions.XY

        if params["coord_type"] == "[interleaved]":
            coord_type = _ga.CoordType.INTERLEAVED
        elif params["type"] in ("wkt", "wkb"):
            coord_type = _ga.CoordType.UNKNOWN
        else:
            coord_type = _ga.CoordType.SEPARATE

        if params["type"] == "point":
            geometry_type = _ga.GeometryType.POINT
        elif params["type"] == "linestring":
            geometry_type = _ga.GeometryType.LINESTRING
        elif params["type"] == "polygon":
            geometry_type = _ga.GeometryType.POLYGON
        elif params["type"] == "multipoint":
            geometry_type = _ga.GeometryType.MULTIPOINT
        elif params["type"] == "multilinestring":
            geometry_type = _ga.GeometryType.MULTILINESTRING
        elif params["type"] == "multipolygon":
            geometry_type = _ga.GeometryType.MULTIPOLYGON
        else:
            geometry_type = _ga.GeometryType.GEOMETRY

        if params["type"] == "wkb":
            base_type = _ga.wkb()
        elif params["type"] == "wkt":
            base_type = _ga.wkt()
        else:
            base_type = _ga.extension_type(geometry_type, dims, coord_type)

        try:
            if params["metadata"]:
                return GeoArrowExtensionDtype(
                    base_type.with_metadata(params["metadata"])
                )
            else:
                return GeoArrowExtensionDtype(base_type)
        except Exception as e:
            raise TypeError(
                f"Cannot construct a 'GeoArrowExtensionDtype' from '{string}'"
            ) from e

    def __repr__(self):
        return f"{type(self).__name__}({repr(self._parent)})"

    def __str__(self):
        ext_name = self._parent.extension_name
        ext_dims = self._parent.dimensions
        ext_coord = self._parent.coord_type
        ext_meta = self._parent.extension_metadata.decode("UTF-8")

        if ext_dims == _ga.Dimensions.XYZ:
            dims_str = "[z]"
        elif ext_dims == _ga.Dimensions.XYM:
            dims_str = "[m]"
        elif ext_dims == _ga.Dimensions.XYZM:
            dims_str = "[zm]"
        else:
            dims_str = ""

        if ext_coord == _ga.CoordType.INTERLEAVED:
            coord_str = "[interleaved]"
        else:
            coord_str = ""

        if ext_meta == "{}":
            meta_str = ""
        else:
            meta_str = ext_meta

        return f"{ext_name}{dims_str}{coord_str}{meta_str}"

    def __hash__(self):
        return hash(str(self))

    @property
    def name(self):
        return str(self)

    @property
    def na_value(self):
        return None

    def __from_arrow__(self, array):
        return GeoArrowExtensionArray(array)


@_pd.api.extensions.register_series_accessor("geoarrow")
class GeoArrowAccessor:
    """
    GeoArrow series accessor

    The GeoArrow series accessor adds a convenient way to apply the type
    introspection and coordinate shuffling capabilities of geoarrow-pyarrow
    to columns in a pandas data frame. The accessor can be applied to
    text columns (interpreted as WKT), binary columns (interpreted as WKB),
    :class:`GeoArrowExtensionDtype` columns, or a ``geopandas.GeoSeries``.

    >>> import geoarrow.pandas as _
    >>> import pandas as pd
    >>> series = pd.Series(["POINT (0 1)", "POINT (2 3)"])
    >>> x, y = series.geoarrow.point_coords()
    >>> x
    0    0.0
    1    2.0
    dtype: float64
    >>> y
    0    1.0
    1    3.0
    dtype: float64
    """

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def _wrap_series(self, array_or_chunked):
        return _pd.Series(
            GeoArrowExtensionArray(array_or_chunked), index=self._obj.index
        )

    def _obj_is_geoarrow(self):
        return isinstance(self._obj.dtype, GeoArrowExtensionDtype)

    def parse_all(self):
        """See :func:`geoarrow.pyarrow.parse_all`"""
        _ga.parse_all(self._obj)
        return self._obj

    def as_wkt(self):
        """See :func:`geoarrow.pyarrow.as_wkt`"""
        return self._wrap_series(_ga.as_wkt(self._obj))

    def as_wkb(self):
        """See :func:`geoarrow.pyarrow.as_wkb`"""
        return self._wrap_series(_ga.as_wkb(self._obj))

    def format_wkt(self, precision=None, max_element_size_bytes=None):
        """See :func:`geoarrow.pyarrow.format_wkt`"""
        if not self._obj_is_geoarrow():
            raise TypeError("Can't format_wkt() a non-geoarrow Series")

        array_or_chunked = _ga.format_wkt(
            _pa.array(self._obj),
            precision=precision,
            max_element_size_bytes=max_element_size_bytes,
        )
        return _pd.Series(
            array_or_chunked,
            index=self._obj.index,
            dtype=_pd.ArrowDtype(array_or_chunked.type),
        )

    def format_wkb(self):
        """See :func:`geoarrow.pyarrow.as_wkb`"""
        if not self._obj_is_geoarrow():
            raise TypeError("Can't format_wkb() a non-geoarrow Series")

        array_or_chunked = _ga.as_wkb(_pa.array(self._obj))

        if isinstance(array_or_chunked, _pa.ChunkedArray):
            storage = [chunk.storage for chunk in array_or_chunked.chunks]
            array_or_chunked = _pa.chunked_array(storage, _pa.binary())
        else:
            array_or_chunked = array_or_chunked.storage

        return _pd.Series(
            array_or_chunked,
            index=self._obj.index,
            dtype=_pd.ArrowDtype(array_or_chunked.type),
        )

    def as_geoarrow(self, type=None, coord_type=None):
        """See :func:`geoarrow.pyarrow.as_geoarrow`"""
        array_or_chunked = _ga.as_geoarrow(self._obj, type=type, coord_type=coord_type)
        return self._wrap_series(array_or_chunked)

    def bounds(self):
        """See :func:`geoarrow.pyarrow.box`"""
        array_or_chunked = _ga.box(self._obj)
        if isinstance(array_or_chunked, _pa.ChunkedArray):
            flattened = [chunk.flatten() for chunk in array_or_chunked.chunks]
            seriesish = [
                _pa.chunked_array(item, _pa.float64()) for item in zip(*flattened)
            ]
        else:
            seriesish = array_or_chunked.flatten()

        return _pd.DataFrame(
            {
                "xmin": seriesish[0],
                "xmax": seriesish[1],
                "ymin": seriesish[2],
                "ymax": seriesish[3],
            },
            index=self._obj.index,
        )

    def total_bounds(self):
        """See :func:`geoarrow.pyarrow.box_agg`"""
        struct_scalar1 = _ga.box_agg(self._obj)
        return _pd.DataFrame({k: [v] for k, v in struct_scalar1.as_py().items()})

    def with_coord_type(self, coord_type):
        """See :func:`geoarrow.pyarrow.with_coord_type`"""
        return self._wrap_series(_ga.with_coord_type(self._obj, coord_type))

    def with_edge_type(self, edge_type):
        """See :func:`geoarrow.pyarrow.with_edge_type`"""
        return self._wrap_series(_ga.with_edge_type(self._obj, edge_type))

    def with_crs(self, crs, crs_type=None):
        """See :func:`geoarrow.pyarrow.with_crs`"""
        return self._wrap_series(_ga.with_crs(self._obj, crs=crs, crs_type=crs_type))

    def with_dimensions(self, dimensions):
        """See :func:`geoarrow.pyarrow.with_dimensions`"""
        return self._wrap_series(_ga.with_dimensions(self._obj, dimensions))

    def with_geometry_type(self, geometry_type):
        """See :func:`geoarrow.pyarrow.with_geometry_type`"""
        return self.with_geometry_type(_ga.with_coord_type(self._obj, geometry_type))

    def point_coords(self, dimensions=None):
        """See :func:`geoarrow.pyarrow.point_coords`"""
        point_coords = _ga.point_coords(_ga.with_coord_type(self._obj, dimensions))
        return tuple(_pd.Series(dim, index=self._obj.index) for dim in point_coords)

    def to_geopandas(self):
        """See :func:`geoarrow.pyarrow.to_geopandas`"""
        return _ga.to_geopandas(self._obj)
