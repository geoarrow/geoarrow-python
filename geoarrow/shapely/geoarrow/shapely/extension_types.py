from enum import Enum
from typing import Optional

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

import shapely
from shapely import GeometryType

from .extension_array import PointArray


class CoordinateDimension(str, Enum):
    XY = "xy"
    XYZ = "xyz"
    XYM = "xym"
    XYZM = "xyzm"


class BaseGeometryType(pa.ExtensionType):
    extension_name: str
    coord_dimension: CoordinateDimension

    # def __init__(self):
    #     # attributes need to be set first before calling
    #     # super init (as that calls serialize)
    #     # self._crs = crs
    #     pa.ExtensionType.__init__(self, self._storage_type, self._extension_name)

    @property
    def crs(self):
        return self._crs

    def __arrow_ext_serialize__(self):
        return b"CREATED"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        # TODO ignore serialized metadata for now
        # serialized = serialized.decode()
        # assert serialized.startswith("crs=")
        # crs = serialized.split('=')[1]
        # if crs == "":
        #     crs = None
        return cls()


def coord_storage_type(*, interleaved: bool, dims: CoordinateDimension) -> pa.DataType:
    """Generate the storage type of a geoarrow coordinate array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
    """
    if interleaved:
        return pa.list_(pa.field(dims, pa.float64()), len(dims))

    else:
        if dims == CoordinateDimension.XY:
            return pa.struct(
                [
                    ("x", pa.float64()),
                    ("y", pa.float64()),
                ]
            )
        if dims == CoordinateDimension.XYZ:
            return pa.struct(
                [
                    ("x", pa.float64()),
                    ("y", pa.float64()),
                    ("z", pa.float64()),
                ]
            )
        if dims == CoordinateDimension.XYM:
            return pa.struct(
                [
                    ("x", pa.float64()),
                    ("y", pa.float64()),
                    ("m", pa.float64()),
                ]
            )
        if dims == CoordinateDimension.XYZM:
            return pa.struct(
                [
                    ("x", pa.float64()),
                    ("y", pa.float64()),
                    ("z", pa.float64()),
                    ("m", pa.float64()),
                ]
            )


def linestring_storage_type(
    *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
) -> pa.DataType:
    """Generate the storage type of a geoarrow.linestring array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
        large_list: Whether to use a large list with int64 offsets for the inner type
    """
    vertices_type = coord_storage_type(interleaved=interleaved, dims=dims)
    if large_list:
        return pa.large_list(pa.field("vertices", vertices_type))
    else:
        return pa.list_(pa.field("vertices", vertices_type))


def polygon_storage_type(
    *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
) -> pa.DataType:
    """Generate the storage type of a geoarrow.polygon array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
        large_list: Whether to use a large list with int64 offsets for the inner type
    """
    rings_type = linestring_storage_type(
        large_list=large_list, interleaved=interleaved, dims=dims
    )
    if large_list:
        return pa.large_list(pa.field("rings", rings_type))
    else:
        return pa.list_(pa.field("rings", rings_type))


def multipoint_storage_type(
    *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
) -> pa.DataType:
    """Generate the storage type of a geoarrow.multipoint array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
        large_list: Whether to use a large list with int64 offsets for the inner type
    """
    points_type = coord_storage_type(interleaved=interleaved, dims=dims)
    if large_list:
        return pa.large_list(pa.field("points", points_type))
    else:
        return pa.list_(pa.field("points", points_type))


def multilinestring_storage_type(
    *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
) -> pa.DataType:
    """Generate the storage type of a geoarrow.multilinestring array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
        large_list: Whether to use a large list with int64 offsets for the inner type
    """
    linestrings_type = linestring_storage_type(
        large_list=large_list, interleaved=interleaved, dims=dims
    )
    if large_list:
        return pa.large_list(pa.field("linestrings", linestrings_type))
    else:
        return pa.list_(pa.field("linestrings", linestrings_type))


def multipolygon_storage_type(
    *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
) -> pa.DataType:
    """Generate the storage type of a geoarrow.multipolygon array

    Args:
        interleaved: Whether coordinates should be interleaved or separated
        dims: The number of dimensions
        large_list: Whether to use a large list with int64 offsets for the inner type
    """
    polygons_type = polygon_storage_type(
        large_list=large_list, interleaved=interleaved, dims=dims
    )
    if large_list:
        return pa.large_list(pa.field("polygons", polygons_type))
    else:
        return pa.list_(pa.field("polygons", polygons_type))


class PointType(BaseGeometryType):
    extension_name = "geoarrow.point"

    def __init__(self, *, interleaved: bool, dims: CoordinateDimension):
        self.coord_dimension = dims

        storage_type = coord_storage_type(interleaved=interleaved, dims=dims)
        super().__init__(storage_type, self.extension_name)

    # def __init__(self):
    #     # attributes need to be set first before calling
    #     # super init (as that calls serialize)
    #     # self._crs = crs
    #     pa.ExtensionType.__init__(self, self._storage_type, self._extension_name)

    def __arrow_ext_class__(self):
        return PointArray


class LineStringType(BaseGeometryType):
    extension_name = "geoarrow.linestring"

    def __init__(
        self, *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
    ):
        self.coord_dimension = dims

        storage_type = linestring_storage_type(
            interleaved=interleaved, dims=dims, large_list=large_list
        )
        super().__init__(storage_type, self.extension_name)


class PolygonType(BaseGeometryType):
    extension_name = "geoarrow.polygon"

    def __init__(
        self, *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
    ):
        self.coord_dimension = dims

        storage_type = polygon_storage_type(
            interleaved=interleaved, dims=dims, large_list=large_list
        )
        super().__init__(storage_type, self.extension_name)


class MultiPointType(BaseGeometryType):
    extension_name = "geoarrow.multipoint"

    def __init__(
        self, *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
    ):
        self.coord_dimension = dims

        storage_type = multipoint_storage_type(
            interleaved=interleaved, dims=dims, large_list=large_list
        )
        super().__init__(storage_type, self.extension_name)


class MultiLineStringType(BaseGeometryType):
    extension_name = "geoarrow.multilinestring"

    def __init__(
        self, *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
    ):
        self.coord_dimension = dims

        storage_type = multilinestring_storage_type(
            interleaved=interleaved, dims=dims, large_list=large_list
        )
        super().__init__(storage_type, self.extension_name)


class MultiPolygonType(BaseGeometryType):
    extension_name = "geoarrow.multipolygon"

    def __init__(
        self, *, interleaved: bool, dims: CoordinateDimension, large_list: bool = False
    ):
        self.coord_dimension = dims

        storage_type = multipolygon_storage_type(
            interleaved=interleaved, dims=dims, large_list=large_list
        )
        super().__init__(storage_type, self.extension_name)


def register_geometry_extension_types():
    for geom_type_class in [
        PointType,
        LineStringType,
        PolygonType,
        MultiPointType,
        MultiLineStringType,
        MultiPolygonType,
    ]:
        # Provide a default to go into the registry, but at runtime, we can choose other
        # type formulations
        geom_type_instance = geom_type_class(
            interleaved=True, dims=CoordinateDimension.XY
        )
        try:
            pa.register_extension_type(geom_type_instance)

        # If already registered with this id, unregister and re register
        except pa.ArrowKeyError:
            pa.unregister_extension_type(geom_type_instance.extension_name)
            pa.register_extension_type(geom_type_instance)


# register_geometry_extension_types()
# shapely_arr = shapely.points([[1, 2], [3, 4], [5, 6], [5, 6]])
# point_arr = construct_geometry_array(shapely_arr)
# point_arr.to_shapely()

# x = point_arr.storage.flatten()
# x.to_numpy?
# dir(x)

# point_arr.to_shapely()
# point_arr.type.coord_dimension


def construct_geometry_array(
    shapely_arr: NDArray[np.object_], include_z: Optional[bool] = None
):
    geom_type, coords, offsets = shapely.to_ragged_array(
        shapely_arr, include_z=include_z
    )

    if coords.shape[-1] == 2:
        dims = CoordinateDimension.XY
    elif coords.shape[-1] == 3:
        dims = CoordinateDimension.XYZ
    else:
        raise ValueError(f"Unexpected coords dimensions: {coords.shape}")

    if geom_type == GeometryType.POINT:
        parr = pa.FixedSizeListArray.from_arrays(coords.flatten(), len(dims))
        return pa.ExtensionArray.from_storage(
            PointType(interleaved=True, dims=dims), parr
        )

    elif geom_type == GeometryType.LINESTRING:
        assert len(offsets) == 1, "Expected one offsets array"
        (offsets1,) = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        parr = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        return pa.ExtensionArray.from_storage(
            LineStringType(interleaved=True, dims=dims), parr
        )

    elif geom_type == GeometryType.POLYGON:
        assert len(offsets) == 2, "Expected two offsets arrays"
        offsets1, offsets2 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        parr = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        return pa.ExtensionArray.from_storage(
            PolygonType(interleaved=True, dims=dims), parr
        )

    elif geom_type == GeometryType.MULTIPOINT:
        assert len(offsets) == 1, "Expected one offsets array"
        (offsets1,) = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        parr = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        return pa.ExtensionArray.from_storage(
            MultiPointType(interleaved=True, dims=dims), parr
        )

    elif geom_type == GeometryType.MULTILINESTRING:
        assert len(offsets) == 2, "Expected two offsets arrays"
        offsets1, offsets2 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        parr = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        return pa.ExtensionArray.from_storage(
            MultiLineStringType(interleaved=True, dims=dims), parr
        )

    elif geom_type == GeometryType.MULTIPOLYGON:
        assert len(offsets) == 3, "Expected three offsets arrays"
        offsets1, offsets2, offsets3 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        _parr2 = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        parr = pa.ListArray.from_arrays(pa.array(offsets3), _parr2)
        return pa.ExtensionArray.from_storage(
            MultiPolygonType(interleaved=True, dims=dims), parr
        )

    else:
        raise ValueError(f"Unsupported type for geoarrow: {geom_type}")
