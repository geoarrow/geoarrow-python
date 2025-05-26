import sys
import re
from math import inf

import pyarrow as pa
import numpy as np
import pytest

import geoarrow.c.lib as lib
from geoarrow import types
import geoarrow.pyarrow as ga
import geoarrow.pyarrow._type as _type
import geoarrow.pyarrow._array as _array


def test_version():
    assert re.match(r"^[0-9]+\.[0-9]+", ga.__version__)


def test_geometry_type_basic():
    pa_type = _type.point()

    assert pa_type.geometry_type == ga.GeometryType.POINT
    assert pa_type.dimensions == ga.Dimensions.XY
    assert pa_type.coord_type == ga.CoordType.SEPARATED

    expected_storage = pa.struct(
        [
            pa.field("x", pa.float64(), nullable=False),
            pa.field("y", pa.float64(), nullable=False),
        ]
    )
    assert pa_type.storage_type == expected_storage


def test_geometry_type_with():
    type_obj = _type.point()

    type_linestring = type_obj.with_geometry_type(ga.GeometryType.LINESTRING)
    assert type_linestring.geometry_type == ga.GeometryType.LINESTRING

    type_xyz = type_obj.with_dimensions(ga.Dimensions.XYZ)
    assert type_xyz.dimensions == ga.Dimensions.XYZ

    type_interleaved = type_obj.with_coord_type(ga.CoordType.INTERLEAVED)
    assert type_interleaved.coord_type == ga.CoordType.INTERLEAVED

    type_spherical = type_obj.with_edge_type(ga.EdgeType.SPHERICAL)
    assert type_spherical.edge_type == ga.EdgeType.SPHERICAL

    type_crs = type_obj.with_crs(types.OGC_CRS84)
    assert type_crs.crs == types.OGC_CRS84

    type_crs = type_obj.with_crs("OGC:CRS84")
    assert repr(type_crs.crs) == "StringCrs(OGC:CRS84)"


def test_type_with_crs_pyproj():
    pyproj = pytest.importorskip("pyproj")
    type_obj = ga.wkb()

    type_crs = type_obj.with_crs(pyproj.CRS("EPSG:32620"))
    assert isinstance(type_crs.crs, pyproj.CRS)
    crs_dict = type_crs.crs.to_json_dict()
    assert crs_dict["id"]["code"] == 32620


def test_constructors():
    assert ga.wkb().extension_name == "geoarrow.wkb"
    assert ga.large_wkb().extension_name == "geoarrow.wkb"
    assert ga.wkb_view().extension_name == "geoarrow.wkb"
    assert ga.wkt().extension_name == "geoarrow.wkt"
    assert ga.large_wkt().extension_name == "geoarrow.wkt"
    assert ga.wkt_view().extension_name == "geoarrow.wkt"
    assert ga.point().extension_name == "geoarrow.point"
    assert ga.linestring().extension_name == "geoarrow.linestring"
    assert ga.polygon().extension_name == "geoarrow.polygon"
    assert ga.multipoint().extension_name == "geoarrow.multipoint"
    assert ga.multilinestring().extension_name == "geoarrow.multilinestring"
    assert ga.multipolygon().extension_name == "geoarrow.multipolygon"

    generic = ga.extension_type(
        types.type_spec(
            ga.Encoding.GEOARROW,
            ga.GeometryType.POINT,
            ga.Dimensions.XYZ,
            ga.CoordType.INTERLEAVED,
            ga.EdgeType.SPHERICAL,
            crs=types.OGC_CRS84,
        )
    )
    assert generic.geometry_type == ga.GeometryType.POINT
    assert generic.dimensions == ga.Dimensions.XYZ
    assert generic.coord_type == ga.CoordType.INTERLEAVED
    assert generic.edge_type == ga.EdgeType.SPHERICAL
    assert generic.crs == types.OGC_CRS84


def test_type_common():
    assert ga.geometry_type_common([]) == ga.wkb()
    assert ga.geometry_type_common([ga.wkt()]) == ga.wkt()
    assert ga.geometry_type_common([ga.point(), ga.point()]) == ga.point()
    assert ga.geometry_type_common([ga.point(), ga.linestring()]) == ga.wkb()


def test_array():
    array = ga.array(["POINT (30 10)"])
    assert array.type == ga.wkt()
    assert isinstance(array[0], ga._scalar.WktScalar)

    wkb_item = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40\x00\x00\x00\x00\x00\x00\x24\x40"
    array = ga.array([wkb_item])
    assert array.type == ga.wkb()
    assert isinstance(array[0], ga._scalar.WkbScalar)

    with pytest.raises(TypeError):
        ga.array([1])

    array = ga.array(["POINT (30 10)"], ga.wkt())
    assert array.type == ga.wkt()
    assert array.type.storage_type == pa.utf8()

    array = ga.array(["POINT (30 10)"], ga.large_wkt())
    assert array.type == ga.large_wkt()
    assert array.type.storage_type == pa.large_utf8()

    array = ga.array([wkb_item], ga.wkb())
    assert array.type == ga.wkb()
    assert array.type.storage_type == pa.binary()

    array = ga.array([wkb_item], ga.large_wkb())
    assert array.type == ga.large_wkb()
    assert array.type.storage_type == pa.large_binary()


def test_array_view_types():
    # This one requires pyarrow >= 18, because that's when the necessary
    # cast() was added.
    try:
        pa.array(["foofy"]).cast(pa.string_view())
    except pa.lib.ArrowNotImplementedError:
        pytest.skip("ga.array() with view types requires pyarrow >= 18.0.0")

    array = ga.array(["POINT (30 10)"], ga.wkt_view())
    assert array.type == ga.wkt_view()
    assert array.type.storage_type == pa.string_view()

    wkb_item = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40\x00\x00\x00\x00\x00\x00\x24\x40"
    array = ga.array([wkb_item], ga.wkb_view())
    assert array.type == ga.wkb_view()
    assert array.type.storage_type == pa.binary_view()


def test_array_repr():
    array = ga.array(["POINT (30 10)"])
    array_repr = repr(array)
    assert array_repr.startswith("GeometryExtensionArray")
    assert "<POINT (30 10)>" in array_repr

    array = ga.array(["POINT (30 10)"] * 12)
    array_repr = repr(array)
    assert "...2 values..." in array_repr

    array = ga.array(
        ["LINESTRING (100000 100000, 100000 100000, 100000 100000, 100000 100000)"]
    )
    array_repr = repr(array)
    assert "...>" in array_repr

    array = ga.array(["THIS IS TOTALLY INVALID WKT"])
    array_repr = repr(array)
    assert array_repr.startswith("GeometryExtensionArray")
    assert "* 1 or more display values failed to parse" in array_repr


def test_scalar_wkt():
    array = ga.array(["POINT (0 1)"])
    assert array[0].wkt == "POINT (0 1)"
    assert array[0].wkb == ga.as_wkb(array).storage[0].as_py()
    assert repr(array[0]).startswith("WktScalar")


def test_scalar_wkb():
    array = ga.as_wkb(["POINT (0 1)"])
    assert array[0].wkt == "POINT (0 1)"
    assert array[0].wkb == ga.as_wkb(array).storage[0].as_py()
    assert repr(array[0]).startswith("WkbScalar")


def test_scalar_geoarrow():
    array = ga.as_geoarrow(["POINT (0 1)"])
    assert array[0].wkt == "POINT (0 1)"
    assert array[0].wkb == ga.as_wkb(array).storage[0].as_py()
    assert repr(array[0]).startswith("GeometryExtensionScalar")


def test_scalar_box():
    # The box kernel doesn't yet implement non XY boxes
    array = ga.box(["LINESTRING ZM (0 1 2 3, 4 5 6 7)"])
    assert array[0].xmin == 0
    assert array[0].ymin == 1
    assert array[0].zmin is None
    assert array[0].mmin is None
    assert array[0].xmax == 4
    assert array[0].ymax == 5
    assert array[0].zmax is None
    assert array[0].mmax is None
    assert repr(array[0]).startswith("BoxScalar")


def test_scalar_repr():
    array = ga.array(
        ["LINESTRING (100000 100000, 100000 100000, 100000 100000, 100000 100000)"]
    )
    assert repr(array[0]).endswith("...>")

    array = ga.array(["TOTALLY INVALID WKT"])
    assert "value failed to parse" in repr(array[0])


def test_kernel_void():
    with pytest.raises(TypeError):
        kernel = ga.Kernel.void(pa.int32())
        kernel.push(5)

    array = ga.array(["POINT (30 10)"])
    kernel = ga.Kernel.void(array.type)
    out = kernel.push(array)
    assert out.type == pa.null()
    assert len(out) == 1

    array = ga.array(["POINT (30 10)", "POINT (31 11)"])
    kernel = ga.Kernel.void_agg(array.type)
    assert kernel.push(array) is None
    out = kernel.finish()
    assert out.type == pa.null()
    assert len(out) == 1


def test_kernel_as():
    array = ga.array(["POINT (30 10)"], ga.wkt().with_crs(types.OGC_CRS84))
    kernel = ga.Kernel.as_wkt(array.type)
    out = kernel.push(array)
    assert out.type.extension_name == "geoarrow.wkt"
    assert out.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()
    assert isinstance(out, _array.GeometryExtensionArray)

    array = ga.array(["POINT (30 10)"], ga.wkt().with_crs(types.OGC_CRS84))
    kernel = ga.Kernel.as_wkb(array.type)
    out = kernel.push(array)
    assert out.type.extension_name == "geoarrow.wkb"
    assert out.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()
    assert isinstance(out, _array.GeometryExtensionArray)

    if sys.byteorder == "little":
        wkb_item = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40\x00\x00\x00\x00\x00\x00\x24\x40"
        assert out[0].as_py() == wkb_item

    array = ga.array(["POINT (30 10)"], ga.wkt().with_crs(types.OGC_CRS84))
    kernel = ga.Kernel.as_geoarrow(array.type, 1)
    out = kernel.push(array)
    assert out.type.extension_name == "geoarrow.point"
    assert out.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()
    assert isinstance(out, _array.GeometryExtensionArray)


def test_kernel_format():
    array = ga.array(["POINT (30.12345 10.12345)"])
    kernel = ga.Kernel.format_wkt(array.type, precision=3, max_element_size_bytes=15)

    out = kernel.push(array)
    assert out.type == pa.string()
    assert out[0].as_py() == "POINT (30.123 1"


def test_kernel_unique_geometry_types():
    array = ga.array(["POINT (0 1)", "POINT (30 10)", "LINESTRING Z (0 1 2, 3 4 5)"])
    kernel = ga.Kernel.unique_geometry_types_agg(array.type)
    kernel.push(array)
    out = kernel.finish()

    assert out.type == pa.int32()
    out_py = [item.as_py() for item in out]
    assert out_py == [1, 1002]


def test_kernel_box():
    array = ga.array(["POINT (0 1)", "POINT (30 10)", "LINESTRING EMPTY"])
    kernel = ga.Kernel.box(array.type)
    out = kernel.push(array)

    assert out[0].as_py() == {"xmin": 0, "xmax": 0, "ymin": 1, "ymax": 1}
    assert out[1].as_py() == {"xmin": 30, "xmax": 30, "ymin": 10, "ymax": 10}
    assert out[2].as_py() == {"xmin": inf, "xmax": -inf, "ymin": inf, "ymax": -inf}


def test_kernel_box_agg():
    array = ga.array(["POINT (0 1)", "POINT (30 10)", "LINESTRING EMPTY"])
    kernel = ga.Kernel.box_agg(array.type)
    kernel.push(array)
    out = kernel.finish()

    assert out[0].as_py() == {"xmin": 0, "xmax": 30, "ymin": 1, "ymax": 10}


def test_kernel_visit_void():
    array = ga.array(["POINT (30 10)"], ga.wkt())
    kernel = ga.Kernel.visit_void_agg(array.type)
    assert kernel.push(array) is None
    out = kernel.finish()
    assert out.type == pa.null()
    assert len(out) == 1

    array = ga.array(["POINT (30 10)", "NOT VALID WKT AT ALL"], ga.wkt())
    kernel = ga.Kernel.visit_void_agg(array.type)
    with pytest.raises(lib.GeoArrowCException):
        kernel.push(array)
    out = kernel.finish()
    assert out.type == pa.null()
    assert len(out) == 1


def test_array_geobuffers():
    arr = ga.as_geoarrow(["POLYGON ((0 0, 1 0, 0 1, 0 0))"])
    bufs = arr.geobuffers()
    assert bufs[0] is None
    np.testing.assert_array_equal(bufs[1], np.array([0, 1]))
    np.testing.assert_array_equal(bufs[2], np.array([0, 4]))
    np.testing.assert_array_equal(bufs[3], np.array([0.0, 1.0, 0.0, 0.0]))
    np.testing.assert_array_equal(bufs[4], np.array([0.0, 0.0, 1.0, 0.0]))


def test_point_array_from_geobuffers():
    arr = ga.point().from_geobuffers(
        b"\xff",
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0, 6.0]),
    )
    assert len(arr) == 3
    assert ga.as_wkt(arr)[2].as_py() == "POINT (3 6)"

    arr = (
        ga.point()
        .with_coord_type(ga.CoordType.INTERLEAVED)
        .from_geobuffers(None, np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]))
    )
    assert len(arr) == 3
    assert ga.as_wkt(arr)[2].as_py() == "POINT (5 6)"


def test_linestring_array_from_geobuffers():
    arr = ga.linestring().from_geobuffers(
        None,
        np.array([0, 3], dtype=np.int32),
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0, 6.0]),
    )
    assert len(arr) == 1
    assert ga.as_wkt(arr)[0].as_py() == "LINESTRING (1 4, 2 5, 3 6)"


def test_polygon_array_from_geobuffers():
    arr = ga.polygon().from_geobuffers(
        None,
        np.array([0, 1], dtype=np.int32),
        np.array([0, 4], dtype=np.int32),
        np.array([1.0, 2.0, 3.0, 1.0]),
        np.array([4.0, 5.0, 6.0, 4.0]),
    )
    assert len(arr) == 1
    assert ga.as_wkt(arr)[0].as_py() == "POLYGON ((1 4, 2 5, 3 6, 1 4))"


def test_multipoint_array_from_geobuffers():
    arr = ga.multipoint().from_geobuffers(
        None,
        np.array([0, 3], dtype=np.int32),
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0, 6.0]),
    )
    assert len(arr) == 1
    assert ga.as_wkt(arr)[0].as_py() == "MULTIPOINT (1 4, 2 5, 3 6)"


def test_multilinestring_array_from_geobuffers():
    arr = ga.multilinestring().from_geobuffers(
        None,
        np.array([0, 1], dtype=np.int32),
        np.array([0, 4], dtype=np.int32),
        np.array([1.0, 2.0, 3.0, 1.0]),
        np.array([4.0, 5.0, 6.0, 4.0]),
    )
    assert len(arr) == 1
    assert ga.as_wkt(arr)[0].as_py() == "MULTILINESTRING ((1 4, 2 5, 3 6, 1 4))"


def test_multipolygon_array_from_geobuffers():
    arr = ga.multipolygon().from_geobuffers(
        None,
        np.array([0, 1], dtype=np.int32),
        np.array([0, 1], dtype=np.int32),
        np.array([0, 4], dtype=np.int32),
        np.array([1.0, 2.0, 3.0, 1.0]),
        np.array([4.0, 5.0, 6.0, 4.0]),
    )
    assert len(arr) == 1
    assert ga.as_wkt(arr)[0].as_py() == "MULTIPOLYGON (((1 4, 2 5, 3 6, 1 4)))"


def test_box_array_from_geobuffers():
    arr = (
        types.box()
        .to_pyarrow()
        .from_geobuffers(
            b"\xff",
            np.array([1.0, 2.0, 3.0]),
            np.array([4.0, 5.0, 6.0]),
            np.array([7.0, 8.0, 9.0]),
            np.array([10.0, 11.0, 12.0]),
        )
    )
    assert len(arr) == 3
    assert arr[2].bounds == {"xmin": 3.0, "ymin": 6.0, "xmax": 9.0, "ymax": 12.0}
    assert "BoxArray" in repr(arr)
    assert "'xmin': 3.0" in repr(arr)


# Easier to test here because we have actual geoarrow arrays to parse
def test_c_array_view():
    arr = ga.as_geoarrow(["POLYGON ((0 0, 1 0, 0 1, 0 0))"])

    cschema = lib.SchemaHolder()
    arr.type._export_to_c(cschema._addr())
    carray = lib.ArrayHolder()
    arr._export_to_c(carray._addr())

    array_view = lib.CArrayView(carray, cschema)
    buffers = array_view.buffers()
    assert len(buffers) == 5

    buffer_arrays = [np.array(b) for b in buffers]

    assert buffers[0] is None

    assert buffer_arrays[1].shape == (2,)
    assert buffer_arrays[1][0] == 0
    assert buffer_arrays[1][1] == 1

    assert buffer_arrays[2].shape == (2,)
    assert buffer_arrays[2][0] == 0
    assert buffer_arrays[2][1] == 4

    assert buffer_arrays[3].shape == (4,)
    assert buffer_arrays[3][1] == 1
    assert buffer_arrays[3][3] == 0

    assert buffer_arrays[4].shape == (4,)
    assert buffer_arrays[4][1] == 0
    assert buffer_arrays[4][3] == 0


def test_c_array_view_interleaved():
    arr = ga.array(["POLYGON ((0 0, 1 0, 0 1, 0 0))"])
    arr = ga.as_geoarrow(arr, ga.polygon().with_coord_type(ga.CoordType.INTERLEAVED))

    cschema = lib.SchemaHolder()
    arr.type._export_to_c(cschema._addr())
    carray = lib.ArrayHolder()
    arr._export_to_c(carray._addr())

    array_view = lib.CArrayView(carray, cschema)
    buffers = array_view.buffers()
    assert len(buffers) == 4

    buffer_arrays = [np.array(b) for b in buffers]

    assert buffers[0] is None

    assert buffer_arrays[1].shape == (2,)
    assert buffer_arrays[1][0] == 0
    assert buffer_arrays[1][1] == 1

    assert buffer_arrays[2].shape == (2,)
    assert buffer_arrays[2][0] == 0
    assert buffer_arrays[2][1] == 4

    assert buffer_arrays[3].shape == (8,)
    assert buffer_arrays[3][0] == 0
    assert buffer_arrays[3][1] == 0
    assert buffer_arrays[3][2] == 1
    assert buffer_arrays[3][3] == 0
    assert buffer_arrays[3][6] == 0
    assert buffer_arrays[3][7] == 0
