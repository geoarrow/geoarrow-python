import pytest

import pandas as pd
import pyarrow as pa
import geoarrow.pandas as gapd
import geoarrow.pyarrow as ga
from geoarrow.c import lib
import numpy as np


def test_dtype_constructor():
    from_pyarrow = gapd.GeoArrowExtensionDtype(ga.point())
    assert from_pyarrow.name == "geoarrow.point"

    from_ctype = gapd.GeoArrowExtensionDtype(ga.point()._type)
    assert from_ctype.name == "geoarrow.point"

    from_dtype = gapd.GeoArrowExtensionDtype(from_ctype)
    assert from_dtype.name == "geoarrow.point"

    with pytest.raises(TypeError):
        gapd.GeoArrowExtensionDtype(b"1234")


def test_dtype_strings():
    dtype = gapd.GeoArrowExtensionDtype(ga.point())
    assert str(dtype) == "geoarrow.point"
    dtype2 = gapd.GeoArrowExtensionDtype.construct_from_string(str(dtype))
    assert dtype2 == dtype

    dtype = gapd.GeoArrowExtensionDtype(ga.point().with_crs("EPSG:1234"))
    assert str(dtype) == 'geoarrow.point{"crs":"EPSG:1234"}'
    dtype2 = gapd.GeoArrowExtensionDtype.construct_from_string(str(dtype))
    assert dtype2 == dtype

    dtype = gapd.GeoArrowExtensionDtype(
        ga.point().with_coord_type(ga.CoordType.INTERLEAVED)
    )
    assert str(dtype) == "geoarrow.point[interleaved]"
    dtype2 = gapd.GeoArrowExtensionDtype.construct_from_string(str(dtype))
    assert dtype2 == dtype

    dtype = gapd.GeoArrowExtensionDtype(ga.point().with_dimensions(ga.Dimensions.XYZ))
    assert str(dtype) == "geoarrow.point[z]"
    dtype2 = gapd.GeoArrowExtensionDtype.construct_from_string(str(dtype))
    assert dtype2 == dtype


def test_scalar():
    scalar_from_wkt = gapd.GeoArrowExtensionScalar("POINT (0 1)")
    assert scalar_from_wkt.wkt == "POINT (0 1)"
    assert isinstance(scalar_from_wkt.wkb, bytes)
    assert str(scalar_from_wkt) == "POINT (0 1)"
    assert repr(scalar_from_wkt) == 'GeoArrowExtensionScalar("POINT (0 1)")'

    scalar_from_wkb = gapd.GeoArrowExtensionScalar(scalar_from_wkt.wkb)
    assert scalar_from_wkb == scalar_from_wkt

    scalar_from_scalar = gapd.GeoArrowExtensionScalar(scalar_from_wkt)
    assert scalar_from_scalar == scalar_from_wkt

    array = ga.as_geoarrow(["POINT (0 1)", "POINT (1 2)"])
    scalar_from_array0 = gapd.GeoArrowExtensionScalar(array, 0)
    assert scalar_from_array0 == scalar_from_wkt

    scalar_from_array1 = gapd.GeoArrowExtensionScalar(array, 1)
    assert scalar_from_array1 == gapd.GeoArrowExtensionScalar("POINT (1 2)")


def test_array_init_without_type():
    array = gapd.GeoArrowExtensionArray(["POINT (0 1)"])
    assert array._data == ga.array(["POINT (0 1)"])
    assert array._dtype._parent.extension_name == "geoarrow.wkt"


def test_array_init_with_type():
    array = gapd.GeoArrowExtensionArray(["POINT (0 1)"], ga.wkt())
    assert array._data == ga.array(["POINT (0 1)"], ga.wkt())
    assert array._dtype._parent.extension_name == "geoarrow.wkt"


def test_array_basic_methods():
    pa_array = ga.array(["POINT (0 1)", "POINT (1 2)", None])
    array = gapd.GeoArrowExtensionArray(pa_array)

    assert array[0] == gapd.GeoArrowExtensionScalar("POINT (0 1)")
    assert array[2] is None
    assert isinstance(array[1:2], gapd.GeoArrowExtensionArray)
    assert len(array[1:2]) == 1
    assert array[1:2][0] == gapd.GeoArrowExtensionScalar("POINT (1 2)")
    assert isinstance(array[[1]], gapd.GeoArrowExtensionArray)
    assert array[[1]][0] == gapd.GeoArrowExtensionScalar("POINT (1 2)")

    assert len(array) == 3
    assert all(array[:2] == array[:2])
    assert array.dtype == gapd.GeoArrowExtensionDtype(ga.wkt())
    assert array.nbytes == pa_array.nbytes
    assert isinstance(array.take(np.array([1])), gapd.GeoArrowExtensionArray)
    assert array.take(np.array([1]))[0] == gapd.GeoArrowExtensionScalar("POINT (1 2)")
    np.testing.assert_array_equal(array.isna(), np.array([False, False, True]))

    assert isinstance(array.copy(), gapd.GeoArrowExtensionArray)
    assert array.copy()[0] == gapd.GeoArrowExtensionScalar("POINT (0 1)")

    np.testing.assert_array_equal(
        array.to_numpy(),
        np.array(
            [
                gapd.GeoArrowExtensionScalar("POINT (0 1)"),
                gapd.GeoArrowExtensionScalar("POINT (1 2)"),
                None,
            ]
        ),
    )


def test_array_concat():
    pa_array_wkt = ga.array(["POINT (0 1)", "POINT (1 2)", None])
    array_wkt = gapd.GeoArrowExtensionArray(pa_array_wkt)
    array_wkt_chunkned = gapd.GeoArrowExtensionArray(pa.chunked_array([array_wkt]))
    pa_array_geoarrow = ga.as_geoarrow(pa_array_wkt)
    array_geoarrow = gapd.GeoArrowExtensionArray(pa_array_geoarrow)

    concatenated0 = gapd.GeoArrowExtensionArray._concat_same_type([])
    assert concatenated0.dtype == gapd.GeoArrowExtensionDtype(ga.wkb())
    assert len(concatenated0) == 0

    concatenated1 = gapd.GeoArrowExtensionArray._concat_same_type([array_wkt])
    assert concatenated1 is array_wkt

    concatenated_same_type = gapd.GeoArrowExtensionArray._concat_same_type(
        [array_wkt, array_wkt_chunkned]
    )
    assert concatenated_same_type.dtype == array_wkt.dtype
    assert len(concatenated_same_type) == 6

    concatenated_diff_type = gapd.GeoArrowExtensionArray._concat_same_type(
        [array_wkt, array_geoarrow]
    )
    assert concatenated_diff_type.dtype == gapd.GeoArrowExtensionDtype(ga.wkb())
    assert len(concatenated_diff_type) == 6


def test_pyarrow_integration():
    pa_array = ga.array(["POINT (0 1)", "POINT (1 2)", None])
    series = pa_array.to_pandas()
    assert series.dtype == gapd.GeoArrowExtensionDtype(ga.wkt())
    assert series[0] == gapd.GeoArrowExtensionScalar("POINT (0 1)")
    assert pa.array(series) is pa_array

    pa_chunked_array = pa.chunked_array([pa_array])
    series = pa_chunked_array.to_pandas()
    assert series.dtype == gapd.GeoArrowExtensionDtype(ga.wkt())
    assert series[0] == gapd.GeoArrowExtensionScalar("POINT (0 1)")


def test_accessor_parse_all():
    series = pd.Series(["POINT (0 1)"])
    assert series.geoarrow.parse_all() is series
    with pytest.raises(lib.GeoArrowCException):
        pd.Series(["NOT WKT"]).geoarrow.parse_all()


def test_accessor_as_wkt():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.as_wkt()
    assert isinstance(ga_series.dtype.pyarrow_dtype, ga.WktType)


def test_accessor_as_wkb():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.as_wkb()
    assert isinstance(ga_series.dtype.pyarrow_dtype, ga.WkbType)


def test_accessor_format_wkt():
    with pytest.raises(TypeError):
        pd.Series(["POINT (0 1)"]).geoarrow.format_wkt()

    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.as_geoarrow().geoarrow.format_wkt()
    assert ga_series.dtype.pyarrow_dtype == pa.utf8()


def test_accessor_format_wkb():
    with pytest.raises(TypeError):
        pd.Series(["POINT (0 1)"]).geoarrow.format_wkb()

    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.as_geoarrow().geoarrow.format_wkb()
    assert ga_series.dtype.pyarrow_dtype == pa.binary()

    # Currently handles ChunkedArray explicitly
    chunked = pa.chunked_array([ga.array(["POINT (0 1)"])])
    ga_series = chunked.to_pandas().geoarrow.format_wkb()
    assert ga_series.dtype.pyarrow_dtype == pa.binary()


def test_accessor_as_geoarrow():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.as_geoarrow()
    assert isinstance(ga_series.dtype.pyarrow_dtype, ga.PointType)


def test_accessor_bounds():
    df = pd.Series(["POINT (0 1)"]).geoarrow.bounds()
    assert isinstance(df, pd.DataFrame)
    assert df.xmin[0] == 0
    assert df.ymin[0] == 1
    assert df.xmax[0] == 0
    assert df.ymax[0] == 1


def test_accessor_total_bounds():
    df = pd.Series(["POINT (0 1)"]).geoarrow.total_bounds()
    assert isinstance(df, pd.DataFrame)
    assert df.xmin[0] == 0
    assert df.ymin[0] == 1
    assert df.xmax[0] == 0
    assert df.ymax[0] == 1


def test_accessor_point_coords():
    series = pd.Series(["POINT (0 1)", "POINT (1 2)"])
    x, y = series.geoarrow.point_coords()
    np.testing.assert_array_equal(np.array(x), np.array([0.0, 1.0]))
    np.testing.assert_array_equal(np.array(y), np.array([1.0, 2.0]))


def test_accessor_with_coord_type():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.with_coord_type(
        ga.CoordType.INTERLEAVED
    )
    assert ga_series.dtype.pyarrow_dtype.coord_type == ga.CoordType.INTERLEAVED


def test_accessor_with_edge_type():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.with_edge_type(
        ga.EdgeType.SPHERICAL
    )
    assert ga_series.dtype.pyarrow_dtype.edge_type == ga.EdgeType.SPHERICAL


def test_accessor_with_crs():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.with_crs("EPSG:1234")
    assert ga_series.dtype.pyarrow_dtype.crs == "EPSG:1234"


def test_accessor_with_dimensions():
    ga_series = pd.Series(["POINT (0 1)"]).geoarrow.with_dimensions(ga.Dimensions.XYZ)
    assert ga_series.dtype.pyarrow_dtype.dimensions == ga.Dimensions.XYZ
