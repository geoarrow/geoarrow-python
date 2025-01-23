import pytest
import pyarrow as pa
from geoarrow import types
import geoarrow.pyarrow as ga


geopandas = pytest.importorskip("geopandas")


def test_from_geopandas():
    geoseries = geopandas.GeoSeries.from_wkt(["POINT (30 10)"]).set_crs("OGC:CRS84")
    array = ga.array(geoseries)
    assert isinstance(array.type, ga.WkbType)
    assert "CRS84" in repr(array.type.crs)
    assert ga.format_wkt(array)[0].as_py() == "POINT (30 10)"


def test_scalar_to_shapely():
    array = ga.array(["POINT (30 10)"])
    assert array[0].to_shapely().wkt == "POINT (30 10)"

    wkb_item = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40\x00\x00\x00\x00\x00\x00\x24\x40"
    array = ga.array([wkb_item])
    assert array[0].to_shapely().wkt == "POINT (30 10)"


def test_to_geopandas_unsupported_type():
    # GeoPandas doesn't support geoarrow.wkt, so this goes through the branch
    # that handles any GeoPandas failure
    array = ga.as_wkt(["POINT (30 10)"])
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"


def test_to_geopandas_using_geopandas():
    array = ga.as_wkb(["POINT (30 10)"])
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"


def test_to_geopandas_with_crs():
    array = ga.with_crs(ga.as_wkt(["POINT (30 10)"]), types.OGC_CRS84)
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"
    assert geoseries.crs.to_authority() == ("OGC", "CRS84")


def test_to_geopandas_with_crs_using_geopandas():
    array = ga.with_crs(ga.as_wkb(["POINT (30 10)"]), types.OGC_CRS84)
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"
    assert geoseries.crs.to_authority() == ("OGC", "CRS84")


def test_table_to_geopandas_unsupported_type():
    # GeoPandas doesn't support geoarrow.wkt, so this goes through the branch
    # that handles any GeoPandas failure
    table = pa.table({"geom": ga.as_wkt(["POINT (30 10)"])})
    gdf = ga.to_geopandas(table)
    assert isinstance(gdf, geopandas.GeoDataFrame)

    geoseries = gdf.geometry
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"


def test_table_to_geopandas_using_geopandas():
    table = pa.table({"geom": ga.as_wkb(["POINT (30 10)"])})
    gdf = ga.to_geopandas(table)
    assert isinstance(gdf, geopandas.GeoDataFrame)

    geoseries = gdf.geometry
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"
