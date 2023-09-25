import pytest

geopandas = pytest.importorskip("geopandas")

import geoarrow.pyarrow as ga


def test_scalar_to_shapely():
    array = ga.array(["POINT (30 10)"])
    assert array[0].to_shapely().wkt == "POINT (30 10)"

    wkb_item = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40\x00\x00\x00\x00\x00\x00\x24\x40"
    array = ga.array([wkb_item])
    assert array[0].to_shapely().wkt == "POINT (30 10)"


def test_to_geopandas():
    array = ga.array(["POINT (30 10)"])
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"


def test_to_geopandas_with_crs():
    array = ga.with_crs(ga.array(["POINT (30 10)"]), "OGC:CRS84")
    geoseries = ga.to_geopandas(array)
    assert isinstance(geoseries, geopandas.GeoSeries)
    assert len(geoseries) == 1
    assert geoseries.to_wkt()[0] == "POINT (30 10)"
    assert geoseries.crs.to_authority() == ("OGC", "CRS84")
