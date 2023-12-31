import pytest
import pandas as pd


pytest.importorskip("geopandas")


def test_scalar_to_shapely():
    series = pd.Series(["POINT (0 1)", "POINT (1 2)"])
    extension_series = series.geoarrow.as_geoarrow()
    assert extension_series[0].to_shapely().wkt == "POINT (0 1)"


def test_accessor_to_geopandas():
    series = pd.Series(["POINT (0 1)", "POINT (1 2)"])
    geoseries = series.geoarrow.to_geopandas()
    assert len(geoseries) == 2
    assert geoseries[0].wkt == "POINT (0 1)"
