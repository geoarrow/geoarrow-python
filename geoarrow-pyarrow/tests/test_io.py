import pytest
import tempfile
import os

import pyarrow as pa
import geoarrow.pyarrow as ga
from geoarrow.pyarrow import io


def test_readpyogrio_table():
    pyogrio = pytest.importorskip("pyogrio")
    geopandas = pytest.importorskip("geopandas")

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_gpkg = os.path.join(tmpdir, "test.gpkg")
        df = geopandas.GeoDataFrame(
            geometry=geopandas.GeoSeries.from_wkt(["POINT (0 1)"], crs="OGC:CRS84")
        )
        crs_json = df.geometry.crs.to_json()
        pyogrio.write_dataframe(df, temp_gpkg)

        table = io.read_pyogrio_table(temp_gpkg)
        assert table.column("geom").type == ga.wkb().with_crs(crs_json)
        assert ga.format_wkt(table.column("geom")).to_pylist() == ["POINT (0 1)"]


def test_geoparquet_column_spec_from_type_geom_type():
    spec_wkb = io._geoparquet_column_spec_from_type(ga.wkb())
    assert spec_wkb["geometry_types"] == []

    spec_point = io._geoparquet_column_spec_from_type(ga.point())
    assert spec_point["geometry_types"] == ["Point"]

    spec_linestring = io._geoparquet_column_spec_from_type(ga.linestring())
    assert spec_linestring["geometry_types"] == ["LineString"]

    spec_polygon = io._geoparquet_column_spec_from_type(ga.polygon())
    assert spec_polygon["geometry_types"] == ["Polygon"]

    spec_multipoint = io._geoparquet_column_spec_from_type(ga.multipoint())
    assert spec_multipoint["geometry_types"] == ["MultiPoint"]

    spec_multilinestring = io._geoparquet_column_spec_from_type(ga.multilinestring())
    assert spec_multilinestring["geometry_types"] == ["MultiLineString"]

    spec_multipolygon = io._geoparquet_column_spec_from_type(ga.multipolygon())
    assert spec_multipolygon["geometry_types"] == ["MultiPolygon"]


def test_geoparquet_column_spec_from_type_crs():
    spec_storage = io._geoparquet_column_spec_from_type(pa.binary())
    assert "crs" not in spec_storage

    spec_none = io._geoparquet_column_spec_from_type(ga.wkb())
    assert spec_none["crs"] is None

    spec_projjson = io._geoparquet_column_spec_from_type(
        ga.wkb().with_crs("{}", ga.CrsType.PROJJSON)
    )
    assert spec_projjson["crs"] == {}

    pytest.importorskip("pyproj")
    spec_not_projjson = io._geoparquet_column_spec_from_type(
        ga.wkb().with_crs("OGC:CRS84")
    )
    assert spec_not_projjson["crs"]["id"]["code"] == "CRS84"


def test_geoparquet_column_spec_from_type_edges():
    spec_planar = io._geoparquet_column_spec_from_type(ga.wkb())
    assert "edges" not in spec_planar

    spec_spherical = io._geoparquet_column_spec_from_type(
        ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
    )
    assert spec_spherical["edges"] == "spherical"


def test_read_geoparquet_table():
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_pq = os.path.join(tmpdir, "test.parquet")
