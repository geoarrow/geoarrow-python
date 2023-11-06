import pytest
import tempfile
import os

import pyarrow as pa
from pyarrow import parquet
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


def test_write_geoparquet_table():
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_pq = os.path.join(tmpdir, "test.parquet")
        tab = pa.table([ga.array(["POINT (0 1)"])], names=["geometry"])
        io.write_geoparquet_table(tab, temp_pq)
        tab2 = parquet.read_table(temp_pq)
        assert b"geo" in tab2.schema.metadata
        assert tab2.schema.types[0] == pa.binary()


def test_read_geoparquet_table():
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_pq = os.path.join(tmpdir, "test.parquet")

        # With "geo" metadata key
        tab = pa.table([ga.array(["POINT (0 1)"])], names=["geometry"])
        io.write_geoparquet_table(tab, temp_pq)
        tab2 = io.read_geoparquet_table(temp_pq)
        assert isinstance(tab2["geometry"].type, ga.GeometryExtensionType)
        assert b"geo" not in tab2.schema.metadata

        # Without "geo" metadata key
        tab = pa.table([pa.array(["POINT (0 1)"])], names=["geometry"])
        parquet.write_table(tab, temp_pq)
        tab2 = io.read_geoparquet_table(temp_pq)
        assert isinstance(tab2["geometry"].type, ga.GeometryExtensionType)


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


def test_geoparquet_guess_primary_geometry_column():
    assert (
        io._geoparquet_guess_primary_geometry_column(pa.schema([]), "explicit_name")
        == "explicit_name"
    )

    assert (
        io._geoparquet_guess_primary_geometry_column(
            pa.schema([pa.field("geometry", pa.binary())])
        )
        == "geometry"
    )

    with pytest.raises(ValueError, match="at least one geometry column"):
        io._geoparquet_guess_primary_geometry_column(
            pa.schema([pa.field("not_geom", pa.binary())])
        )

    assert (
        io._geoparquet_guess_primary_geometry_column(
            pa.schema([pa.field("first_def_geom", ga.wkb())])
        )
        == "first_def_geom"
    )


def test_geoparquet_columns_from_schema():
    schema = pa.schema([pa.field("col_a", ga.wkb()), pa.field("col_b", pa.binary())])

    # Guessing should just return GeoArrow columns
    cols = io._geoparquet_columns_from_schema(schema)
    assert list(cols.keys()) == ["col_a"]
    assert cols["col_a"] == {"encoding": "WKB", "geometry_types": [], "crs": None}

    # Explicit should just return specified columns
    cols_explicit = io._geoparquet_columns_from_schema(schema, ["col_b"])
    assert list(cols_explicit.keys()) == ["col_b"]
    assert cols_explicit["col_b"] == {"encoding": "WKB", "geometry_types": []}

    # Guessing should always include primary geometry column
    cols_primary = io._geoparquet_columns_from_schema(
        schema, primary_geometry_column="col_b"
    )
    assert list(cols_primary.keys()) == ["col_a", "col_b"]


def test_geoparquet_metadata_from_schema():
    schema = pa.schema([pa.field("col_a", ga.wkb()), pa.field("col_b", pa.binary())])
    metadata = io._geoparquet_metadata_from_schema(schema)
    assert list(metadata.keys()) == ["version", "primary_column", "columns"]
    assert metadata["version"] == "1.0.0"
    assert metadata["primary_column"] == "col_a"
    assert list(metadata["columns"].keys()) == ["col_a"]


def test_guess_geometry_columns():
    assert io._geoparquet_guess_geometry_columns(pa.schema([])) == {}

    guessed_wkb = io._geoparquet_guess_geometry_columns(
        pa.schema([pa.field("geometry", pa.binary())])
    )
    assert list(guessed_wkb.keys()) == ["geometry"]
    assert guessed_wkb["geometry"] == {"encoding": "WKB"}

    guessed_wkt = io._geoparquet_guess_geometry_columns(
        pa.schema([pa.field("geometry", pa.utf8())])
    )
    assert list(guessed_wkt.keys()) == ["geometry"]
    assert guessed_wkt["geometry"] == {"encoding": "WKT"}


def test_chunked_array_to_geoarrow_encodings():
    item_already_geoarrow = pa.chunked_array([ga.array(["POINT (0 1)"])])
    assert (
        io._geoparquet_chunked_array_to_geoarrow(item_already_geoarrow, {})
        is item_already_geoarrow
    )

    with pytest.raises(ValueError, match="missing 'encoding'"):
        io._geoparquet_chunked_array_to_geoarrow(pa.array([]), {})

    with pytest.raises(ValueError, match="Invalid GeoParquet encoding"):
        io._geoparquet_chunked_array_to_geoarrow(
            pa.array([]), {"encoding": "NotAnEncoding"}
        )

    item_binary = pa.chunked_array([ga.as_wkb(["POINT (0 1)"]).storage])
    item_geoarrow = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB", "crs": None}
    )
    assert item_geoarrow.type == ga.wkb()

    item_wkt = pa.chunked_array([ga.as_wkt(["POINT (0 1)"]).storage])
    item_geoarrow = io._geoparquet_chunked_array_to_geoarrow(
        item_wkt, {"encoding": "WKT", "crs": None}
    )
    assert item_geoarrow.type == ga.wkt()


def test_chunked_array_to_geoarrow_crs():
    item_binary = pa.chunked_array([ga.as_wkb(["POINT (0 1)"]).storage])

    item_missing_crs = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB"}
    )
    assert item_missing_crs.type.crs_type == ga.CrsType.PROJJSON

    item_explicit_crs = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB", "crs": {}}
    )
    assert item_explicit_crs.type.crs_type == ga.CrsType.PROJJSON
    assert item_explicit_crs.type.crs == "{}"


def test_chunked_array_to_geoarrow_edges():
    item_binary = pa.chunked_array([ga.as_wkb(["POINT (0 1)"]).storage])

    item_planar_default = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB"}
    )
    assert item_planar_default.type.edge_type == ga.EdgeType.PLANAR

    item_planar_explicit = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB", "edges": "planar"}
    )
    assert item_planar_explicit.type.edge_type == ga.EdgeType.PLANAR

    item_spherical = io._geoparquet_chunked_array_to_geoarrow(
        item_binary, {"encoding": "WKB", "edges": "spherical"}
    )
    assert item_spherical.type.edge_type == ga.EdgeType.SPHERICAL


def test_table_to_geoarrow():
    tab = pa.table([pa.array([], pa.binary())], names=["col_name"])
    tab_geo = io._geoparquet_table_to_geoarrow(tab, {"col_name": {"encoding": "WKB"}})
    assert "col_name" in tab_geo.schema.names
    assert isinstance(tab_geo["col_name"].type, ga.GeometryExtensionType)
    assert tab_geo["col_name"].type.crs_type == ga.CrsType.PROJJSON
