import pytest
import tempfile
import os

pyogrio = pytest.importorskip("pyogrio")
geopandas = pytest.importorskip("geopandas")

import geoarrow.pyarrow as ga
from geoarrow.pyarrow import io


def test_readpyogrio_table():
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
