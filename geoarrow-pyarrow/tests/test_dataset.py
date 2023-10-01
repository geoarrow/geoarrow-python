from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import geoarrow.pyarrow as ga
import geoarrow.pyarrow.dataset as gads


def test_geodataset_column_name_guessing():
    table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    geods = gads.dataset(table)
    assert geods.geometry_columns == ("geometry",)


def test_geodataset_column_type_guessing():
    # Already a geoarrow type
    table = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    geods = gads.dataset(table, geometry_columns=["geometry"])
    assert geods.geometry_types == (ga.wkt(),)

    # utf8 maps to wkt
    table = pa.table([ga.array(["POINT (0.5 1.5)"]).storage], ["geometry"])
    geods = gads.dataset(table, geometry_columns=["geometry"])
    assert geods.geometry_types == (ga.wkt(),)

    # binary maps to wkb
    table = pa.table([ga.as_wkb(["POINT (0.5 1.5)"]).storage], ["geometry"])
    geods = gads.dataset(table, geometry_columns=["geometry"])
    assert geods.geometry_types == (ga.wkb(),)

    # Error for other types
    with pytest.raises(TypeError):
        table = pa.table([[123]], ["geometry"])
        geods = gads.dataset(table, geometry_columns=["geometry"])
        geods.geometry_types


def test_geodataset_in_memory():
    table1 = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    table2 = pa.table([ga.array(["POINT (2.5 3.5)"])], ["geometry"])

    geods = gads.dataset([table1, table2])
    assert isinstance(geods._parent, ds.InMemoryDataset)
    assert len(list(geods._parent.get_fragments())) == 2

    filtered1 = geods.filter_fragments("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))")
    assert isinstance(filtered1, gads.GeoDataset)
    assert filtered1.to_table().num_rows == 1
    assert filtered1._index.column("_fragment_index") == pa.chunked_array([[0]])
    assert filtered1._index.column("geometry") == geods._index.column("geometry").take(
        [1]
    )

    # Make sure we can filter to empty
    filtered0 = geods.filter_fragments("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    assert filtered0.to_table().num_rows == 0

    with pytest.raises(TypeError):
        gads.dataset([table1], use_row_groups=True)


def test_geodataset_in_memory_guessed_type():
    table1 = pa.table([ga.array(["POINT (0.5 1.5)"]).storage], ["geometry"])
    table2 = pa.table([ga.array(["POINT (2.5 3.5)"]).storage], ["geometry"])
    geods = gads.dataset([table1, table2], geometry_columns=["geometry"])

    filtered1 = geods.filter_fragments("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))")
    assert filtered1.to_table().num_rows == 1


def test_geodataset_multiple_geometry_columns():
    table1 = pa.table(
        [ga.array(["POINT (0.5 1.5)"]), ga.array(["POINT (2.5 3.5)"])],
        ["geometry1", "geometry2"],
    )
    table2 = pa.table(
        [ga.array(["POINT (4.5 5.5)"]), ga.array(["POINT (6.5 7.5)"])],
        ["geometry1", "geometry2"],
    )

    geods = gads.dataset([table1, table2])
    assert isinstance(geods._parent, ds.InMemoryDataset)
    assert len(list(geods._parent.get_fragments())) == 2

    filtered1 = geods.filter_fragments("POLYGON ((0 1, 1 1, 1 2, 0 2, 0 1))").to_table()
    assert filtered1.num_rows == 1

    filtered2 = geods.filter_fragments("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))").to_table()
    assert filtered2.num_rows == 1


def test_geodataset_parquet():
    table1 = pa.table([ga.array(["POINT (0.5 1.5)"])], ["geometry"])
    table2 = pa.table([ga.array(["POINT (2.5 3.5)"])], ["geometry"])
    with TemporaryDirectory() as td:
        pq.write_table(table1, f"{td}/table1.parquet")
        pq.write_table(table2, f"{td}/table2.parquet")
        geods = gads.dataset(
            [f"{td}/table1.parquet", f"{td}/table2.parquet"], use_row_groups=False
        )

        filtered1 = geods.filter_fragments(
            "POLYGON ((0 1, 1 1, 1 2, 0 2, 0 1))"
        ).to_table()
        assert filtered1.num_rows == 1


def test_geodataset_parquet_rowgroups():
    table = pa.table([ga.array(["POINT (0.5 1.5)", "POINT (2.5 3.5)"])], ["geometry"])
    with TemporaryDirectory() as td:
        pq.write_table(table, f"{td}/table.parquet", row_group_size=1)

        geods = gads.dataset(f"{td}/table.parquet")
        assert isinstance(geods, gads.ParquetRowGroupGeoDataset)
        assert len(geods.get_fragments()) == 2

        filtered1 = geods.filter_fragments("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))")
        assert isinstance(filtered1, gads.ParquetRowGroupGeoDataset)
        assert filtered1.to_table().num_rows == 1
        assert filtered1._index.column("_fragment_index") == pa.chunked_array([[0]])
        assert filtered1._index.column("geometry") == geods._index.column(
            "geometry"
        ).take([1])

        assert filtered1._row_group_ids == [1]


def test_geodataset_parquet_index_rowgroups():
    array_wkt = ga.array(
        ["LINESTRING (0.5 1.5, 2.5 3.5)", "LINESTRING (4.5 5.5, 6.5 7.5)"]
    )
    array_geoarrow = ga.as_geoarrow(
        ["LINESTRING (8.5 9.5, 10.5 11.5)", "LINESTRING (12.5 13.5, 14.5 15.5)"]
    )

    table_wkt = pa.table([array_wkt], ["geometry"])
    table_geoarrow = pa.table([array_geoarrow], ["geometry"])
    table_both = pa.table(
        [array_wkt, array_geoarrow], ["geometry_wkt", "geometry_geoarrow"]
    )

    with TemporaryDirectory() as td:
        pq.write_table(table_wkt, f"{td}/table_wkt.parquet", row_group_size=1)
        pq.write_table(table_geoarrow, f"{td}/table_geoarrow.parquet", row_group_size=1)
        pq.write_table(
            table_geoarrow,
            f"{td}/table_geoarrow_nostats.parquet",
            row_group_size=1,
            write_statistics=False,
        )
        pq.write_table(table_both, f"{td}/table_both.parquet", row_group_size=1)

        ds_wkt = gads.dataset(f"{td}/table_wkt.parquet")
        ds_geoarrow = gads.dataset(f"{td}/table_geoarrow.parquet")
        ds_geoarrow_nostats = gads.dataset(f"{td}/table_geoarrow_nostats.parquet")
        ds_both = gads.dataset(f"{td}/table_both.parquet")

        index_wkt = ds_wkt.index_fragments()
        index_geoarrow = ds_geoarrow.index_fragments()
        index_geoarrow_nostats = ds_geoarrow_nostats.index_fragments()
        index_both = ds_both.index_fragments()

        # All the fragment indices should be the same
        assert index_geoarrow.column(0) == index_wkt.column(0)
        assert index_geoarrow_nostats.column(0) == index_wkt.column(0)
        assert index_both.column(0) == index_wkt.column(0)

        # The wkt index should be the same in index_both and index_wkt
        assert index_both.column("geometry_wkt") == index_wkt.column("geometry")

        # The geoarrow index should be the same everywhere
        assert index_geoarrow_nostats.column("geometry") == index_geoarrow.column(
            "geometry"
        )
        assert index_both.column("geometry_geoarrow") == index_geoarrow.column(
            "geometry"
        )


def test_geodataset_parquet_filter_rowgroups_with_stats():
    arr = ga.as_geoarrow(["POINT (0.5 1.5)", "POINT (2.5 3.5)"])
    table = pa.table([arr], ["geometry"])
    with TemporaryDirectory() as td:
        pq.write_table(table, f"{td}/table.parquet", row_group_size=1)

        geods = gads.dataset(f"{td}/table.parquet")
        assert len(geods.get_fragments()) == 2

        geods._build_index_using_stats(["geometry"])

        filtered1 = geods.filter_fragments(
            "POLYGON ((0 1, 1 1, 1 2, 0 2, 0 1))"
        ).to_table()
        assert filtered1.num_rows == 1


def test_parquet_fields_before():
    schema = pa.schema([pa.field("col1", pa.int32()), pa.field("col2", pa.int32())])
    fields_before = gads.ParquetRowGroupGeoDataset._count_fields_before(schema)
    assert fields_before == [(("col1",), 0), (("col2",), 1)]

    schema = pa.schema(
        [pa.field("col1", pa.list_(pa.int32())), pa.field("col2", pa.int32())]
    )
    fields_before = gads.ParquetRowGroupGeoDataset._count_fields_before(schema)
    assert fields_before == [(("col1",), 0), (("col1", "item"), 0), (("col2",), 1)]

    schema = pa.schema(
        [pa.field("col1", ga.linestring()), pa.field("col2", pa.int32())]
    )
    fields_before = gads.ParquetRowGroupGeoDataset._count_fields_before(schema)
    assert fields_before == [
        (("col1",), 0),
        (("col1", "vertices"), 0),
        (("col1", "vertices", "x"), 0),
        (("col1", "vertices", "y"), 1),
        (("col2",), 2),
    ]
