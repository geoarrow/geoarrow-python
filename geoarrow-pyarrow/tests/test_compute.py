import math

import pyarrow as pa
import numpy as np
import pytest

from geoarrow import types
import geoarrow.pyarrow as ga
import geoarrow.pyarrow._kernel as _kernel
import geoarrow.pyarrow._compute as _compute
import geoarrow.c.lib as lib


def test_as_array_or_chunked():
    wkt_array = ga.array(["POINT (0 1)"])
    assert _compute.obj_as_array_or_chunked(wkt_array) is wkt_array

    wkt_chunked = pa.chunked_array([wkt_array])
    assert _compute.obj_as_array_or_chunked(wkt_chunked)

    wkt_array2 = _compute.obj_as_array_or_chunked(["POINT (0 1)"])
    assert wkt_array2.storage == wkt_array.storage

    wkt_array3 = _compute.obj_as_array_or_chunked(wkt_array.storage)
    assert wkt_array3.storage == wkt_array.storage


def test_push_all():
    wkt_array = ga.array(["POINT (0 1)"])
    wkt_chunked0 = pa.chunked_array([], type=wkt_array.type)
    wkt_chunked1 = pa.chunked_array([wkt_array])
    wkt_chunked2 = pa.chunked_array([wkt_array, ga.array(["POINT (2 3)"])])

    result_array = _compute.push_all(_kernel.Kernel.as_wkt, wkt_array)
    assert result_array.storage == wkt_array.storage

    result_chunked0 = _compute.push_all(_kernel.Kernel.as_wkt, wkt_chunked0)
    assert result_chunked0 == wkt_chunked0

    result_chunked1 = _compute.push_all(_kernel.Kernel.as_wkt, wkt_chunked1)
    for result_chunk, wkt_chunk in zip(result_chunked1.chunks, wkt_chunked1.chunks):
        assert result_chunk.storage == wkt_chunk.storage

    result_chunked2 = _compute.push_all(_kernel.Kernel.as_wkt, wkt_chunked2)
    for result_chunk, wkt_chunk in zip(result_chunked2.chunks, wkt_chunked2.chunks):
        assert result_chunk.storage == wkt_chunk.storage


def test_parse_all():
    assert _compute.parse_all(["POINT (0 1)"]) is None
    with pytest.raises(lib.GeoArrowCException):
        _compute.parse_all(["not valid wkt"])

    geoarrow_array = ga.as_geoarrow(["POINT (0 1)"])
    assert _compute.parse_all(geoarrow_array) is None


def test_as_wkt():
    wkt_array = ga.array(["POINT (0 1)"])
    assert _compute.as_wkt(wkt_array) is wkt_array

    assert _compute.as_wkt(ga.as_wkb(wkt_array)).storage == wkt_array.storage


def test_as_wkb():
    wkb_array = ga.as_wkb(["POINT (0 1)"])
    assert _compute.as_wkb(wkb_array) is wkb_array

    assert _compute.as_wkb(ga.as_wkb(wkb_array)).storage == wkb_array.storage


def test_any_ewkb():
    not_ewkb_item = (
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3e\x40"
        b"\x00\x00\x00\x00\x00\x00\x24\x40"
    )
    ewkb_item = (
        b"\x01\x01\x00\x00\x80\x00\x00\x00\x00\x00\x00\x3e\x40"
        b"\x00\x00\x00\x00\x00\x00\x24\x40\x00\x00\x00\x00\x00\x00\x24\x40"
    )

    # No ewkb in a zero-size array
    assert _compute._any_ewkb(pa.array([], pa.binary())) is False

    # No ewkb in something that is not WKB
    assert _compute._any_ewkb(ga.as_geoarrow(["POINT (0 1)"])) is False

    # Check size 1 array for both
    assert _compute._any_ewkb(pa.array([not_ewkb_item])) is False
    assert _compute._any_ewkb(pa.array([ewkb_item])) is True

    # Check size 2 array in both orders
    assert _compute._any_ewkb(pa.array([ewkb_item, not_ewkb_item])) is True
    assert _compute._any_ewkb(pa.array([not_ewkb_item, ewkb_item])) is True

    # Undefined results for invalid WKB, but it shouldn't error
    assert _compute._any_ewkb(pa.array([b""])) is True

    # Check that as_wkb() can fix ewkb
    wkb_array_with_ewkb = ga.array([ewkb_item, not_ewkb_item])
    assert _compute.as_wkb(wkb_array_with_ewkb) is wkb_array_with_ewkb
    fixed = _compute.as_wkb(wkb_array_with_ewkb, strict_iso_wkb=True)
    assert fixed != wkb_array_with_ewkb


def test_format_wkt():
    wkt_array = ga.array(["POINT (0 1)"])
    assert _compute.format_wkt(wkt_array, max_element_size_bytes=5) == pa.array(
        ["POINT"]
    )


def test_unique_geometry_types():
    ga_array = ga.as_geoarrow(pa.array([], type=pa.utf8()), ga.point())
    out = _compute.unique_geometry_types(ga_array).flatten()
    assert out[0] == pa.array([ga.GeometryType.POINT.value], type=pa.int32())
    assert out[1] == pa.array([ga.Dimensions.XY.value], type=pa.int32())

    wkt_array = ga.array(
        [
            "POINT ZM (0 1 2 3)",
            "LINESTRING M (0 0 0, 1 1 1)",
            "POLYGON Z ((0 0 0, 1 0 0, 0 1 0, 0 0 0))",
            "MULTIPOINT (0 1)",
        ]
    )

    out = _compute.unique_geometry_types(wkt_array).flatten()
    assert out[0] == pa.array(
        [
            ga.GeometryType.MULTIPOINT.value,
            ga.GeometryType.POLYGON.value,
            ga.GeometryType.LINESTRING.value,
            ga.GeometryType.POINT.value,
        ],
        type=pa.int32(),
    )

    assert out[1] == pa.array(
        [
            ga.Dimensions.XY.value,
            ga.Dimensions.XYZ.value,
            ga.Dimensions.XYM.value,
            ga.Dimensions.XYZM.value,
        ],
        type=pa.int32(),
    )


def test_infer_type_common():
    empty = ga.wkt().wrap_array(pa.array([], type=pa.utf8()))
    common = _compute.infer_type_common(empty)
    assert common == pa.null()

    already_geoarrow = ga.as_geoarrow(["POINT (0 1)"])
    common = _compute.infer_type_common(already_geoarrow)
    assert common.spec == already_geoarrow.type.spec
    common_interleaved = _compute.infer_type_common(
        already_geoarrow, coord_type=ga.CoordType.INTERLEAVED
    )
    assert (
        common_interleaved.spec
        == already_geoarrow.type.with_coord_type(ga.CoordType.INTERLEAVED).spec
    )

    point = ga.wkt().with_crs(types.OGC_CRS84).wrap_array(pa.array(["POINT (0 1)"]))
    common = _compute.infer_type_common(point)
    assert common.spec == ga.point().with_crs(types.OGC_CRS84).spec

    common_promote_multi = _compute.infer_type_common(point, promote_multi=True)
    assert common_promote_multi.spec == ga.multipoint().with_crs(types.OGC_CRS84).spec

    point_z_and_zm = ga.array(["POINT (0 1)", "POINT ZM (0 1 2 3)"])
    common = _compute.infer_type_common(point_z_and_zm)
    assert common.spec == ga.point().with_dimensions(ga.Dimensions.XYZM).spec

    point_m_and_z = ga.array(["POINT M (0 1 2)", "POINT Z (0 1 2)"])
    common = _compute.infer_type_common(point_m_and_z)
    assert common.spec == ga.point().with_dimensions(ga.Dimensions.XYZM).spec

    mixed = (
        ga.wkt()
        .with_crs(types.OGC_CRS84)
        .wrap_array(pa.array(["POINT (0 1)", "LINESTRING (0 1, 2 3)"]))
    )
    common = _compute.infer_type_common(mixed)
    assert common.spec == ga.wkb().with_crs(types.OGC_CRS84).spec

    point_and_multi = ga.array(["POINT (0 1)", "MULTIPOINT (2 3)"])
    common = _compute.infer_type_common(point_and_multi)
    assert common.spec == ga.multipoint().spec

    linestring_and_multi = ga.array(
        ["LINESTRING (0 1, 2 3)", "MULTILINESTRING ((0 1, 2 3))"]
    )
    common = _compute.infer_type_common(linestring_and_multi)
    assert common.spec == ga.multilinestring().spec

    polygon_and_multi = ga.array(
        ["POLYGON ((0 0, 0 1, 1 0, 0 0))", "MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))"]
    )
    common = _compute.infer_type_common(polygon_and_multi)
    assert common.spec == ga.multipolygon().spec


def test_as_geoarrow():
    array = _compute.as_geoarrow(["POINT (0 1)"])
    assert array.type.spec == ga.point().spec

    array2 = _compute.as_geoarrow(array)
    assert array2 is array

    array2 = _compute.as_geoarrow(array, coord_type=ga.CoordType.INTERLEAVED)
    assert array2.type.spec == ga.point().with_coord_type(ga.CoordType.INTERLEAVED).spec

    array = _compute.as_geoarrow(["POINT (0 1)"], coord_type=ga.CoordType.INTERLEAVED)
    assert array.type.spec == ga.point().with_coord_type(ga.CoordType.INTERLEAVED).spec

    array = _compute.as_geoarrow(["POINT (0 1)"], type=ga.multipoint())
    assert array.type.spec == ga.multipoint().spec

    array = _compute.as_geoarrow(["POINT (0 1)", "LINESTRING (0 1, 2 3)"])
    assert array.type.spec == ga.wkb().spec


def test_make_point():
    xs = [1, 2, 3]
    ys = [4, 5, 6]
    zs = [7, 8, 9]
    ms = [10, 11, 12]

    xy = _compute.make_point(xs, ys)
    assert xy.type.dimensions == ga.Dimensions.XY
    assert _compute.format_wkt(xy).to_pylist() == [
        "POINT (1 4)",
        "POINT (2 5)",
        "POINT (3 6)",
    ]

    xyz = _compute.make_point(xs, ys, zs)
    assert xyz.type.dimensions == ga.Dimensions.XYZ
    assert _compute.format_wkt(xyz).to_pylist() == [
        "POINT Z (1 4 7)",
        "POINT Z (2 5 8)",
        "POINT Z (3 6 9)",
    ]

    xym = _compute.make_point(xs, ys, m=ms)
    assert xym.type.dimensions == ga.Dimensions.XYM
    assert _compute.format_wkt(xym).to_pylist() == [
        "POINT M (1 4 10)",
        "POINT M (2 5 11)",
        "POINT M (3 6 12)",
    ]

    xyzm = _compute.make_point(xs, ys, zs, ms)
    assert xyzm.type.dimensions == ga.Dimensions.XYZM
    assert _compute.format_wkt(xyzm).to_pylist() == [
        "POINT ZM (1 4 7 10)",
        "POINT ZM (2 5 8 11)",
        "POINT ZM (3 6 9 12)",
    ]

    xy_crs = _compute.make_point(xs, ys, crs=types.OGC_CRS84)
    assert xy_crs.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()


def test_box():
    wkt_array = ga.array(["POINT (0 1)", "POINT (2 3)"])
    box = _compute.box(wkt_array)
    assert box[0].as_py() == {"xmin": 0, "xmax": 0, "ymin": 1, "ymax": 1}
    assert box[1].as_py() == {"xmin": 2, "xmax": 2, "ymin": 3, "ymax": 3}

    # Test optmizations that zero-copy rearrange the points
    array = _compute.as_geoarrow(["POINT (0 1)", "POINT (2 3)"])
    box2 = _compute.box(array)
    assert box2 == box

    chunked_array = pa.chunked_array([array])
    box2 = _compute.box(chunked_array)
    assert box2 == pa.chunked_array([box])

    # Make sure spherical edges error
    with pytest.raises(TypeError):
        wkt_spherical = _compute.with_edge_type(wkt_array, ga.EdgeType.SPHERICAL)
        _compute.box(wkt_spherical)


def test_box_agg():
    wkt_array = ga.array(["POINT (0 1)", "POINT (2 3)"])
    box = _compute.box_agg(wkt_array)
    assert box.as_py() == {"xmin": 0, "xmax": 2, "ymin": 1, "ymax": 3}

    # Test optmization that uses pyarrow.compute.min_max()
    array = _compute.as_geoarrow(["POINT (0 1)", "POINT (2 3)"])
    box2 = _compute.box_agg(array)
    assert box2 == box

    chunked_array = pa.chunked_array([array])
    box2 = _compute.box_agg(chunked_array)
    assert box2 == box

    # Make sure spherical edges error
    with pytest.raises(TypeError):
        wkt_spherical = _compute.with_edge_type(wkt_array, ga.EdgeType.SPHERICAL)
        _compute.box(wkt_spherical)


def test_rechunk_max_bytes():
    wkt_array = ga.array(
        ["LINESTRING (0 1, 2 3, 4 5)", "LINESTRING (0 1, 2 3)", "POINT (0 1)"]
    )
    not_rechunked = _compute.rechunk(wkt_array, max_bytes=1000)
    assert isinstance(not_rechunked, pa.ChunkedArray)
    assert not_rechunked.chunks[0] == wkt_array

    rechunked = _compute.rechunk(wkt_array, max_bytes=4)
    assert isinstance(rechunked, pa.ChunkedArray)
    assert rechunked.num_chunks == 3
    assert len(rechunked) == 3

    rechunked_chunked_array = _compute.rechunk(
        pa.chunked_array([wkt_array]), max_bytes=4
    )
    assert rechunked_chunked_array == rechunked

    rechunked_with_empty_chunk = _compute.rechunk(
        pa.chunked_array(wkt_array[:0]), max_bytes=4
    )
    assert rechunked_with_empty_chunk == pa.chunked_array([], type=wkt_array.type)


def test_with_edge_type():
    storage_array = pa.array(["POINT (0 1)", "POINT (2 3)"])
    spherical = _compute.with_edge_type(storage_array, ga.EdgeType.SPHERICAL)
    assert isinstance(spherical.type, ga.WktType)
    assert spherical.type.edge_type == ga.EdgeType.SPHERICAL

    planar = _compute.with_edge_type(spherical, ga.EdgeType.PLANAR)
    assert planar.type.edge_type == ga.EdgeType.PLANAR

    planar_chunked = pa.chunked_array([planar])
    spherical_chunked = _compute.with_edge_type(planar_chunked, ga.EdgeType.SPHERICAL)
    assert spherical_chunked.type.edge_type == ga.EdgeType.SPHERICAL


def test_with_crs():
    storage_array = pa.array(["POINT (0 1)", "POINT (2 3)"])
    crsified = _compute.with_crs(storage_array, types.OGC_CRS84)
    assert isinstance(crsified.type, ga.WktType)
    assert crsified.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()

    crsified2 = _compute.with_crs(crsified, "OGC:CRS84")
    assert repr(crsified2.type.crs) == "StringCrs(OGC:CRS84)"

    crsnope = _compute.with_crs(crsified, None)
    assert crsnope.type.crs is None

    crsnope_chunked = pa.chunked_array([crsnope])
    crsified_chunked = _compute.with_crs(crsnope_chunked, types.OGC_CRS84)
    assert crsified_chunked.type.crs.to_json_dict() == types.OGC_CRS84.to_json_dict()


def test_with_coord_type():
    wkt_array = ga.array(["POINT (0 1)", "POINT (2 3)"])
    with_interleaved = _compute.with_coord_type(wkt_array, ga.CoordType.INTERLEAVED)
    assert with_interleaved.type.coord_type == ga.CoordType.INTERLEAVED

    with_struct = _compute.with_coord_type(with_interleaved, ga.CoordType.SEPARATED)
    assert with_struct.type.coord_type == ga.CoordType.SEPARATED


def test_with_dimensions():
    wkt_array = ga.array(["POINT (0 1)", "POINT (2 3)"])
    xyz = _compute.with_dimensions(wkt_array, ga.Dimensions.XYZ)
    assert xyz.type.dimensions == ga.Dimensions.XYZ
    assert _compute.as_wkt(xyz).storage[0].as_py() == "POINT Z (0 1 nan)"

    xyz2 = _compute.with_dimensions(xyz, ga.Dimensions.XYZ)
    assert xyz2 == xyz


def test_with_geometry_type():
    wkt_array = ga.array(["POINT (0 1)", "POINT (2 3)"])
    point = _compute.as_geoarrow(wkt_array)
    multipoint = _compute.with_geometry_type(point, ga.GeometryType.MULTIPOINT)
    assert multipoint.type.geometry_type == ga.GeometryType.MULTIPOINT

    multipoint2 = _compute.with_geometry_type(multipoint, ga.GeometryType.MULTIPOINT)
    assert multipoint2 == multipoint

    point2 = _compute.with_geometry_type(multipoint2, ga.GeometryType.POINT)
    assert point2 == point


def test_point_coords():
    x, y = _compute.point_coords(["POINT (0 1)", "POINT (2 3)"])
    assert x == pa.array([0.0, 2.0])
    assert y == pa.array([1.0, 3.0])

    x, y, z = _compute.point_coords(["POINT (0 1)", "POINT (2 3)"], ga.Dimensions.XYZ)
    assert x == pa.array([0.0, 2.0])
    assert y == pa.array([1.0, 3.0])
    assert all(math.isnan(el.as_py()) for el in z)

    chunked = pa.chunked_array([ga.as_wkt(["POINT (0 1)", "POINT (2 3)"])])
    x, y = _compute.point_coords(chunked)
    assert x == pa.chunked_array([pa.array([0.0, 2.0])])
    assert y == pa.chunked_array([pa.array([1.0, 3.0])])


def test_point_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    point = ga.point().wrap_array(point_storage[1:])
    assert ga.as_wkt(point) == ga.as_wkt(["POINT (2 3)", "POINT (4 5)", "POINT (6 7)"])


def test_linestring_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    linestring_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 3], values=point_storage[1:]
    )

    linestring = ga.linestring().wrap_array(linestring_storage[2:])
    assert ga.as_wkt(linestring) == ga.as_wkt(["LINESTRING (2 3, 4 5, 6 7)"])


def test_polygon_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    ring_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 3], values=point_storage[1:]
    )

    polygon_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 0, 1], values=ring_storage[2:]
    )

    polygon = ga.polygon().wrap_array(polygon_storage[3:])
    assert ga.as_wkt(polygon) == ga.as_wkt(["POLYGON ((2 3, 4 5, 6 7))"])


def test_multipoint_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    multipoint_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 3], values=point_storage[1:]
    )

    multipoint = ga.multipoint().wrap_array(multipoint_storage[2:])
    assert ga.as_wkt(multipoint) == ga.as_wkt(["MULTIPOINT (2 3, 4 5, 6 7)"])


def test_multilinestring_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    linestring_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 3], values=point_storage[1:]
    )

    multilinestring_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 0, 1], values=linestring_storage[2:]
    )

    multilinestring = ga.multilinestring().wrap_array(multilinestring_storage[3:])
    assert ga.as_wkt(multilinestring) == ga.as_wkt(
        ["MULTILINESTRING ((2 3, 4 5, 6 7))"]
    )


def test_multipolygon_with_offset():
    point_storage = pa.array(
        [
            {"x": 0.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 4.0, "y": 5.0},
            {"x": 6.0, "y": 7.0},
        ]
    )

    ring_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 3], values=point_storage[1:]
    )

    polygon_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 0, 1], values=ring_storage[2:]
    )

    multipolygon_storage = pa.ListArray.from_arrays(
        offsets=[0, 0, 0, 0, 0, 1], values=polygon_storage[3:]
    )

    multipolygon = ga.multipolygon().wrap_array(multipolygon_storage[4:])
    assert ga.as_wkt(multipolygon) == ga.as_wkt(["MULTIPOLYGON (((2 3, 4 5, 6 7)))"])


def test_multipolygon_with_offset_nonempty_inner_lists():
    ordinate_storage = pa.array([float(i) for i in range(101)])
    point_storage = pa.StructArray.from_arrays(
        [ordinate_storage[:100], ordinate_storage[1:]], names=["x", "y"]
    )

    ring_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([2, 3] * 19)), values=point_storage[5:]
    )

    polygon_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([0, 1, 2] * 12)), values=ring_storage[2:]
    )

    multipolygon_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([1, 2] * 11)), values=polygon_storage[3:]
    )

    multipolygon = ga.multipolygon().wrap_array(multipolygon_storage[21:])
    assert ga.as_wkt(multipolygon) == ga.as_wkt(
        [
            "MULTIPOLYGON (((92 93, 93 94, 94 95)), ((95 96, 96 97), (97 98, 98 99, 99 100)))"
        ]
    )


def test_interleaved_multipolygon_with_offset():
    ordinate_storage_list = [-1]
    for i in range(100):
        ordinate_storage_list.append(float(i))
        ordinate_storage_list.append(i + 1.0)
    ordinate_storage = pa.array(ordinate_storage_list)

    point_storage = pa.FixedSizeListArray.from_arrays(ordinate_storage[1:], list_size=2)

    ring_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([2, 3] * 19)), values=point_storage[5:]
    )

    polygon_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([0, 1, 2] * 12)), values=ring_storage[2:]
    )

    multipolygon_storage = pa.ListArray.from_arrays(
        offsets=[0] + list(np.cumsum([1, 2] * 11)), values=polygon_storage[3:]
    )

    multipolygon = (
        ga.multipolygon()
        .with_coord_type(ga.CoordType.INTERLEAVED)
        .wrap_array(multipolygon_storage[21:])
    )
    assert ga.as_wkt(multipolygon) == ga.as_wkt(
        [
            "MULTIPOLYGON (((92 93, 93 94, 94 95)), ((95 96, 96 97), (97 98, 98 99, 99 100)))"
        ]
    )
