import numpy as np
import pyarrow as pa
import pytest

import geoarrow.types as gt
from geoarrow.types import type_pyarrow


def test_wrap_array_non_exact():
    pa_version_tuple = tuple(int(component) for component in pa.__version__.split("."))
    if pa_version_tuple < (14,):
        pytest.skip("wrap_array with non-exact type requires pyarrow >= 14")

    from pyarrow import compute as pc

    storage = pc.make_struct(
        pa.array([1.0, 2.0, 3.0]), pa.array([3.0, 4.0, 5.0]), field_names=["x", "y"]
    )
    point = gt.point().to_pyarrow()
    point_ext = point.wrap_array(storage)
    assert point_ext.type.storage_type.field(0).nullable is False

    storage_chunked = pa.chunked_array([storage, storage])
    point_chunked_ext = point.wrap_array(storage_chunked)
    assert point_chunked_ext.type.storage_type.field(0).nullable is False
    assert point_chunked_ext.num_chunks == 2


def test_classes_serialized():
    wkt = gt.wkt().to_pyarrow()
    assert isinstance(wkt, type_pyarrow.WktType)
    assert wkt.encoding == gt.Encoding.WKT
    assert wkt.geometry_type == gt.GeometryType.GEOMETRY
    assert wkt.dimensions == gt.Dimensions.UNKNOWN
    assert wkt.coord_type == gt.CoordType.UNSPECIFIED

    wkb = gt.wkb().to_pyarrow()
    assert isinstance(wkb, type_pyarrow.WkbType)
    assert wkb.encoding == gt.Encoding.WKB
    assert wkb.geometry_type == gt.GeometryType.GEOMETRY
    assert wkb.dimensions == gt.Dimensions.UNKNOWN
    assert wkb.coord_type == gt.CoordType.UNSPECIFIED


def test_geometry_types():
    xy = pa.struct(
        [
            pa.field("x", pa.float64(), nullable=False),
            pa.field("y", pa.float64(), nullable=False),
        ]
    )

    point = gt.point().to_pyarrow()
    assert isinstance(point, type_pyarrow.PointType)
    assert point.encoding == gt.Encoding.GEOARROW
    assert point.geometry_type == gt.GeometryType.POINT
    assert point.storage_type == xy

    linestring = gt.linestring().to_pyarrow()
    assert isinstance(linestring, type_pyarrow.LinestringType)
    assert linestring.encoding == gt.Encoding.GEOARROW
    assert linestring.geometry_type == gt.GeometryType.LINESTRING
    assert linestring.storage_type == pa.list_(pa.field("vertices", xy, False))

    polygon = gt.polygon().to_pyarrow()
    assert isinstance(polygon, type_pyarrow.PolygonType)
    assert polygon.encoding == gt.Encoding.GEOARROW
    assert polygon.geometry_type == gt.GeometryType.POLYGON
    assert polygon.storage_type == pa.list_(
        pa.field("rings", pa.list_(pa.field("vertices", xy, False)), False)
    )

    multipoint = gt.multipoint().to_pyarrow()
    assert isinstance(multipoint, type_pyarrow.MultiPointType)
    assert multipoint.encoding == gt.Encoding.GEOARROW
    assert multipoint.geometry_type == gt.GeometryType.MULTIPOINT
    assert multipoint.storage_type == pa.list_(pa.field("points", xy, False))

    multilinestring = gt.multilinestring().to_pyarrow()
    assert isinstance(multilinestring, type_pyarrow.MultiLinestringType)
    assert multilinestring.encoding == gt.Encoding.GEOARROW
    assert multilinestring.geometry_type == gt.GeometryType.MULTILINESTRING
    assert polygon.storage_type == pa.list_(
        pa.field("linestrings", pa.list_(pa.field("vertices", xy, False)), False)
    )

    multipolygon = gt.multipolygon().to_pyarrow()
    assert isinstance(multipolygon, type_pyarrow.MultiPolygonType)
    assert multipolygon.encoding == gt.Encoding.GEOARROW
    assert multipolygon.geometry_type == gt.GeometryType.MULTIPOLYGON
    assert multipolygon.storage_type == pa.list_(
        pa.field(
            "polygons",
            pa.list_(
                pa.field("rings", pa.list_(pa.field("vertices", xy, False)), False)
            ),
            False,
        )
    )

    with pytest.raises(ValueError, match="Can't compute extension name"):
        gt.type_spec(gt.GeometryType.GEOMETRYCOLLECTION).to_pyarrow()


def test_interleaved_dimensions():
    point_xy = gt.point(dimensions="xy", coord_type="interleaved").to_pyarrow()
    assert point_xy.coord_type == gt.CoordType.INTERLEAVED
    assert point_xy.dimensions == gt.Dimensions.XY
    assert point_xy.storage_type.field(0).name == "xy"

    point_xyz = gt.point(dimensions="xyz", coord_type="interleaved").to_pyarrow()
    assert point_xyz.coord_type == gt.CoordType.INTERLEAVED
    assert point_xyz.dimensions == gt.Dimensions.XYZ
    assert point_xyz.storage_type.field(0).name == "xyz"

    point_xym = gt.point(dimensions="xym", coord_type="interleaved").to_pyarrow()
    assert point_xym.coord_type == gt.CoordType.INTERLEAVED
    assert point_xym.dimensions == gt.Dimensions.XYM
    assert point_xym.storage_type.field(0).name == "xym"

    point_xyzm = gt.point(dimensions="xyzm", coord_type="interleaved").to_pyarrow()
    assert point_xyzm.coord_type == gt.CoordType.INTERLEAVED
    assert point_xyzm.dimensions == gt.Dimensions.XYZM
    assert point_xyzm.storage_type.field(0).name == "xyzm"


def test_separated_dimensions():
    point_xy = gt.point(dimensions="xy", coord_type="separated").to_pyarrow()
    assert point_xy.coord_type == gt.CoordType.SEPARATED
    assert point_xy.dimensions == gt.Dimensions.XY
    storage_names = [
        point_xy.storage_type.field(i).name
        for i in range(point_xy.storage_type.num_fields)
    ]
    assert storage_names == ["x", "y"]

    point_xyz = gt.point(dimensions="xyz", coord_type="separated").to_pyarrow()
    assert point_xy.coord_type == gt.CoordType.SEPARATED
    assert point_xyz.dimensions == gt.Dimensions.XYZ
    storage_names = [
        point_xyz.storage_type.field(i).name
        for i in range(point_xyz.storage_type.num_fields)
    ]
    assert storage_names == ["x", "y", "z"]

    point_xym = gt.point(dimensions="xym", coord_type="separated").to_pyarrow()
    assert point_xy.coord_type == gt.CoordType.SEPARATED
    assert point_xym.dimensions == gt.Dimensions.XYM
    storage_names = [
        point_xym.storage_type.field(i).name
        for i in range(point_xym.storage_type.num_fields)
    ]
    assert storage_names == ["x", "y", "m"]

    point_xyzm = gt.point(dimensions="xyzm", coord_type="separated").to_pyarrow()
    assert point_xy.coord_type == gt.CoordType.SEPARATED
    assert point_xyzm.dimensions == gt.Dimensions.XYZM
    storage_names = [
        point_xyzm.storage_type.field(i).name
        for i in range(point_xyzm.storage_type.num_fields)
    ]
    assert storage_names == ["x", "y", "z", "m"]


def test_deserialize_infer_encoding():
    extension_type = type_pyarrow._deserialize_storage(pa.utf8())
    assert extension_type.encoding == gt.Encoding.WKT

    extension_type = type_pyarrow._deserialize_storage(pa.large_utf8())
    assert extension_type.encoding == gt.Encoding.LARGE_WKT

    extension_type = type_pyarrow._deserialize_storage(pa.binary())
    assert extension_type.encoding == gt.Encoding.WKB

    extension_type = type_pyarrow._deserialize_storage(pa.large_binary())
    assert extension_type.encoding == gt.Encoding.LARGE_WKB

    # Should fail if given if given a non-sensical type
    with pytest.raises(ValueError, match="Can't guess encoding from type nesting"):
        type_pyarrow._deserialize_storage(pa.float64())

    # ...and slightly differently if it uses a type that is never
    # used in any geoarrow storage
    with pytest.raises(
        ValueError, match="Type int8 is not a valid GeoArrow type component"
    ):
        type_pyarrow._deserialize_storage(pa.int8())


def test_deserialize_infer_geometry_type():
    # We can infer the required information for points and multipolygons
    # based purely on the level of nesting.
    point = pa.struct({"x": pa.float64(), "y": pa.float64()})
    multipolygon = pa.list_(pa.list_(pa.list_(point)))
    interleaved_point = pa.list_(pa.float64(), list_size=2)
    interleaved_multipolygon = pa.list_(pa.list_(pa.list_(interleaved_point)))

    extension_type = type_pyarrow._deserialize_storage(point)
    assert extension_type.encoding == gt.Encoding.GEOARROW
    assert extension_type.geometry_type == gt.GeometryType.POINT
    assert extension_type.coord_type == gt.CoordType.SEPARATED

    extension_type = type_pyarrow._deserialize_storage(multipolygon)
    assert extension_type.encoding == gt.Encoding.GEOARROW
    assert extension_type.geometry_type == gt.GeometryType.MULTIPOLYGON
    assert extension_type.coord_type == gt.CoordType.SEPARATED

    extension_type = type_pyarrow._deserialize_storage(interleaved_point)
    assert extension_type.encoding == gt.Encoding.GEOARROW
    assert extension_type.geometry_type == gt.GeometryType.POINT
    assert extension_type.coord_type == gt.CoordType.INTERLEAVED

    extension_type = type_pyarrow._deserialize_storage(interleaved_multipolygon)
    assert extension_type.encoding == gt.Encoding.GEOARROW
    assert extension_type.geometry_type == gt.GeometryType.MULTIPOLYGON
    assert extension_type.coord_type == gt.CoordType.INTERLEAVED

    # extension name would be required for other levels of nesting
    with pytest.raises(ValueError, match="Can't compute extension name"):
        type_pyarrow._deserialize_storage(pa.list_(point))

    # If we manually specify the wrong extension name, this should error
    with pytest.raises(ValueError, match="GeometryType is overspecified"):
        type_pyarrow._deserialize_storage(point, "geoarrow.linestring")


def test_deserialize_infer_dimensions_separated():
    extension_type = type_pyarrow._deserialize_storage(
        pa.struct({d: pa.float64() for d in "xy"})
    )
    assert extension_type.dimensions == gt.Dimensions.XY

    extension_type = type_pyarrow._deserialize_storage(
        pa.struct({d: pa.float64() for d in "xyz"})
    )
    assert extension_type.dimensions == gt.Dimensions.XYZ

    extension_type = type_pyarrow._deserialize_storage(
        pa.struct({d: pa.float64() for d in "xym"})
    )
    assert extension_type.dimensions == gt.Dimensions.XYM

    extension_type = type_pyarrow._deserialize_storage(
        pa.struct({d: pa.float64() for d in "xyzm"})
    )
    assert extension_type.dimensions == gt.Dimensions.XYZM

    # Struct coordinates should never have dimensions inferred from number of children
    with pytest.raises(
        ValueError, match="Can't infer dimensions from coord field names"
    ):
        type_pyarrow._deserialize_storage(pa.struct({d: pa.float64() for d in "ab"}))


def test_deserialize_infer_dimensions_interleaved():
    extension_type = type_pyarrow._deserialize_storage(
        pa.list_(pa.float64(), list_size=2)
    )
    assert extension_type.dimensions == gt.Dimensions.XY

    extension_type = type_pyarrow._deserialize_storage(
        pa.list_(pa.float64(), list_size=4)
    )
    assert extension_type.dimensions == gt.Dimensions.XYZM

    # Fixed-size list should never have dimensions inferred where this would be
    # ambiguous.
    with pytest.raises(
        ValueError, match="Can't infer dimensions from coord field names"
    ):
        type_pyarrow._deserialize_storage(pa.list_(pa.float64(), list_size=3))

    # ...but this should be able to be specified using the field name
    extension_type = type_pyarrow._deserialize_storage(
        pa.list_(pa.field("xyz", pa.float64()), list_size=3)
    )
    assert extension_type.dimensions == gt.Dimensions.XYZ

    extension_type = type_pyarrow._deserialize_storage(
        pa.list_(pa.field("xym", pa.float64()), list_size=3)
    )
    assert extension_type.dimensions == gt.Dimensions.XYM

    # If the number of inferred dimensions does not match the number of actual dimensions,
    # this should error
    with pytest.raises(ValueError, match="Expected 4 dimensions but got"):
        type_pyarrow._deserialize_storage(
            pa.list_(pa.field("xyz", pa.float64()), list_size=4)
        )


def test_geometry_union_type():
    geometry = gt.type_spec(gt.Encoding.GEOARROW, gt.GeometryType.GEOMETRY).to_pyarrow()
    assert isinstance(geometry, type_pyarrow.GeometryUnionType)
    assert geometry.encoding == gt.Encoding.GEOARROW
    assert geometry.geometry_type == gt.GeometryType.GEOMETRY


def test_geometry_collection_union_type():
    geometry = gt.type_spec(
        gt.Encoding.GEOARROW, gt.GeometryType.GEOMETRYCOLLECTION
    ).to_pyarrow()
    assert isinstance(geometry, type_pyarrow.GeometryCollectionUnionType)
    assert geometry.encoding == gt.Encoding.GEOARROW
    assert geometry.geometry_type == gt.GeometryType.GEOMETRYCOLLECTION


def test_box_array_from_geobuffers():
    pa_type = gt.box(dimensions=gt.Dimensions.XY).to_pyarrow()
    arr = pa_type.from_geobuffers(
        b"\xff",
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0, 6.0]),
        np.array([7.0, 8.0, 9.0]),
        np.array([10.0, 11.0, 12.0]),
    )
    assert len(arr) == 3
    assert arr.type == pa_type
    assert arr.storage == pa.array(
        [
            {"xmin": 1.0, "ymin": 4.0, "xmax": 7.0, "ymax": 10.0},
            {"xmin": 2.0, "ymin": 5.0, "xmax": 8.0, "ymax": 11.0},
            {"xmin": 3.0, "ymin": 6.0, "xmax": 9.0, "ymax": 12.0},
        ],
        pa_type.storage_type,
    )


def test_point_array_from_geobuffers():
    pa_type = gt.point(dimensions=gt.Dimensions.XYZM).to_pyarrow()
    arr = pa_type.from_geobuffers(
        b"\xff",
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0, 6.0]),
        np.array([7.0, 8.0, 9.0]),
        np.array([10.0, 11.0, 12.0]),
    )
    assert len(arr) == 3
    assert arr.type == pa_type
    assert arr.storage == pa.array(
        [
            {"x": 1.0, "y": 4.0, "z": 7.0, "m": 10.0},
            {"x": 2.0, "y": 5.0, "z": 8.0, "m": 11.0},
            {"x": 3.0, "y": 6.0, "z": 9.0, "m": 12.0},
        ],
        pa_type.storage_type,
    )

    pa_type = gt.point(coord_type=gt.CoordType.INTERLEAVED).to_pyarrow()
    arr = pa_type.from_geobuffers(None, np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]))
    assert len(arr) == 3
    assert arr.storage == pa.array(
        [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], pa_type.storage_type
    )


@pytest.mark.parametrize(
    "pa_type", [gt.linestring().to_pyarrow(), gt.multipoint().to_pyarrow()]
)
def test_linestringish_array_from_geobuffers(pa_type):
    arr = pa_type.from_geobuffers(
        b"\xff",
        np.array([0, 4], np.int32),
        np.array([0.0, 1.0, 0.0, 0.0]),
        np.array([0.0, 0.0, 1.0, 0.0]),
    )
    assert len(arr) == 1
    assert arr.storage == pa.array(
        [
            [
                {"x": 0.0, "y": 0.0},
                {"x": 1.0, "y": 0.0},
                {"x": 0.0, "y": 1.0},
                {"x": 0.0, "y": 0.0},
            ]
        ],
        pa_type.storage_type,
    )


@pytest.mark.parametrize(
    "pa_type", [gt.polygon().to_pyarrow(), gt.multilinestring().to_pyarrow()]
)
def test_polygonish_array_from_geobuffers(pa_type):
    arr = pa_type.from_geobuffers(
        b"\xff",
        np.array([0, 1], np.int32),
        np.array([0, 4], np.int32),
        np.array([0.0, 1.0, 0.0, 0.0]),
        np.array([0.0, 0.0, 1.0, 0.0]),
    )
    assert len(arr) == 1
    assert arr.storage == pa.array(
        [
            [
                [
                    {"x": 0.0, "y": 0.0},
                    {"x": 1.0, "y": 0.0},
                    {"x": 0.0, "y": 1.0},
                    {"x": 0.0, "y": 0.0},
                ]
            ]
        ],
        pa_type.storage_type,
    )


def test_multipolygon_array_from_geobuffers():
    pa_type = gt.multipolygon().to_pyarrow()
    arr = pa_type.from_geobuffers(
        b"\xff",
        np.array([0, 1], np.int32),
        np.array([0, 1], np.int32),
        np.array([0, 4], np.int32),
        np.array([0.0, 1.0, 0.0, 0.0]),
        np.array([0.0, 0.0, 1.0, 0.0]),
    )
    assert len(arr) == 1
    assert arr.storage == pa.array(
        [
            [
                [
                    [
                        {"x": 0.0, "y": 0.0},
                        {"x": 1.0, "y": 0.0},
                        {"x": 0.0, "y": 1.0},
                        {"x": 0.0, "y": 0.0},
                    ]
                ]
            ]
        ],
        pa_type.storage_type,
    )


@pytest.mark.parametrize(
    "spec",
    [
        # Serialized types
        gt.wkt(),
        gt.large_wkt(),
        gt.wkb(),
        gt.large_wkb(),
        gt.wkt_view(),
        gt.wkb_view(),
        # Geometry types
        gt.box(),
        gt.point(),
        gt.linestring(),
        gt.polygon(),
        gt.multipoint(),
        gt.multilinestring(),
        gt.multipolygon(),
        # All dimensions, separated coords
        gt.point(dimensions="xy", coord_type="separated"),
        gt.point(dimensions="xyz", coord_type="separated"),
        gt.point(dimensions="xym", coord_type="separated"),
        gt.point(dimensions="xyzm", coord_type="separated"),
        # All dimensions, interleaved coords
        gt.point(dimensions="xy", coord_type="interleaved"),
        gt.point(dimensions="xyz", coord_type="interleaved"),
        gt.point(dimensions="xym", coord_type="interleaved"),
        gt.point(dimensions="xyzm", coord_type="interleaved"),
        # Box with all dimensions
        gt.box(dimensions="xy"),
        gt.box(dimensions="xyz"),
        gt.box(dimensions="xym"),
        gt.box(dimensions="xyzm"),
        # Union types
        gt.type_spec(gt.Encoding.GEOARROW, gt.GeometryType.GEOMETRY),
        gt.type_spec(gt.Encoding.GEOARROW, gt.GeometryType.GEOMETRYCOLLECTION),
    ],
)
def test_roundtrip_extension_type(spec):
    if not hasattr(pa, "binary_view") and spec.encoding in (
        gt.Encoding.WKB_VIEW,
        gt.Encoding.WKT_VIEW,
    ):
        pytest.skip("binary_view/string_view requires pyarrow >= 14")

    extension_type = type_pyarrow.extension_type(spec)
    serialized = extension_type.__arrow_ext_serialize__()
    extension_type2 = type_pyarrow._deserialize_storage(
        extension_type.storage_type, extension_type._extension_name, serialized
    )
    assert extension_type2 == extension_type


def test_register_extension_type():
    pa_version_tuple = tuple(int(component) for component in pa.__version__.split("."))
    if pa_version_tuple < (14,):
        pytest.skip("Can't test extension type registration pyarrow < 14")

    with type_pyarrow.registered_extension_types():
        schema_capsule = gt.point().to_pyarrow().__arrow_c_schema__()
        pa_type = pa.DataType._import_from_c_capsule(schema_capsule)
        assert isinstance(pa_type, type_pyarrow.GeometryExtensionType)

    with type_pyarrow.unregistered_extension_types():
        schema_capsule = gt.point().to_pyarrow().__arrow_c_schema__()
        pa_type = pa.DataType._import_from_c_capsule(schema_capsule)
        assert not isinstance(pa_type, type_pyarrow.GeometryExtensionType)
