import pyarrow as pa
import pytest

import geoarrow.types as gt
from geoarrow.types import type_pyarrow


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


@pytest.mark.parametrize(
    "spec",
    [
        # Serialized types
        gt.wkt(),
        gt.large_wkt(),
        gt.wkb(),
        gt.large_wkb(),
        # Geometry types
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
    ],
)
def test_roundtrip_extension_type(spec):
    extension_type = type_pyarrow.extension_type(spec)
    serialized = extension_type.__arrow_ext_serialize__()
    extension_type2 = type_pyarrow._deserialize_storage(
        extension_type.storage_type, extension_type._extension_name, serialized
    )
    assert extension_type2 == extension_type
