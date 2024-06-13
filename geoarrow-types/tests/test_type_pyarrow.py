import pyarrow as pa

import geoarrow.types as gt
from geoarrow.types import type_pyarrow


def test_classes_serialized():
    wkt = gt.wkt().to_pyarrow()
    assert isinstance(wkt, type_pyarrow.WktType)
    assert wkt.encoding == gt.Encoding.WKT

    wkb = gt.wkb().to_pyarrow()
    assert isinstance(wkb, type_pyarrow.WkbType)
    assert wkb.encoding == gt.Encoding.WKB


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
