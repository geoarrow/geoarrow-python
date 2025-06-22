from __future__ import annotations

import numpy as np
import pyarrow as pa

import shapely
from shapely import GeometryType

# TODO: change the Python repr of the pyarrow scalar value so that it doesn't call as_py
# and create a new GEOS object every time it prints the scalar?

# TODO: support separated coords; right now it assumes interleaved

# TODO: add tests where the selected scalar is _not_ the first polygon. The offsets are
# incorrect when not the first polygon.


class Point(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.Point:
        return self.as_py()

    def as_py(self) -> shapely.Point:
        coords = self.value.values.to_numpy().reshape(
            -1, len(self.type.coord_dimension)
        )
        geoms = shapely.from_ragged_array(
            GeometryType.POINT,
            coords,
            None,
        )
        assert len(geoms) == 1
        return geoms[0]


class LineString(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.LineString:
        return self.as_py()

    def as_py(self) -> shapely.LineString:
        coords = (
            self.value.values.flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = np.array([0, coords.shape[0]], dtype=np.int32)
        geoms = shapely.from_ragged_array(
            GeometryType.LINESTRING,
            coords,
            (geom_offsets,),
        )
        assert len(geoms) == 1
        return geoms[0]


class Polygon(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.Polygon:
        return self.as_py()

    def as_py(self) -> shapely.Polygon:
        coords = (
            self.value.values.flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        ring_offsets = self.value.values.offsets
        geom_offsets = np.array([0, 1], dtype=np.int32)
        geoms = shapely.from_ragged_array(
            GeometryType.POLYGON,
            coords,
            (ring_offsets, geom_offsets),
        )
        assert len(geoms) == 1
        return geoms[0]


class MultiPoint(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.MultiPoint:
        return self.as_py()

    def as_py(self) -> shapely.MultiPoint:
        coords = (
            self.value.values.flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = np.array([0, coords.shape[0]], dtype=np.int32)
        geoms = shapely.from_ragged_array(
            GeometryType.MULTIPOINT,
            coords,
            (geom_offsets,),
        )
        assert len(geoms) == 1
        return geoms[0]


class MultiLineString(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.MultiLineString:
        return self.as_py()

    def as_py(self) -> shapely.MultiLineString:
        coords = (
            self.value.values.flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        ring_offsets = self.value.values.offsets
        geom_offsets = np.array([0, 1], dtype=np.int32)
        geoms = shapely.from_ragged_array(
            GeometryType.MULTILINESTRING,
            coords,
            (ring_offsets, geom_offsets),
        )
        assert len(geoms) == 1
        return geoms[0]


class MultiPolygon(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.MultiPolygon:
        return self.as_py()

    def as_py(self) -> shapely.MultiPolygon:
        coords = (
            self.value.values.flatten()
            .flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        polygon_offsets = self.value.values.offsets
        ring_offsets = self.value.values.flatten().offsets
        geom_offsets = np.array([0, 1], dtype=np.int32)
        geoms = shapely.from_ragged_array(
            GeometryType.MULTIPOLYGON,
            coords,
            (
                ring_offsets,
                polygon_offsets,
                geom_offsets,
            ),
        )
        assert len(geoms) == 1
        return geoms[0]
