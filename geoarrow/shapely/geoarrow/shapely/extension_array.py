from __future__ import annotations

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

import shapely
from shapely import GeometryType


class PointArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""
        flat_coords = (
            self.storage.flatten()
            .to_numpy()
            .reshape(-1, len(self.type.coord_dimension))
        )
        return shapely.from_ragged_array(GeometryType.POINT, flat_coords, None)


class LineStringArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""

        # TODO: shapely fails on version 2.0.1 with a read-only coords buffer, so we
        # make a copy here by setting writable=True.
        # ValueError: buffer source array is read-only
        flat_coords = (
            self.storage.flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = self.storage.offsets.to_numpy()
        return shapely.from_ragged_array(
            GeometryType.LINESTRING, flat_coords, (geom_offsets,)
        )


class PolygonArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""

        # TODO: shapely fails on version 2.0.1 with a read-only coords buffer, so we
        # make a copy here by setting writable=True.
        # ValueError: buffer source array is read-only
        flat_coords = (
            self.storage.flatten()
            .flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = self.storage.offsets.to_numpy()
        ring_offsets = self.storage.flatten().offsets.to_numpy()
        return shapely.from_ragged_array(
            GeometryType.POLYGON, flat_coords, (ring_offsets, geom_offsets)
        )


class MultiPointArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""

        # TODO: shapely fails on version 2.0.1 with a read-only coords buffer, so we
        # make a copy here by setting writable=True.
        # ValueError: buffer source array is read-only
        flat_coords = (
            self.storage.flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = self.storage.offsets.to_numpy()
        return shapely.from_ragged_array(
            GeometryType.MULTIPOINT, flat_coords, (geom_offsets,)
        )


class MultiLineStringArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""

        # TODO: shapely fails on version 2.0.1 with a read-only coords buffer, so we
        # make a copy here by setting writable=True.
        # ValueError: buffer source array is read-only
        flat_coords = (
            self.storage.flatten()
            .flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = self.storage.offsets.to_numpy()
        ring_offsets = self.storage.flatten().offsets.to_numpy()
        return shapely.from_ragged_array(
            GeometryType.MULTILINESTRING, flat_coords, (ring_offsets, geom_offsets)
        )


class MultiPolygonArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""

        # TODO: shapely fails on version 2.0.1 with a read-only coords buffer, so we
        # make a copy here by setting writable=True.
        # ValueError: buffer source array is read-only
        flat_coords = (
            self.storage.flatten()
            .flatten()
            .flatten()
            .flatten()
            .to_numpy(zero_copy_only=False, writable=True)
            .reshape(-1, len(self.type.coord_dimension))
        )
        geom_offsets = self.storage.offsets.to_numpy()
        polygon_offsets = self.storage.flatten().offsets.to_numpy()
        ring_offsets = self.storage.flatten().flatten().offsets.to_numpy()
        return shapely.from_ragged_array(
            GeometryType.MULTIPOLYGON,
            flat_coords,
            (ring_offsets, polygon_offsets, geom_offsets),
        )
