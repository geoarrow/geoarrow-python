from __future__ import annotations

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

import shapely
from shapely import GeometryType


class PointArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""
        coord_dimension = self.type.coord_dimension
        flat_coords = (
            self.storage.flatten()
            .to_numpy(zero_copy_only=True, writable=False)
            .reshape(-1, len(coord_dimension))
        )
        return shapely.from_ragged_array(GeometryType.POINT, flat_coords, None)

class LineStringArray(pa.ExtensionArray):
    def to_shapely(self) -> NDArray[np.object_]:
        """Convert to an array of shapely geometries"""
        coord_dimension = self.type.coord_dimension
        flat_coords = (
            self.storage.flatten()
            .to_numpy(zero_copy_only=True, writable=False)
            .reshape(-1, len(coord_dimension))
        )
        return shapely.from_ragged_array(GeometryType.POINT, flat_coords, None)
