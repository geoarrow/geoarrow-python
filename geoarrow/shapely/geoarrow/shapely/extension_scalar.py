from __future__ import annotations

import pyarrow as pa

import shapely
from shapely import GeometryType


# TODO: change the Python repr of the pyarrow scalar value so that it doesn't call as_py
# and create a new GEOS object every time it prints the scalar?

class PointScalar(pa.ExtensionScalar):
    def to_shapely(self) -> shapely.Point:
        return self.as_py()

    def as_py(self) -> shapely.Point:
        geoms = shapely.from_ragged_array(
            GeometryType.POINT,
            self.value.values.to_numpy().reshape(-1, len(self.type.coord_dimension)),
            None,
        )
        assert len(geoms) == 1
        return geoms[0]
