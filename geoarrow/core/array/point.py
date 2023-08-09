from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    import geoarrow.geo
    import geoarrow.geos
    import geoarrow.proj


@dataclass
class PointArray:

    arr: pa.Array

    def __post_init__(self):
        validate_point_array(self.arr)

    @property
    def geo(self) -> geoarrow.geo.PointArray:
        """Access GeoRust algorithms on this PointArray"""
        import geoarrow.geo

        return geoarrow.geo.PointArray.from_pyarrow(self.arr)

    @property
    def geos(self) -> geoarrow.geos.PointArray:
        """Access GEOS algorithms on this PointArray"""
        import geoarrow.geos

        return geoarrow.geos.PointArray.from_pyarrow(self.arr)

    @property
    def proj(self) -> geoarrow.proj.PointArray:
        """Access Proj algorithms on this PointArray"""
        import geoarrow.proj

        return geoarrow.proj.PointArray.from_pyarrow(self.arr)


def validate_point_array(arr: pa.Array):
    """Validate that this pyarrow Array is a valid GeoArrow Point array
    """
    pass
