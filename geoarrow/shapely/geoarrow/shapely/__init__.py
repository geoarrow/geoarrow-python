from .extension_array import PointArray
from .extension_types import (
    CoordinateDimension,
    LineStringType,
    MultiLineStringType,
    MultiPointType,
    MultiPolygonType,
    PointType,
    PolygonType,
    construct_geometry_array,
)


def register_geometry_extension_types():
    import pyarrow as pa

    for geom_type_class in [
        PointType,
        LineStringType,
        PolygonType,
        MultiPointType,
        MultiLineStringType,
        MultiPolygonType,
    ]:
        # Provide a default to go into the registry, but at runtime, we can choose other
        # type formulations
        geom_type_instance = geom_type_class(
            interleaved=True, dims=CoordinateDimension.XY
        )
        try:
            pa.register_extension_type(geom_type_instance)

        # If already registered with this id, unregister and re register
        except pa.ArrowKeyError:
            pa.unregister_extension_type(geom_type_instance.extension_name)
            pa.register_extension_type(geom_type_instance)


register_geometry_extension_types()
