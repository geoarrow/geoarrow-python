
geoarrow-pyarrow
================

.. automodule:: geoarrow.pyarrow

    Array constructors
    ------------------

    .. autofunction:: array

    Type Constructors
    -----------------

    .. autofunction:: wkb

    .. autofunction:: wkt

    .. autofunction:: large_wkb

    .. autofunction:: large_wkt

    .. autofunction:: point

    .. autofunction:: linestring

    .. autofunction:: polygon

    .. autofunction:: multipoint

    .. autofunction:: multilinestring

    .. autofunction:: multipolygon

    Compute functions
    -----------------

    .. autofunction:: parse_all

    .. autofunction:: unique_geometry_types

    .. autofunction:: infer_type_common

    .. autofunction:: as_wkt

    .. autofunction:: as_wkb

    .. autofunction:: as_geoarrow

    .. autofunction:: format_wkt

    .. autofunction:: box

    .. autofunction:: box_agg

    .. autofunction:: rechunk

    .. autofunction:: with_coord_type

    .. autofunction:: with_edge_type

    .. autofunction:: with_crs

    .. autofunction:: with_dimensions

    .. autofunction:: with_geometry_type

    .. autofunction:: point_coords

    .. autofunction:: to_geopandas

    Class Reference
    ---------------

    .. autoclass:: GeometryExtensionType
        :members:

    .. autoclass:: WkbType
        :members:

    .. autoclass:: WktType
        :members:

    .. autoclass:: PointType
        :members:

    .. autoclass:: LinestringType
        :members:

    .. autoclass:: PolygonType
        :members:

    .. autoclass:: MultiPointType
        :members:

    .. autoclass:: MultiLinestringType
        :members:

    .. autoclass:: MultiPolygonType
        :members:

IO helpers
--------------------

.. automodule:: geoarrow.pyarrow.io

    .. autofunction:: read_pyogrio_table

    .. autofunction:: read_geoparquet_table

    .. autofunction:: write_geoparquet_table


Dataset constructors
--------------------

.. automodule:: geoarrow.pyarrow.dataset

    .. autofunction:: dataset

    .. autoclass:: geoarrow.pyarrow.dataset.GeoDataset
        :members:

    .. autoclass:: geoarrow.pyarrow.dataset.ParquetRowGroupGeoDataset
        :members:
