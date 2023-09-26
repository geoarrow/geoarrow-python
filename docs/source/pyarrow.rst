
Integration with pyarrow
========================

.. automodule:: geoarrow.pyarrow

    Array constructors
    ------------------

    .. autofunction:: array

    Dataset constructors
    --------------------

    .. autofunction:: dataset

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

    Class Reference
    ---------------

    .. autoclass:: VectorType
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

    .. autoclass:: geoarrow.pyarrow._dataset.GeoDataset
        :members:

    .. autoclass:: geoarrow.pyarrow._dataset.ParquetRowGroupGeoDataset
        :members:
