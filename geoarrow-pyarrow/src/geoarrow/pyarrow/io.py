"""GeoArrow IO helpers

A module wrapping IO functionality from external libraries to simplify
testing and documenting the GeoArrow format and encodings.

>>> from geoarrow.pyarrow import io
"""

import geoarrow.pyarrow as _ga


def read_pyogrio_table(
    *args,
    geoarrow_type=None,
    geoarrow_coord_type=None,
    geoarrow_promote_multi=False,
    **kwargs
):
    """Read a file using GDAL/OGR

    Reads a file as a ``pyarrow.Table`` using ``pyogrio.raw.read_arrow()``,
    applying :func:`geoarrow.pyarrow.as_geoarrow` to the geometry column.
    This aggressively parses the geometry column into a geoarrow-native
    encoding: to prevent this, use ``geoarrow_type=geoarrow.pyarrow.wkb()``.

    >>> from geoarrow.pyarrow import io
    >>> import tempfile
    >>> import geopandas
    >>> import os
    >>> with tempfile.TemporaryDirectory() as tmpdir:
    ...     temp_gpkg = os.path.join(tmpdir, "test.gpkg")
    ...     geopandas.GeoDataFrame(
    ...         geometry=geopandas.GeoSeries.from_wkt(["POINT (0 1)"],
    ...         crs="OGC:CRS84")
    ...     ).to_file(temp_gpkg)
    ...     io.read_pyogrio_table(temp_gpkg)
    pyarrow.Table
    geom: extension<geoarrow.point<PointType>>
    ----
    geom: [  -- is_valid: all not null
      -- child 0 type: double
    [0]
      -- child 1 type: double
    [1]]

    """
    from pyogrio.raw import read_arrow
    import pyproj

    meta, table = read_arrow(*args, **kwargs)

    # Maybe not always true? meta["geometry_name"] is occasionally `""`
    geometry_name = meta["geometry_name"] if meta["geometry_name"] else "wkb_geometry"

    # Create the geoarrow-enabled geometry column with a crs attribute
    prj = pyproj.CRS(meta["crs"])

    # Apply geoarrow type to geometry column. This doesn't scale to multiple geometry
    # columns, but it's unclear if other columns would share the same CRS.
    for i, nm in enumerate(table.column_names):
        if nm == geometry_name:
            geometry = table.column(i)
            geometry = _ga.wkb().wrap_array(geometry)
            geometry = _ga.as_geoarrow(
                geometry,
                type=geoarrow_type,
                coord_type=geoarrow_coord_type,
                promote_multi=geoarrow_promote_multi,
            )
            table = table.set_column(i, nm, _ga.with_crs(geometry, prj.to_json()))
            break

    return table
