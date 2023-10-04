"""GeoArrow IO helpers

A module wrapping IO functionality from external libraries to simplify
testing and documenting the GeoArrow format and encodings.

>>> from geoarrow.pyarrow import io
"""

import geoarrow.pyarrow as _ga


def read_pyogrio_table(*args, **kwargs):
    """Read a file using GDAL/OGR

    Reads a file as a ``pyarrow.Table`` using ``pyogrio.raw.read_arrow()``.
    This does not parse the input, which OGR returns as
    :func:`geoarrow.pyarrow.wkb`.

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
    ...     table = io.read_pyogrio_table(temp_gpkg)
    ...     table.column("geom").chunk(0)
    GeometryExtensionArray:WkbType(geoarrow.wkb <{"$schema":"https://proj.org/schem...>)[1]
    <POINT (0 1)>
    """
    from pyogrio.raw import read_arrow
    import pyproj

    meta, table = read_arrow(*args, **kwargs)

    # Maybe not always true? meta["geometry_name"] is occasionally `""`
    geometry_name = meta["geometry_name"] if meta["geometry_name"] else "wkb_geometry"

    # Get the JSON representation of the CRS
    prj_as_json = pyproj.CRS(meta["crs"]).to_json()

    # Apply geoarrow type to geometry column. This doesn't scale to multiple geometry
    # columns, but it's unclear if other columns would share the same CRS.
    for i, nm in enumerate(table.column_names):
        if nm == geometry_name:
            geometry = table.column(i)
            geometry = _ga.wkb().wrap_array(geometry)
            geometry = _ga.with_crs(geometry, prj_as_json)
            table = table.set_column(i, nm, geometry)
            break

    return table
