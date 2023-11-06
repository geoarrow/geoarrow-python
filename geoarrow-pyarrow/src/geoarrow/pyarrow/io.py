"""GeoArrow IO helpers

A module wrapping IO functionality from external libraries to simplify
testing and documenting the GeoArrow format and encodings.

>>> from geoarrow.pyarrow import io
"""

import json
import pyarrow.parquet as _pq
import pyarrow.types as _types
import geoarrow.pyarrow as _ga
from geoarrow.pyarrow._compute import ensure_storage


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


def _geoparquet_guess_geometry_columns(schema):
    columns = {}

    # Only attempt guessing the "geometry" column
    if "geometry" in schema.names:
        type = schema.field("geometry").type
        if _types.is_binary(type) or _types.is_large_binary(type):
            columns["geometry"] = {"encoding": "WKB"}
        elif _types.is_string(type) or _types.is_large_string(type):
            # WKT is not actually a geoparquet encoding but the guidance on
            # putting geospatial things in parquet without metadata says you
            # can do it and this is the internal sentinel for that case.
            columns["geometry"] = {"encoding": "WKT"}

    return columns


def _geoparquet_chunked_array_to_geoarrow(item, spec):
    # If item was written as a GeoArrow extension type to the Parquet file,
    # ignore any information in the column spec
    if isinstance(item.type, _ga.GeometryExtensionType):
        return item

    if "encoding" not in spec:
        raise ValueError("Invalid GeoParquet column specification: missing 'encoding'")

    encoding = spec["encoding"]
    if encoding in ("WKB", "WKT"):
        item = _ga.array(item)
    else:
        raise ValueError(f"Invalid GeoParquet encoding value: '{encoding}'")

    if "crs" not in spec:
        crs = _CRS_LONLAT
    else:
        crs = spec["crs"]

    if "edges" not in spec or spec["edges"] == "planar":
        edge_type = None
    elif spec["edges"] == "spherical":
        edge_type = _ga.EdgeType.SPHERICAL
    else:
        raise ValueError("Invalid GeoParuqet edges value")

    if crs is not None:
        item = _ga.with_crs(item, crs)

    if edge_type is not None:
        item = _ga.with_edge_type(item, edge_type)

    return item


def _geoparquet_table_to_geoarrow(tab, columns):
    tab_names = set(tab.schema.names)
    for col_name, spec in columns.items():
        # col_name might not exist if only a subset of columns were read from file
        if col_name not in tab_names:
            continue

        col_i = tab.schema.get_field_index(col_name)
        new_geometry = _geoparquet_chunked_array_to_geoarrow(tab[col_i], spec)
        tab = tab.set_column(col_i, col_name, new_geometry)

    return tab


def read_geoparquet_table(*args, **kwargs):
    tab = _pq.read_table(*args, **kwargs)
    tab_metadata = tab.schema.metadata if tab.schema.metadata else {}
    if b"geo" in tab_metadata:
        geo_meta = json.loads(tab_metadata[b"geo"])
    else:
        geo_meta = {}

    # Remove "geo" schema metadata key since after this transformation
    # it will no longer contain valid encodings
    non_geo_meta = {k: v for k, v in tab_metadata.items() if k != b"geo"}
    tab = tab.replace_schema_metadata(non_geo_meta)

    # Assign extension types to columns
    if "columns" in geo_meta:
        columns = geo_meta["columns"]
    else:
        columns = _geoparquet_guess_geometry_columns(tab.schema)

    return _geoparquet_table_to_geoarrow(tab, columns)


def _geoparquet_guess_primary_geometry_column(schema, primary_geometry_column=None):
    if primary_geometry_column is not None:
        return primary_geometry_column

    # If there's a "geometry" column, pick that one
    if "geometry" in schema.names:
        return "geometry"

    # Otherwise, pick the first thing we know is actually geometry
    for name, type in zip(schema.names, schema.types):
        if isinstance(type, _ga.GeometryExtensionType):
            return name

    raise ValueError(
        "write_geoparquet_table() requires source with at least one geometry column"
    )


def _geoparquet_column_spec_from_type(type):
    # We always encode to WKB since it's the only supported value
    spec = {"encoding": "WKB", "geometry_types": []}

    # Pass along extra information from GeoArrow extension type metadata
    if isinstance(type, _ga.GeometryExtensionType):
        if type.crs_type == _ga.CrsType.PROJJSON:
            spec["crs"] = json.loads(type.crs)
        elif type.crs_type == _ga.CrsType.NONE:
            spec["crs"] = None
        else:
            import pyproj

            spec["crs"] = pyproj.CRS(type.crs).to_json_dict()

        if type.edge_type == _ga.EdgeType.SPHERICAL:
            spec["edges"] = "spherical"

        # GeoArrow-encoded types can confidently declare a single geometry type
        if type.geometry_type == _ga.GeometryType.POINT:
            spec["geometry_types"] = ["Point"]
        elif type.geometry_type == _ga.GeometryType.LINESTRING:
            spec["geometry_types"] = ["LineString"]
        elif type.geometry_type == _ga.GeometryType.POLYGON:
            spec["geometry_types"] = ["Polygon"]
        elif type.geometry_type == _ga.GeometryType.MULTIPOINT:
            spec["geometry_types"] = ["MultiPoint"]
        elif type.geometry_type == _ga.GeometryType.MULTILINESTRING:
            spec["geometry_types"] = ["MultiLineString"]
        elif type.geometry_type == _ga.GeometryType.MULTIPOLYGON:
            spec["geometry_types"] = ["MultiPolygon"]

    return spec


def _geoparquet_columns_from_schema(
    schema, geometry_columns=None, primary_geometry_column=None
):
    schema_names = schema.names
    schema_types = schema.types

    if geometry_columns is None:
        geometry_columns = set()
        if primary_geometry_column is not None:
            geometry_columns.add(primary_geometry_column)

        for name, type in zip(schema_names, schema_types):
            if isinstance(type, _ga.GeometryExtensionType):
                geometry_columns.add(name)
    else:
        geometry_columns = set(geometry_columns)

    specs = {}
    for name, type in zip(schema_names, schema_types):
        if name in geometry_columns:
            specs[name] = _geoparquet_column_spec_from_type(type)

    return specs


def _geoparquet_metadata_from_schema(
    schema, geometry_columns=None, primary_geometry_column=None
):
    primary_geometry_column = _geoparquet_guess_primary_geometry_column(
        schema, primary_geometry_column
    )
    columns = _geoparquet_columns_from_schema(schema, geometry_columns)
    return {
        "version": "1.0.0",
        "primary_column": primary_geometry_column,
        "columns": columns,
    }


def _geoparquet_encode_chunked_array(item, spec):
    # ...because we're currently only ever encoding using WKB
    item = _ga.as_wkb(item)
    return ensure_storage(item)


def write_geoparquet_table(
    table, *args, primary_geometry_column=None, geometry_columns=None, **kwargs
):
    geo_meta = _geoparquet_metadata_from_schema(
        table.schema,
        primary_geometry_column=primary_geometry_column,
        geometry_columns=geometry_columns,
    )
    for i, name in enumerate(table.schema.names):
        if name in geo_meta["columns"]:
            table = table.set_column(
                i,
                name,
                _geoparquet_encode_chunked_array(table[i], geo_meta["columns"][name]),
            )

    metadata = table.schema.metadata if table.schema.metadata else {}
    metadata["geo"] = json.dumps(geo_meta)
    table = table.replace_schema_metadata(metadata)
    return _pq.write_table(table, *args, **kwargs)


_CRS_LONLAT = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
            {
                "name": "World Geodetic System 1984 (Transit)",
                "id": {"authority": "EPSG", "code": 1166},
            },
            {
                "name": "World Geodetic System 1984 (G730)",
                "id": {"authority": "EPSG", "code": 1152},
            },
            {
                "name": "World Geodetic System 1984 (G873)",
                "id": {"authority": "EPSG", "code": 1153},
            },
            {
                "name": "World Geodetic System 1984 (G1150)",
                "id": {"authority": "EPSG", "code": 1154},
            },
            {
                "name": "World Geodetic System 1984 (G1674)",
                "id": {"authority": "EPSG", "code": 1155},
            },
            {
                "name": "World Geodetic System 1984 (G1762)",
                "id": {"authority": "EPSG", "code": 1156},
            },
            {
                "name": "World Geodetic System 1984 (G2139)",
                "id": {"authority": "EPSG", "code": 1309},
            },
        ],
        "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
        },
        "accuracy": "2.0",
        "id": {"authority": "EPSG", "code": 6326},
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "scope": "Not known.",
    "area": "World.",
    "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180,
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}
