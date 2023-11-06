import json
import pyarrow.parquet as _pq
import pyarrow.types as _types
import geoarrow.pyarrow as _ga


def _guess_geometry_columns(schema):
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


def _chunked_array_to_geoarrow(item, spec):
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
        crs = "OGC:CRS84"
    elif isinstance(spec["crs"], dict):
        crs = json.dumps(spec["crs"])
    else:
        crs = spec["crs"]

    if crs is not None:
        return _ga.with_crs(crs)

    return item


def _table_geometry_columns_to_geoarrow(tab, columns):
    for col_name, spec in columns.items():
        # col_name might not exist if columns was passed
        col_i = tab.schema.get_field_index(col_name)
        new_geometry = _chunked_array_to_geoarrow(tab[col_i], spec)
        tab = tab.set_column(col_i, col_name, new_geometry)

    return tab


def read_geoparquet_table(*args, **kwargs):
    tab = _pq.read_table(*args, **kwargs)
    if b"geo" not in tab.schema.metadata:
        return tab

    geo_meta = json.loads(tab.schema.metadata[b"geo"])

    # Remove "geo" schema metadata key since after this transformation
    # it will no longer contain valid encodings
    non_geo_meta = {k: v for k, v in tab.schema.metadata if k != b"geo"}
    tab = tab.replace_schema_metadata(non_geo_meta)

    # Assign extension types to columns
    if "columns" in geo_meta:
        columns = geo_meta["columns"]
    else:
        columns = _guess_geometry_columns(tab.schema)

    return _table_geometry_columns_to_geoarrow(tab, columns)


def write_geoparquet(obj, *args, **kwargs):
    pass
