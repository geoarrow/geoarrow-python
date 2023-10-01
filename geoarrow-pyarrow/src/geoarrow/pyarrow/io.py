import geoarrow.pyarrow as _ga


def read_pyogrio_table(*args, **kwargs):
    from pyogrio.raw import read_arrow
    import pyproj

    meta, table = read_arrow(*args, **kwargs)

    # Maybe not always true? meta["geometry_name"] is occasionally `""`
    geometry_name = meta["geometry_name"] if meta["geometry_name"] else "wkb_geometry"

    # Create the geoarrow-enabled geometry column with a crs attribute
    prj = pyproj.CRS(meta["crs"])
    geoarrow_type = _ga.wkb().with_crs(prj.to_json())
    geometry = geoarrow_type.wrap_array(table.column(geometry_name))

    # Apply CRS to geometry column. This doesn't scale to multiple geometry
    # columns, but it's unclear if other columns would share the same CRS.
    for i, nm in enumerate(table.column_names):
        if nm == geometry_name:
            geometry = table.column(i)
            geometry = geoarrow_type.wrap_array(geometry)
            table = table.set_column(i, nm, geometry)
            break

    return table
