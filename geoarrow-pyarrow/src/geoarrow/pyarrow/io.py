import geoarrow.pyarrow as _ga


def read_pyogrio_table(*args, **kwargs):
    from pyogrio.raw import read_arrow
    import pyproj

    meta, table = read_arrow(*args, **kwargs)
    # Maybe not always true? meta["geometry_name"] is occasionally empty ""
    geometry_name = meta["geometry_name"] if meta["geometry_name"] else "wkb_geometry"

    # Create the geoarrow-enabled geometry column
    prj = pyproj.CRS(meta["crs"])
    geometry_type = _ga.wkb().with_crs(prj.to_json())
    geometry = geometry_type.wrap_array(table.column(geometry_name))

    # Move geometry column to the end and name it 'geometry'
    for i, nm in enumerate(table.column_names):
        if nm == geometry_name:
            table = table.remove_column(i)
            break

    return table.append_column("geometry", geometry)
