from typing import List, Optional

import geopandas as gpd
import numpy as np
import pyarrow as pa

from geoarrow.shapely.extension_types import construct_geometry_array


def geopandas_to_geoarrow(
    gdf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    preserve_index: Optional[bool] = None,
):
    df_attr = gdf.drop(columns=[gdf._geometry_column_name])

    if columns is not None:
        df_attr = df_attr[columns]

    table = pa.Table.from_pandas(df_attr, preserve_index=preserve_index)
    geom_arr = construct_geometry_array(np.array(gdf.geometry))
    return table.append_column("geometry", geom_arr)
