# geoarrow for Python

The geoarrow Python packages provide an implementation of the [GeoArrow specification](https://github.com/geoarrow/geoarrow) that integrations with [pyarrow](https://arrow.apache.org/docs/python) and [pandas](https://pandas.pydata.org/). The geoarrow Python bindings provide input/output to/from Arrow-friendly formats (e.g., Parquet, Arrow Stream, Arrow File) and general-purpose coordinate shuffling tools among GeoArrow, WKT, and WKB encodings. 

## Installation

Python bindings for geoarrow are not yet available on PyPI. You can install via URL:

```bash
python -m pip install "git+https://github.com/geoarrow/geoarrow-python.git#egg=geoarrow-pyarrow&subdirectory=geoarrow-pyarrow"
python -m pip install "git+https://github.com/geoarrow/geoarrow-python.git#egg=geoarrow-pandas&subdirectory=geoarrow-pandas"
```

If you can import the namespace, you're good to go! The most user-friendly interface to geoarrow currently depends on `pyarrow`, which you can import with:


```python
import geoarrow.pyarrow as ga
```

## Examples

You can create geoarrow-encoded arrays with `as_geoarrow()`:


```python
ga.as_geoarrow(["POINT (0 1)"])
```




    PointArray:PointType(geoarrow.point)[1]
    <POINT (0 1)>



This will work with:

- An existing array created by geoarrow
- A `geopandas.GeoSeries`
- A `pyarrow.Array` or `pyarrow.ChunkedArray` (geoarrow text interpreted as well-known text; binary interpreted as well-known binary)
- Anything that `pyarrow.array()` will convert to a text or binary array

If there is no common geometry type among elements of the input, `as_geoarrow()` will fall back to well-known binary encoding. To explicitly convert to well-known text or binary, use `as_wkt()` or `as_wkb()`.

Alternatively, you can construct GeoArrow arrays directly from a series of buffers as described in the specification:


```python
import numpy as np

ga.point().from_geobuffers(
    None, 
    np.array([1.0, 2.0, 3.0]),
    np.array([3.0, 4.0, 5.0])
)
```




    PointArray:PointType(geoarrow.point)[3]
    <POINT (1 3)>
    <POINT (2 4)>
    <POINT (3 5)>




```python
ga.point().with_coord_type(ga.CoordType.INTERLEAVED).from_geobuffers(
    None,
    np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
)
```




    PointArray:PointType(interleaved geoarrow.point)[3]
    <POINT (1 2)>
    <POINT (3 4)>
    <POINT (5 6)>



Importing `geoarrow.pyarrow` will register the geoarrow extension types with pyarrow such that you can read/write Arrow streams, Arrow files, and Parquet that contains Geoarrow extension types. A number of these files are available from the [geoarrow-data](https://github.com/geoarrow/geoarrow-data) repository.


```python
import urllib.request
import pyarrow.parquet as pq

url = "https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/ns-water-basin_line.parquet"
local_filename, headers = urllib.request.urlretrieve(url)
pq.read_table(local_filename).schema
```




    OBJECTID: int64
    FEAT_CODE: string
    LINE_CLASS: int32
    MISCID_1: string
    MISCNAME_1: string
    MISCID_2: string
    MISCNAME_2: string
    HID: string
    MISCID_3: string
    MISCNAME_3: string
    MISCID_4: string
    MISCNAME_4: string
    SHAPE_LEN: double
    geometry: extension<geoarrow.multilinestring<MultiLinestringType>>
    -- schema metadata --
    geo: '
        {
        "columns": {
            "geometry": {
            "encoding": "' + 2919




```python
import geopandas

url = "https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/ns-water-basin_line.gpkg"
df = geopandas.read_file(url)
array = ga.as_geoarrow(df.geometry)
array
```




    MultiLinestringArray:MultiLinestringType(geoarrow.multilinestring <{"$schema":"https://proj.org/schem...>)[255]
    <MULTILINESTRING ((648686.0197000001 5099181.984099999, 648626.018...>
    <MULTILINESTRING ((687687.8200000003 5117029.181600001, 686766.020...>
    <MULTILINESTRING ((631355.5193999996 5122892.2849, 631364.34339999...>
    <MULTILINESTRING ((665166.0199999996 5138641.9825, 665146.01999999...>
    <MULTILINESTRING ((673606.0199999996 5162961.9823, 673606.01999999...>
    ...245 values...
    <MULTILINESTRING ((681672.6200000001 5078601.5823, 681866.01999999...>
    <MULTILINESTRING ((414867.91700000037 5093040.8807, 414793.8169999...>
    <MULTILINESTRING ((414867.91700000037 5093040.8807, 414829.7170000...>
    <MULTILINESTRING ((414867.91700000037 5093040.8807, 414937.2170000...>
    <MULTILINESTRING ((648686.0197000001 5099181.984099999, 648866.019...>



You can convert back to geopandas using `to_geopandas()`:


```python
ga.to_geopandas(array)
```




    0      MULTILINESTRING ((648686.020 5099181.984, 6486...
    1      MULTILINESTRING ((687687.820 5117029.182, 6867...
    2      MULTILINESTRING ((631355.519 5122892.285, 6313...
    3      MULTILINESTRING ((665166.020 5138641.982, 6651...
    4      MULTILINESTRING ((673606.020 5162961.982, 6736...
                                 ...                        
    250    MULTILINESTRING ((681672.620 5078601.582, 6818...
    251    MULTILINESTRING ((414867.917 5093040.881, 4147...
    252    MULTILINESTRING ((414867.917 5093040.881, 4148...
    253    MULTILINESTRING ((414867.917 5093040.881, 4149...
    254    MULTILINESTRING ((648686.020 5099181.984, 6488...
    Length: 255, dtype: geometry



## Pandas integration

The `geoarrow-pandas` package provides an extension array that wraps geoarrow memory and an accessor that provides pandas-friendly wrappers around the compute functions available in `geoarrow.pyarrow`.


```python
import geoarrow.pandas as _
import pandas as pd

df = pd.read_parquet("https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/ns-water-basin_point.parquet")
df.geometry.geoarrow.format_wkt().head(5)
```




    0     POINT (277022.6936181751 4820886.609673489)
    1     POINT (315701.2552756762 4855051.378571571)
    2    POINT (255728.65994492616 4851022.107901295)
    3     POINT (245206.7841665779 4895609.409696873)
    4    POINT (337143.18135472975 4860312.288760258)
    dtype: string[pyarrow]



## Building

Python bindings for geoarrow are managed with [setuptools](https://setuptools.pypa.io/en/latest/index.html).
This means you can build the project using:

```shell
git clone https://github.com/geoarrow/geoarrow-python.git
pip install -e geoarrow-pyarrow/ geoarrow-pandas/
```

Tests use [pytest](https://docs.pytest.org/):

```shell
pytest
```
