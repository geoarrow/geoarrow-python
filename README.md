# GeoArrow for Python

The GeoArrow Python packages provide an implementation of the [GeoArrow specification](https://github.com/geoarrow/geoarrow) that integrates with [pyarrow](https://arrow.apache.org/docs/python) and [pandas](https://pandas.pydata.org/). The GeoArrow Python bindings enable input/output to/from Arrow-friendly formats (e.g., Parquet, Arrow Stream, Arrow File) and general-purpose coordinate shuffling tools among GeoArrow, WKT, and WKB encodings.

## Installation

Python bindings for GeoArrow are available on PyPI. You can install them with:

```bash
pip install geoarrow-pyarrow geoarrow-pandas
```

You can install the latest development versions with:

```bash
pip install "git+https://github.com/geoarrow/geoarrow-python.git#egg=geoarrow-pyarrow&subdirectory=geoarrow-pyarrow"
pip install "git+https://github.com/geoarrow/geoarrow-python.git#egg=geoarrow-pandas&subdirectory=geoarrow-pandas"
```

If you can import the namespaces, you're good to go!


```python
import geoarrow.pyarrow as ga
import geoarrow.pandas as _
```

## Examples

You can create geoarrow-encoded `pyarrow.Array`s with `as_geoarrow()`:


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
from pyarrow import feather

url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/ns-water-basin_line.arrow"
local_filename, headers = urllib.request.urlretrieve(url)
feather.read_table(local_filename).schema
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



The `as_geoarrow()` function can accept a `geopandas.GeoSeries` as input:


```python
import geopandas

url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/ns-water-basin_line.fgb.zip"
df = geopandas.read_file(url)
array = ga.as_geoarrow(df.geometry)
array
```




    MultiLinestringArray:MultiLinestringType(geoarrow.multilinestring <{"$schema":"https://proj.org/schema...>)[255]
    <MULTILINESTRING ((648686.210534334 5099183.050480807, 648626.2095...>
    <MULTILINESTRING ((687688.0166642987 5117030.253445747, 686766.217...>
    <MULTILINESTRING ((631355.7058094738 5122893.354471898, 631364.529...>
    <MULTILINESTRING ((665166.2114203956 5138643.056812348, 665146.211...>
    <MULTILINESTRING ((673606.2114490251 5162963.061371056, 673606.211...>
    ...245 values...
    <MULTILINESTRING ((681672.817898342 5078602.646958541, 681866.2179...>
    <MULTILINESTRING ((414868.0669037141 5093041.933686847, 414793.966...>
    <MULTILINESTRING ((414868.0669037141 5093041.933686847, 414829.866...>
    <MULTILINESTRING ((414868.0669037141 5093041.933686847, 414937.366...>
    <MULTILINESTRING ((648686.210534334 5099183.050480807, 648866.2105...>



You can convert back to geopandas using `to_geopandas()`:


```python
ga.to_geopandas(array)
```




    0      MULTILINESTRING ((648686.211 5099183.050, 6486...
    1      MULTILINESTRING ((687688.017 5117030.253, 6867...
    2      MULTILINESTRING ((631355.706 5122893.354, 6313...
    3      MULTILINESTRING ((665166.211 5138643.057, 6651...
    4      MULTILINESTRING ((673606.211 5162963.061, 6736...
                                 ...
    250    MULTILINESTRING ((681672.818 5078602.647, 6818...
    251    MULTILINESTRING ((414868.067 5093041.934, 4147...
    252    MULTILINESTRING ((414868.067 5093041.934, 4148...
    253    MULTILINESTRING ((414868.067 5093041.934, 4149...
    254    MULTILINESTRING ((648686.211 5099183.050, 6488...
    Length: 255, dtype: geometry



## Pandas integration

The `geoarrow-pandas` package provides an extension array that wraps geoarrow memory and an accessor that provides pandas-friendly wrappers around the compute functions available in `geoarrow.pyarrow`.


```python
import geoarrow.pandas as _
import pandas as pd

df = pd.read_feather("https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/ns-water-basin_point.arrow")
df.geometry.geoarrow.format_wkt().head(5)
```




    0     MULTIPOINT (277022.6936181751 4820886.609673489)
    1     MULTIPOINT (315701.2552756762 4855051.378571571)
    2    MULTIPOINT (255728.65994492616 4851022.107901295)
    3     MULTIPOINT (245206.7841665779 4895609.409696873)
    4    MULTIPOINT (337143.18135472975 4860312.288760258)
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
