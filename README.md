# GeoArrow for Python

The GeoArrow Python packages provide an implementation of the [GeoArrow specification](https://geoarrow.org) that integrates with [pyarrow](https://arrow.apache.org/docs/python). The GeoArrow Python bindings enable input/output to/from Arrow-friendly formats (e.g., Parquet, Arrow Stream, Arrow File) and general-purpose coordinate shuffling tools among GeoArrow, WKT, and WKB encodings.

## Installation

Python bindings for GeoArrow are available on PyPI. You can install them with:

```bash
pip install geoarrow-pyarrow
```

You can install the latest development version with:

```bash
pip install "git+https://github.com/geoarrow/geoarrow-python.git#subdirectory=geoarrow-pyarrow"
```

If you can import the namespace, you're good to go!


```python
import geoarrow.pyarrow as ga
```

## Example

The most important thing that `geoarrow.pyarrow` does is register pyarrow extension types so that metadata is kept intact when reading files or interacting with other libraries. For example, we can now read Arrow IPC files written with GeoArrow extension types and the CRS and geometry type is kept:


```python
import pyarrow as pa
import urllib.request

url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_wkb.arrows"
with urllib.request.urlopen(url) as f, pa.ipc.open_stream(f) as reader:
    tab = reader.read_all()

tab.schema.field("geometry").type
```




    WkbType(geoarrow.wkb <ProjJsonCrs(EPSG:4326)>)



Use `geoarrow.pyarrow.to_geopandas()` to convert to [geopandas](https://geopandas.org):


```python
df = ga.to_geopandas(tab)
df.geometry.crs
```




    <Geographic 2D CRS: EPSG:4326>
    Name: WGS 84
    Axis Info [ellipsoidal]:
    - Lat[north]: Geodetic latitude (degree)
    - Lon[east]: Geodetic longitude (degree)
    Area of Use:
    - name: World.
    - bounds: (-180.0, -90.0, 180.0, 90.0)
    Datum: World Geodetic System 1984 ensemble
    - Ellipsoid: WGS 84
    - Prime Meridian: Greenwich



...and use `GeoDataFrame.to_arrow()` to get it back:


```python
pa.table(df.to_arrow())["geometry"].type.crs
```




    ProjJsonCrs(EPSG:4326)



These Python bindings also include [GeoParquet](https://geoparquet.org) and [pyogrio](https://github.com/geopandas/pyogrio) integration for direct IO to/from pyarrow. This can be useful when loading data approaching the size of available memory as GeoPandas requires many times more memory for some types of data (notably: large numbers of points).


```python
import geoarrow.pyarrow.io

url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities.fgb"
geoarrow.pyarrow.io.read_pyogrio_table(url)
```




    pyarrow.Table
    name: string
    geometry: extension<geoarrow.wkb<WkbType>>
    ----
    name: [["Vatican City","San Marino","Vaduz","Lobamba","Luxembourg",...,"Rio de Janeiro","Sao Paulo","Sydney","Singapore","Hong Kong"]]
    geometry: [[010100000054E57B4622E828408B074AC09EF34440,0101000000DCB122B42FE228402376B7FCD1F74540,01010000006DAE9AE78808234032D989DC1D914740,01010000007BCB8B0233333F40289B728577773AC0,0101000000C08D39741F8518400F2153E34ACE4840,...,0101000000667B47AA269B45C002B53F5745E836C0,0101000000F15A536A405047C0C1148A19868E37C0,0101000000A286FD30CDE662401F04CF2989EF40C0,01010000003A387DE2A5F659409AF3E7363CB8F43F,0101000000D865F84FB78B5C40144438C1924E3640]]




```python
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet"
local_filename, _ = urllib.request.urlretrieve(url)

geoarrow.pyarrow.io.read_geoparquet_table(local_filename)
```




    pyarrow.Table
    name: string
    geometry: extension<geoarrow.wkb<WkbType>>
    ----
    name: [["Vatican City","San Marino","Vaduz","Lobamba","Luxembourg",...,"Rio de Janeiro","Sao Paulo","Sydney","Singapore","Hong Kong"]]
    geometry: [[010100000054E57B4622E828408B074AC09EF34440,0101000000DCB122B42FE228402376B7FCD1F74540,01010000006DAE9AE78808234032D989DC1D914740,01010000007BCB8B0233333F40289B728577773AC0,0101000000C08D39741F8518400F2153E34ACE4840,...,0101000000667B47AA269B45C002B53F5745E836C0,0101000000F15A536A405047C0C1148A19868E37C0,0101000000A286FD30CDE662401F04CF2989EF40C0,01010000003A387DE2A5F659409AF3E7363CB8F43F,0101000000D865F84FB78B5C40144438C1924E3640]]



Finally, a number of compute functions are provided for common transformations required to create/consume arrays of geometries:


```python
ga.format_wkt(tab["geometry"])[:5]
```




    <pyarrow.lib.ChunkedArray object at 0x1108252a0>
    [
      [
        "POINT (12.4533865 41.9032822)",
        "POINT (12.4417702 43.9360958)",
        "POINT (9.5166695 47.1337238)",
        "POINT (31.1999971 -26.4666675)",
        "POINT (6.1300028 49.6116604)"
      ]
    ]



## Create/Consume GeoArrow Arrays

The `geoarrow-pyarrow` package also provides a number of utilities for working with serialized and GeoArrow-native arrays. For example, you can create geoarrow-encoded `pyarrow.Array`s with `as_geoarrow()`:


```python
ga.as_geoarrow(["POINT (0 1)"])
```




    GeometryExtensionArray:PointType(geoarrow.point)[1]
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




    GeometryExtensionArray:PointType(geoarrow.point)[3]
    <POINT (1 3)>
    <POINT (2 4)>
    <POINT (3 5)>




```python
ga.point().with_coord_type(ga.CoordType.INTERLEAVED).from_geobuffers(
    None,
    np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
)
```




    GeometryExtensionArray:PointType(interleaved geoarrow.point)[3]
    <POINT (1 2)>
    <POINT (3 4)>
    <POINT (5 6)>



## For Developers

One of the challeneges with GeoArrow data is the large number of permutations between X, Y, Z, M, geometry types, and serialized encodings. The `geoarrow-types` package provides pure Python utilities to manage, compute on, and specify these types (or parts of them, as required).


```python
import geoarrow.types as gt

gt.TypeSpec.common(
    gt.Encoding.GEOARROW,
    gt.GeometryType.POINT,
    gt.GeometryType.MULTIPOINT,
    gt.Dimensions.XYM,
    gt.Dimensions.XYZ,
).to_pyarrow()
```




    MultiPointType(geoarrow.multipoint_zm)



## Building

Python bindings for geoarrow are managed with [setuptools](https://setuptools.pypa.io/en/latest/index.html).
This means you can build the project using:

```shell
git clone https://github.com/geoarrow/geoarrow-python.git
pip install -e geoarrow-pyarrow/ geoarrow-types/
```

Tests use [pytest](https://docs.pytest.org/):

```shell
pytest
```
