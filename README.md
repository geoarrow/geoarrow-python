# geoarrow for Python

The geoarrow Python packages provide bindings to the geoarrow-c implementation of the [GeoArrow specification](https://github.com/geoarrow/geoarrow) and integrations with [pyarrow](https://arrow.apache.org/docs/python) and [pandas](https://pandas.pydata.org/). The geoarrow Python bindings provide input/output to/from Arrow-friendly formats (e.g., Parquet, Arrow Stream, Arrow File) and general-purpose coordinate shuffling tools among GeoArrow, WKT, and WKB encodings.

## Installation

Python bindings for geoarrow are not yet available on PyPI. You can install via URL (requires a C++ compiler):

```bash
python -m pip install "https://github.com/geoarrow/geoarrow-c/archive/refs/heads/main.zip#egg=geoarrow-c&subdirectory=python/geoarrow-c"
python -m pip install "https://github.com/geoarrow/geoarrow-c/archive/refs/heads/main.zip#egg=geoarrow-pyarrow&subdirectory=python/geoarrow-pyarrow"
python -m pip install "https://github.com/geoarrow/geoarrow-c/archive/refs/heads/main.zip#egg=geoarrow-pandas&subdirectory=python/geoarrow-pandas"
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



You can convert back to geopandas using `as_wkb()` and `GeoSeries.from_wkb()`:


```python
geopandas.GeoSeries.from_wkb(ga.as_wkb(array))
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



If you'd like to do some of your own processing, you can access buffers as numpy arrays
using `.geobuffers()`:


```python
array.geobuffers()
```




    [None,
     array([  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,
             13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,
             26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,
             39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
             52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,
             65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,
             78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,
             91,  92,  93,  94,  95,  96,  97,  98,  99, 100, 101, 102, 103,
            104, 105, 106, 107, 108, 109, 110, 112, 113, 114, 115, 116, 117,
            118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130,
            131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
            144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156,
            157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169,
            170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182,
            183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195,
            196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208,
            209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221,
            222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
            235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247,
            248, 249, 250, 251, 252, 253, 254, 255, 256], dtype=int32),
     array([    0,   405,  1095,  1728,  2791,  3242,  3952,  4025,  4709,
             6366,  6368,  6373,  6375,  6377,  6381,  6384,  6386,  6395,
             6397,  6399,  6401,  6403,  6409,  6412,  6414,  6418,  6420,
             6423,  6426,  6428,  6432,  6445,  6448,  6450,  6457,  6462,
             6464,  6466,  6476,  6479,  6482,  6484,  6486,  6493,  6501,
             6503,  6506,  6509,  6511,  6513,  6515,  6517,  6519,  6521,
             6523,  6525,  6527,  6529,  6531,  6534,  6653,  6943,  7047,
             7086,  7131,  7140,  7220,  7230,  7233,  7285,  7420,  7450,
             7460,  7485,  7492,  7494,  7497,  7499,  7501,  7504,  7506,
             7568,  7629,  7633,  7635,  7689,  7778,  7780,  7783,  7786,
             7788,  7792,  7795,  7798,  7800,  7805,  8366,  8368,  8372,
             8376,  8380,  8382,  8385,  8388,  8392,  8395,  8398,  8400,
             8402,  8404,  8406,  8408,  8411,  8413,  8416,  8420,  8435,
             8437,  8440,  8443,  8446,  8453,  8456,  8458,  8460,  8463,
             8465,  8468,  8475,  8478,  8488,  8490,  8493,  8495,  8498,
             8502,  8505,  8508,  8510,  8513,  8515,  8517,  8529,  8531,
             8534,  8539,  8546,  8549,  8555,  8564,  8566,  8569,  8571,
             8855,  9156,  9957, 10118, 10179, 10380, 10561, 10862, 11111,
            11212, 11352, 11765, 11848, 11949, 12043, 12261, 12742, 12943,
            13244, 13285, 13306, 13527, 13548, 13609, 13650, 13771, 13823,
            13994, 14407, 14982, 15631, 16161, 16655, 17779, 17879, 17918,
            18382, 18783, 20028, 21223, 21344, 21492, 21601, 21686, 21952,
            22278, 22756, 23118, 23391, 24186, 24379, 24655, 25164, 25486,
            26279, 26741, 27295, 28306, 28331, 28844, 29429, 30233, 31317,
            31472, 33015, 33061, 33245, 33415, 33715, 33866, 34589, 34834,
            35317, 35474, 35595, 35997, 36210, 36770, 37053, 37074, 37434,
            37874, 38724, 39846, 40443, 41369, 42169, 43122, 43289, 43539,
            44133, 44771, 44781, 44890, 46058, 46534, 46988, 47466, 48053,
            48712, 49007, 49221, 49275, 49494], dtype=int32),
     array([648686.0197, 648626.0187, 648586.0197, ..., 658335.0659,
            658341.5039, 658351.4199]),
     array([5099181.9841, 5099181.9841, 5099161.9831, ..., 5099975.8904,
            5099981.8684, 5099991.9824])]



You can do the inverse operation (from raw buffers to GeoPandas) using `.from_geobuffers()`, and `.as_wkb()`:


```python
ga_type = ga.multilinestring() \
    .with_dimensions(ga.Dimensions.XY) \
    .with_crs(array.type.crs)
geoarrow_array2 = ga_type.from_geobuffers(*array.geobuffers())
geoarrow_array2
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



## Building

Python bindings for nanoarrow are managed with [setuptools](https://setuptools.pypa.io/en/latest/index.html).
This means you can build the project using:

```shell
git clone https://github.com/geoarrow/geoarrow-c.git
cd python
pip install -e geoarrow-c/ goearrow-pyarrow/ geoarrow-pandas/
```

Tests use [pytest](https://docs.pytest.org/):

```shell
# Install dependencies
for d in geoarrow-c geoarrow-pyarrow geoarrow-pandas; do
    cd $d && pip install -e ".[test]" && cd ..
done

# Run tests
for d in geoarrow-c geoarrow-pyarrow geoarrow-pandas; do
    cd $d && pytest && cd ..
done
```
