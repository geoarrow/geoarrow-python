import pytest
from pandas.tests.extension import base
import geoarrow.pandas as gapd
import geoarrow.pyarrow as ga
import operator

from pandas import (
    Series,
    options,
)


@pytest.fixture
def dtype():
    """A fixture providing the ExtensionDtype to validate."""
    return gapd.GeoArrowExtensionDtype(ga.wkt())


@pytest.fixture
def data():
    """
    Length-100 array for this type.

    * data[0] and data[1] should both be non missing
    * data[0] and data[1] should not be equal
    """
    strings = [f"POINT ({i} {i + 1})" for i in range(100)]
    return gapd.GeoArrowExtensionArray(ga.array(strings))


@pytest.fixture
def data_for_twos():
    """Length-100 array in which all the elements are two."""
    pytest.skip()


@pytest.fixture
def data_missing():
    """Length-2 array with [NA, Valid]"""
    return gapd.GeoArrowExtensionArray([None, "POINT (0 1)"])


@pytest.fixture(params=["data", "data_missing"])
def all_data(request, data, data_missing):
    """Parametrized fixture giving 'data' and 'data_missing'"""
    if request.param == "data":
        return data
    elif request.param == "data_missing":
        return data_missing


@pytest.fixture
def data_repeated(data):
    """
    Generate many datasets.

    Parameters
    ----------
    data : fixture implementing `data`

    Returns
    -------
    Callable[[int], Generator]:
        A callable that takes a `count` argument and
        returns a generator yielding `count` datasets.
    """

    def gen(count):
        for _ in range(count):
            yield data

    return gen


@pytest.fixture
def data_for_sorting():
    """
    Length-3 array with a known sort order.

    This should be three items [B, C, A] with
    A < B < C
    """
    pytest.skip()


@pytest.fixture
def data_missing_for_sorting():
    """
    Length-3 array with a known sort order.

    This should be three items [B, NA, A] with
    A < B and NA missing.
    """
    pytest.skip()


@pytest.fixture
def na_cmp():
    """
    Binary operator for comparing NA values.

    Should return a function of two arguments that returns
    True if both arguments are (scalar) NA for your type.

    By default, uses ``operator.is_``
    """
    return operator.is_


@pytest.fixture
def na_value():
    """The scalar missing value for this type. Default 'None'"""
    return None


@pytest.fixture
def data_for_grouping():
    """
    Data for factorization, grouping, and unique tests.

    Expected to be like [B, B, NA, NA, A, A, B, C]

    Where A < B < C and NA is missing
    """
    pytest.skip()


@pytest.fixture(params=[True, False])
def box_in_series(request):
    """Whether to box the data in a Series"""
    return request.param


@pytest.fixture(
    params=[
        lambda x: 1,
        lambda x: [1] * len(x),
        lambda x: Series([1] * len(x)),
        lambda x: x,
    ],
    ids=["scalar", "list", "series", "object"],
)
def groupby_apply_op(request):
    """
    Functions to test groupby.apply().
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_frame(request):
    """
    Boolean fixture to support Series and Series.to_frame() comparison testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_series(request):
    """
    Boolean fixture to support arr and Series(arr) comparison testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def use_numpy(request):
    """
    Boolean fixture to support comparison testing of ExtensionDtype array
    and numpy array.
    """
    return request.param


@pytest.fixture(params=["ffill", "bfill"])
def fillna_method(request):
    """
    Parametrized fixture giving method parameters 'ffill' and 'bfill' for
    Series.fillna(method=<method>) testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_array(request):
    """
    Boolean fixture to support ExtensionDtype _from_sequence method testing.
    """
    return request.param


@pytest.fixture
def invalid_scalar(data):
    """
    A scalar that *cannot* be held by this ExtensionArray.

    The default should work for most subclasses, but is not guaranteed.

    If the array can hold any item (i.e. object dtype), then use pytest.skip.
    """
    return object.__new__(object)


@pytest.fixture
def using_copy_on_write() -> bool:
    """
    Fixture to check if Copy-on-Write is enabled.
    """
    return options.mode.copy_on_write and options.mode.data_manager == "block"


class TestGeoArrowDtype(base.BaseDtypeTests):
    pass


class TestGeoArrowConstructors(base.BaseConstructorsTests):
    pass


class TestGeoArrowGetItem(base.BaseGetitemTests):
    pass


class TestGeoArrowMissing(base.BaseMissingTests):
    def test_fillna_scalar(self, data_missing):
        pytest.skip()

    def test_fillna_frame(self, data_missing):
        pytest.skip()

    def test_fillna_series(self, data_missing):
        pytest.skip()


class TestGeoArrowMethods(base.BaseMethodsTests):
    def test_value_counts(self, all_data):
        pytest.skip()

    def test_value_counts_with_normalize(self, data):
        pytest.skip()

    def test_factorize_empty(self, data):
        pytest.skip()

    def test_fillna_copy_frame(self, data_missing):
        pytest.skip()

    def test_fillna_copy_series(self, data_missing):
        pytest.skip()

    def test_combine_first(self, data):
        pytest.skip()

    def test_shift_0_periods(self, data):
        pytest.skip()

    def test_where_series(self, data, na_value, as_frame):
        pytest.skip()


class TestGeoArrowIndex(base.BaseIndexTests):
    pass


class TestGeoArrowInterface(base.BaseInterfaceTests):
    def test_copy(self, data):
        pytest.skip()

    def test_view(self, data):
        pytest.skip()


class TestGeoArrowParsing(base.BaseParsingTests):
    pass


class TestGeoArrowPrinting(base.BasePrintingTests):
    pass
