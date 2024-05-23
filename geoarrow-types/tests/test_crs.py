import pytest

from geoarrow.types import crs


def test_projjson_crs_from_string():
    crs_obj = crs.ProjJsonCrs.from_json('{"some key": "some value"}')
    assert crs_obj.to_json() == '{"some key": "some value"}'
    assert crs_obj.to_json_dict() == {"some key": "some value"}


def test_projjson_crs_from_dict():
    crs_obj = crs.ProjJsonCrs.from_json_dict({"some key": "some value"})
    assert crs_obj.to_json() == '{"some key": "some value"}'
    assert crs_obj.to_json_dict() == {"some key": "some value"}


def test_projjson_crs_from_bytes():
    crs_obj = crs.ProjJsonCrs('{"some key": "some value"}'.encode())
    assert crs_obj.to_json() == '{"some key": "some value"}'


def test_projjson_crs_from_crs():
    crs_obj = crs.ProjJsonCrs.from_json('{"some key": "some value"}')
    crs_obj_from_crs = crs.ProjJsonCrs(crs_obj)
    assert crs_obj_from_crs.to_json() == crs_obj.to_json()


def test_projjson_crs_repr():
    crs_valid_projjson = crs.OGC_CRS84
    assert repr(crs_valid_projjson) == "ProjJsonCrs(OGC:CRS84)"

    crs_valid_json = crs.ProjJsonCrs('{"some key": "some value"}')
    assert repr(crs_valid_json) == 'ProjJsonCrs({"some key": "some value"})'

    # repr() shouldn't error here
    crs_invalid_json = crs.ProjJsonCrs('{"this is not valid json')
    assert repr(crs_invalid_json) == 'ProjJsonCrs({"this is not valid json)'


def test_crs_coalesce():
    assert crs._coalesce2(crs.UNSPECIFIED, crs.OGC_CRS84) is crs.OGC_CRS84
    assert crs._coalesce2(None, crs.OGC_CRS84) is None


def test_crs_coalesce_unspecified():
    assert crs._coalesce_unspecified2(crs.UNSPECIFIED, crs.OGC_CRS84) is crs.OGC_CRS84
    assert crs._coalesce_unspecified2(crs.OGC_CRS84, crs.UNSPECIFIED) is crs.OGC_CRS84
    assert crs._coalesce_unspecified2(crs.OGC_CRS84, crs.OGC_CRS84) is crs.OGC_CRS84

    ogc_crs84_clone = crs.ProjJsonCrs(crs.OGC_CRS84.to_json())
    assert crs._coalesce_unspecified2(ogc_crs84_clone, crs.OGC_CRS84) is ogc_crs84_clone
    assert crs._coalesce_unspecified2(crs.OGC_CRS84, ogc_crs84_clone) is crs.OGC_CRS84

    with pytest.raises(ValueError):
        crs._coalesce_unspecified2(None, crs.OGC_CRS84)


def test_crs_common():
    assert crs._common2(crs.UNSPECIFIED, crs.OGC_CRS84) is crs.OGC_CRS84
    assert crs._common2(crs.OGC_CRS84, crs.UNSPECIFIED) is crs.OGC_CRS84
    assert crs._common2(crs.OGC_CRS84, crs.OGC_CRS84) is crs.OGC_CRS84

    with pytest.raises(ValueError):
        crs._common2(None, crs.OGC_CRS84)
