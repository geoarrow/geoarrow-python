from copy import deepcopy
import json
from typing import Union, Mapping, Protocol, Optional


class Crs(Protocol):
    """Coordinate reference system protocol

    Defines an protocol with the methods required by GeoArrow types
    to consume a coordinate reference system. This is a subset of the
    methods available from a ``pyproj.CRS`` such that a pyproj CRS
    can be used to create GeoArrow types.
    """

    @classmethod
    def from_json(cls, crs_json: str) -> "Crs":
        """Create an instance from a PROJJSON string."""
        raise NotImplementedError()

    @classmethod
    def from_json_dict(cls, crs_dict: Mapping) -> "Crs":
        """Create an instance from the dictionary representation of a parsed
        PROJJSON string.
        """
        raise NotImplementedError()

    def to_json(self) -> str:
        """Returns the PROJJSON representation of this coordinate reference
        system.
        """
        raise NotImplementedError()

    def to_json_dict(self) -> Mapping:
        """Returns the parsed PROJJSON representation of this coordinate reference
        system."""
        raise NotImplementedError()


class ProjJsonCrs(Crs):
    """Concrete Crs implementation wrapping a previously-generated
    PROJJSON string or dictionary.

    Parameters
    ----------
    obj : dict or str or bytes
        The PROJJSON representation as a string, dictionary representation
        of the parsed string, or UTF-8 bytes.


    Examples
    --------
    >>> from geoarrow.types import crs
    >>> crs.ProjJsonCrs('{"key": "value"}')
    ProjJsonCrs({"key": "value"})
    """

    @classmethod
    def from_json(cls, crs_json: str) -> "Crs":
        return ProjJsonCrs(crs_json)

    @classmethod
    def from_json_dict(cls, crs_dict: Mapping) -> "Crs":
        return ProjJsonCrs(crs_dict)

    def __init__(self, obj: Union[Crs, Mapping, str, bytes]) -> None:
        if isinstance(obj, dict):
            self._obj = obj
            self._str = None
        elif isinstance(obj, str):
            self._obj = None
            self._str = obj
        elif isinstance(obj, bytes):
            self._obj = None
            self._str = obj.decode()
        elif hasattr(obj, "to_json"):
            self._obj = None
            self._str = obj.to_json()
        else:
            raise TypeError(
                "ProjJsonCrs can only be created from Crs, dict, str, or bytes"
            )

    def to_json(self) -> str:
        if self._str is None:
            self._str = json.dumps(self._obj)

        return self._str

    def to_json_dict(self) -> Mapping:
        if self._obj is None:
            self._obj = json.loads(self._str)

        return deepcopy(self._obj)

    def __repr__(self) -> str:
        try:
            crs_dict = self.to_json_dict()
            if "id" in crs_dict:
                crs_id = crs_dict["id"]
                if "authority" in crs_id and "code" in crs_id:
                    return f'ProjJsonCrs({crs_id["authority"]}:{crs_id["code"]})'

        except ValueError:
            pass

        return f"ProjJsonCrs({self.to_json()[:80]})"


_CRS_LONLAT_DICT = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
            {
                "name": "World Geodetic System 1984 (Transit)",
                "id": {"authority": "EPSG", "code": 1166},
            },
            {
                "name": "World Geodetic System 1984 (G730)",
                "id": {"authority": "EPSG", "code": 1152},
            },
            {
                "name": "World Geodetic System 1984 (G873)",
                "id": {"authority": "EPSG", "code": 1153},
            },
            {
                "name": "World Geodetic System 1984 (G1150)",
                "id": {"authority": "EPSG", "code": 1154},
            },
            {
                "name": "World Geodetic System 1984 (G1674)",
                "id": {"authority": "EPSG", "code": 1155},
            },
            {
                "name": "World Geodetic System 1984 (G1762)",
                "id": {"authority": "EPSG", "code": 1156},
            },
            {
                "name": "World Geodetic System 1984 (G2139)",
                "id": {"authority": "EPSG", "code": 1309},
            },
        ],
        "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
        },
        "accuracy": "2.0",
        "id": {"authority": "EPSG", "code": 6326},
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "scope": "Not known.",
    "area": "World.",
    "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180,
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}

OGC_CRS84 = ProjJsonCrs.from_json_dict(_CRS_LONLAT_DICT)
"""Longitude/latitude CRS definition"""


class UnspecifiedCrs(Crs):
    def __eq__(self, value):
        return value is UNSPECIFIED


UNSPECIFIED = UnspecifiedCrs()
"""Unspecified CRS sentinel

A :class:`Crs` singleton indicating that a CRS has not been specified.
This is necessary because ``None`` is a valid CRS specification denoting
an explicitly unset CRS.
"""


def create(obj) -> Optional[Crs]:
    """Create a Crs from an arbitrary Python object

    Applies some heuristics to sanitize an object as a CRS that can be
    exported to PROJJSON for use with a GeoArrow type.

    Parameters
    ----------
    obj : None, crs-like, string, bytes, or dict
        Can be any of:
        - ``None``, in which case ``None`` will be returned. This is the
          sentinel used to indcate an explicitly unset CRS.
        - A crs-like object (i.e., an object with a ``to_json_dict()`` method)
        - A string, bytes, or dictionary representation of a PROJJSON crs
          (passed to :class:`ProjJsonCrs`).

    Examples
    --------
    >>> from geoarrow.types import crs
    >>> crs.create(None)
    >>> crs.create(crs.OGC_CRS84)
    ProjJsonCrs(OGC:CRS84)
    """
    if obj is None:
        return None
    elif hasattr(obj, "to_json_dict"):
        return obj
    else:
        return ProjJsonCrs(obj)


def _coalesce2(value, default):
    if value is UNSPECIFIED:
        return default
    else:
        return value


def _coalesce_unspecified2(lhs, rhs):
    if _crs_equal(lhs, rhs):
        return lhs
    elif lhs == UNSPECIFIED:
        return rhs
    elif rhs == UNSPECIFIED:
        return lhs
    else:
        raise ValueError(f"Crs {lhs} and {rhs} are both specified")


def _common2(lhs, rhs):
    return _coalesce_unspecified2(lhs, rhs)


def _crs_equal(lhs, rhs):
    if lhs is UNSPECIFIED or rhs is UNSPECIFIED:
        return lhs == rhs
    elif lhs == rhs:
        return True
    elif hasattr(lhs, "to_json_dict") and hasattr(rhs, "to_json_dict"):
        # This could be more sophisticated; however, CRS equality is
        # hard and is currently outside the scope of this module
        return lhs.to_json_dict() == rhs.to_json_dict()
    else:
        return False
