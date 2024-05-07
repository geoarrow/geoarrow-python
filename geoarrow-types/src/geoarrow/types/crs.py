import json
from typing import Union, Mapping


class Crs:
    """Abstract coordinate reference system definition

    Defines an abstract class with the methods required by GeoArrow types
    to consume a coordinate reference system.
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
    >>> crs.ProjJsonCrs({})
    {}
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

        return self._obj

    def __repr__(self) -> str:
        try:
            crs_dict = self.to_json_dict()
            if "id" in crs_dict:
                crs_id = crs_dict["id"]
                if "authority" in crs_id and "code" in crs_id:
                    return f'{crs_id["authority"]}{crs_id["code"]}'
            return repr(crs_dict)[:80]
        except ValueError:
            return repr(self.to_json())[:80]


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

#: Longitude/latitude CRS definition
OGC_CRS84 = ProjJsonCrs.from_json_dict(_CRS_LONLAT_DICT)
