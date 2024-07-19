"""
Contains pandas integration for the geoarrow Python bindings.
Importing this package will register pyarrow extension types and
register the ``geoarrow`` accessor on ``pandas.Series`` objects.

Examples
--------

>>> import geoarrow.pandas as _
"""

from geoarrow.types._version import __version__, __version_tuple__  # NOQA: F401

from .lib import (
    GeoArrowAccessor,
    GeoArrowExtensionDtype,
    GeoArrowExtensionArray,
    GeoArrowExtensionScalar,
)

__all__ = [
    "GeoArrowAccessor",
    "GeoArrowExtensionDtype",
    "GeoArrowExtensionArray",
    "GeoArrowExtensionScalar",
]
