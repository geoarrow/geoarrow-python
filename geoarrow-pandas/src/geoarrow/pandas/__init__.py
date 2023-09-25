"""
Contains pandas integration for the geoarrow Python bindings.
Importing this package will register pyarrow extension types and
register the ``geoarrow`` accessor on ``pandas.Series`` objects.

Examples
--------

>>> import geoarrow.pandas as _
"""

from .lib import (
    GeoArrowAccessor,
    GeoArrowExtensionDtype,
    GeoArrowExtensionArray,
    GeoArrowExtensionScalar,
)
