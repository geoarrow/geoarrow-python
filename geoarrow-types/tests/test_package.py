
import re

import geoarrow.types as gat

def test_version():
    assert re.match(r"^[0-9]+\.[0-9]+\.[0-9]+", gat.__version__)
