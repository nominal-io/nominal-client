from .nominal import (
    Dataset as Dataset,
    Ingest as Ingest,
    Run as Run,
)

# Allows:
# import nominal as nm
# nm.cloud ...
# nm.data ...
from . import data as data
from . import cloud as cloud

import os

os.environ["NOMINAL_BASE_URL"] = "https://api.gov.nominal.io/api"
