from .nominal import (
    Dataset as Dataset,
    Ingest as Ingest,
    Run as Run,
)

from . import data as data

import os
os.environ["NOMINAL_BASE_URL"] = "https://api-staging.gov.nominal.io/api"
