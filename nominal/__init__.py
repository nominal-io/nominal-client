from .nominal import (
    Dataset as Dataset,
    Ingest as Ingest,
    Run as Run,
)

# Allows:
# import nominal as nm
# nm.auth ...
# nm.data ...
from . import data as data
from . import auth as auth

import os
os.environ["NOMINAL_BASE_URL"] = "https://api-staging.gov.nominal.io/api"
