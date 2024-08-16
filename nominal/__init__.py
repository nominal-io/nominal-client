import os

# Allows:
# import nominal as nm
# nm.cloud ...
# nm.data ...
from . import cloud as cloud
from . import data as data
from .nominal import Dataset as Dataset
from .nominal import Ingest as Ingest
from .nominal import Run as Run

os.environ["NOMINAL_BASE_URL"] = "https://api-staging.gov.nominal.io/api"
