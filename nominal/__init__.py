from .nominal import (
    Dataset as Dataset,
    Ingest as Ingest,
    Run as Run,
)
import os

os.environ["NOMINAL_BASE_URL"] = "https://api-staging.gov.nominal.io/api"
