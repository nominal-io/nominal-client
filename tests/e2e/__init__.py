import random
from datetime import datetime, timedelta

from nominal_api import scout_catalog

from nominal.core import NominalClient
from nominal.core.dataset import Dataset, _create_dataset_request

POLL_INTERVAL = timedelta(seconds=0.1)
"""Default polling interval for `poll_until_ingestion_completed` calls in e2e tests."""


def create_legacy_dataset(client: NominalClient, name: str, description: str | None = None) -> Dataset:
    """Create a LEGACY-backed dataset, for tests that ingest a file but are not about the backing type.

    The environment default backing can take minutes to ingest a ten-row file, and reads can trail
    the reported ingest success. LEGACY completes within seconds and is readable as soon as it
    does. Tests of the default ingest path call `client.create_dataset` instead.
    """
    clients = client._clients
    request = _create_dataset_request(
        name,
        description=description,
        workspace_rid=clients.resolve_default_workspace_rid(),
        dataset_type=scout_catalog.DatasetBackingType.LEGACY,
    )
    return Dataset._from_conjure(clients, clients.catalog.create_dataset(clients.auth_header, request))


def _create_random_start_end() -> tuple[datetime, datetime]:
    random_epoch_start = int(datetime(2020, 1, 1).timestamp())
    random_epoch_end = int(datetime(2025, 1, 1).timestamp())
    epoch_start = random.randint(random_epoch_start, random_epoch_end)
    start = datetime.fromtimestamp(epoch_start)
    end = start + timedelta(hours=1)
    return start, end
