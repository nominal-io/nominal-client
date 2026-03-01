"""Shared pytest fixtures and configuration for the e2e test suite.

Connection setup
----------------
The `client` fixture builds a NominalClient from either a named profile
(``--profile``) or a raw token+URL pair (``--auth-token`` / ``--base-url``).
The `set_connection` fixture patches `nominal.nominal.get_default_client` so
that any code path relying on the module-level default client also uses the
same authenticated instance.

Shared data fixtures
--------------------
``csv_data``    — 10 rows of sensor readings with an ISO 8601 timestamp column.
``csv_data2``   — 10 more rows continuing the same schema (used for multi-file upload tests).
``csv_gz_data`` — gzip-compressed version of csv_data (used to test FileTypes.CSV_GZ).
``mp4_data``    — a short MP4 clip read from disk (used for video ingest tests).

Shared resource fixtures
------------------------
``ingested_dataset`` — a single session-scoped dataset ingested from csv_data; shared
                       by all read-only channel/pandas tests to avoid redundant ingest calls.

Teardown helpers
----------------
``archive`` — a function-scoped helper that registers arbitrary objects for cleanup via
              `request.addfinalizer`, so resources are archived even if the test fails.
"""

from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path
from typing import Iterator
from unittest import mock
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from tests.e2e import POLL_INTERVAL


def pytest_addoption(parser):
    """Register e2e-specific CLI options for choosing the target Nominal environment."""
    parser.addoption("--base-url", default="https://api.nominal.test")
    parser.addoption("--auth-token", default=None)
    parser.addoption(
        "--profile", default=None, help="Nominal profile name (takes precedence over --auth-token / --base-url)"
    )


@pytest.fixture(scope="session")
def client(pytestconfig) -> NominalClient:
    """Build a NominalClient for the target environment.

    Precedence: ``--profile`` > ``--auth-token`` + ``--base-url``.
    Raises UsageError if neither is supplied.
    """
    profile = pytestconfig.getoption("profile")
    if profile is not None:
        print(f"Using NominalClient.from_profile({profile!r})")
        return NominalClient.from_profile(profile)
    auth_token = pytestconfig.getoption("auth_token")
    if auth_token is None:
        raise pytest.UsageError("Either --profile or --auth-token must be provided")
    base_url = pytestconfig.getoption("base_url")
    print(f"Using NominalClient.create(base_url={base_url!r})")
    return NominalClient.create(base_url=base_url, token=auth_token)


@pytest.fixture(scope="session", autouse=True)
def set_connection(client) -> Iterator[None]:
    """Patch the module-level default client so top-level `nominal.*` calls use the test client."""
    with mock.patch("nominal.nominal.get_default_client", return_value=client):
        yield


@pytest.fixture
def archive(request):
    """Register archivable objects to be cleaned up (archived) after the test.

    Usage::

        def test_something(client, archive):
            obj = client.create_dataset(...)
            archive(obj)  # archived even if the test fails
            ...
    """

    def _register(obj):
        request.addfinalizer(obj.archive)

    return _register


@pytest.fixture(scope="session")
def ingested_dataset(client: NominalClient, csv_data: bytes) -> Iterator[Dataset]:
    """A single ingested dataset shared across all read-only tests in the suite."""
    ds = client.create_dataset(f"dataset-e2e-readonly-{uuid4().hex[:8]}")
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(interval=POLL_INTERVAL)
    yield ds
    ds.archive()


@pytest.fixture(scope="session")
def csv_data():
    """Ten rows of sensor readings (timestamp, relative_minutes, temperature, humidity) in ISO 8601 format."""
    return b"""\
timestamp,relative_minutes,temperature,humidity
2024-09-05T18:00:00Z,0,20,50
2024-09-05T18:01:00Z,1,21,49
2024-09-05T18:02:00Z,2,22,48
2024-09-05T18:03:00Z,3,23,47
2024-09-05T18:04:00Z,4,24,46
2024-09-05T18:05:00Z,5,25,45
2024-09-05T18:06:00Z,6,26,44
2024-09-05T18:07:00Z,7,27,43
2024-09-05T18:08:00Z,8,28,42
2024-09-05T18:09:00Z,9,29,41
"""


@pytest.fixture(scope="session")
def csv_data2():
    """Ten additional rows continuing the csv_data schema (used for multi-file dataset tests)."""
    return b"""\
timestamp,relative_minutes,temperature,humidity
2024-09-05T18:10:00Z,10,30,40
2024-09-05T18:11:00Z,11,31,39
2024-09-05T18:12:00Z,12,32,38
2024-09-05T18:13:00Z,13,33,37
2024-09-05T18:14:00Z,14,34,36
2024-09-05T18:15:00Z,15,35,35
2024-09-05T18:16:00Z,16,36,34
2024-09-05T18:17:00Z,17,37,33
2024-09-05T18:18:00Z,18,38,32
2024-09-05T18:19:00Z,19,39,31
"""


@pytest.fixture(scope="session")
def csv_gz_data(csv_data):
    """Gzip-compressed version of csv_data; used to test FileTypes.CSV_GZ uploads."""
    return gzip.compress(csv_data)


@pytest.fixture(scope="session")
def mp4_data():
    r"""Short MP4 clip for video ingest tests.

    From chromium tests: https://github.com/chromium/chromium/blob/main/media/test/data/bear-1280x720.mp4

    To download:
        curl https://raw.githubusercontent.com/chromium/chromium/main/media/test/data/bear-1280x720.mp4 \
            -o data/bear-1280x720.mp4
    """
    path = Path(__file__).parent / "data/bear-1280x720.mp4"
    with open(path, "rb") as f:
        return f.read()
