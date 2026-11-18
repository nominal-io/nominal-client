"""Shared pytest fixtures and configuration for the e2e test suite.

Connection setup
----------------
The `client` fixture builds a NominalClient from either a named profile
(``--profile``) or a raw token+URL pair (``--auth-token`` / ``--base-url``).
With ``--temporary-api-key``, each session client uses a one-day key revoked during
teardown. Under xdist this is one key per worker; migration source and destination
clients have separate keys. Expiry bounds their lifetime if the runner is killed.

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
``default_backed_datasets`` — the datasets ingested into the environment's default backing,
                       uploaded together so their ingest waits overlap.

Teardown helpers
----------------
``archive`` — a function-scoped helper that registers arbitrary objects for cleanup via
              `request.addfinalizer`, so resources are archived even if the test fails.
"""

from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path
from typing import Iterator, Mapping, Sequence
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile, IngestStatus
from tests.e2e import POLL_INTERVAL, DefaultBackedDatasets, create_legacy_dataset
from tests.e2e.credentials import with_temporary_api_key


def pytest_addoption(parser):
    """Register e2e-specific CLI options for choosing the target Nominal environment."""
    parser.addoption(
        "--temporary-api-key",
        action="store_true",
        help="Create a one-day API key per session client and revoke on teardown",
    )
    parser.addoption("--base-url", default="https://api.nominal.test")
    parser.addoption("--auth-token", default=None)
    parser.addoption(
        "--profile", default=None, help="Nominal profile name (takes precedence over --auth-token / --base-url)"
    )


@pytest.fixture(scope="session")
def client(pytestconfig, request: pytest.FixtureRequest) -> NominalClient:
    """Build a NominalClient for the target environment.

    Precedence: ``--profile`` > ``--auth-token`` + ``--base-url``.
    Raises UsageError if neither is supplied.
    """
    profile = pytestconfig.getoption("profile")
    if profile is not None:
        print(f"Using NominalClient.from_profile({profile!r})")
        return with_temporary_api_key(NominalClient.from_profile(profile), request)
    auth_token = pytestconfig.getoption("auth_token")
    if auth_token is None:
        raise pytest.UsageError("Either --profile or --auth-token must be provided")
    base_url = pytestconfig.getoption("base_url")
    print(f"Using NominalClient.create(base_url={base_url!r})")
    return with_temporary_api_key(NominalClient.create(base_url=base_url, token=auth_token), request)


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
    """A LEGACY-backed dataset shared across the read-only channel/pandas tests."""
    ds = create_legacy_dataset(client, f"dataset-e2e-readonly-{uuid4().hex[:8]}")
    try:
        dataset_file = ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(
            interval=POLL_INTERVAL
        )
        assert dataset_file.ingest_status == IngestStatus.SUCCESS
        yield ds
    finally:
        ds.archive()


def _archive_all(datasets: Sequence[Dataset]) -> None:
    """Archive every dataset, so one failure cannot strand the rest, and report the ones that failed.

    Raising here turns a leaked dataset into a visible teardown error rather than a silent one left on the
    stack for someone to find later.
    """
    failures = []
    for dataset in datasets:
        try:
            dataset.archive()
        except Exception as error:  # noqa: BLE001 — teardown reports every failure rather than stopping at one
            failures.append(f"{dataset.rid}: {error!r}")
    if failures:
        raise RuntimeError("failed to archive e2e datasets: " + "; ".join(failures))


@pytest.fixture(scope="session")
def default_backed_datasets(
    client: NominalClient, csv_data: bytes, csv_data2: bytes
) -> Iterator[DefaultBackedDatasets]:
    """The datasets the suite ingests into the environment's default backing and then reads.

    That backing can take two minutes to ingest a ten-row file, but ingests started together take
    about as long as one. So every upload is started before any is waited on, and a test that needs
    another default-backed ingest should add it here rather than wait on its own.

    `two_files` holds two untagged files. `vehicle_a` and `vehicle_b` carry one `vehicle` tag value
    each over disjoint time ranges, and `mixed_vehicles` holds both in one dataset.
    """
    datasets: list[Dataset] = []
    files: list[DatasetFile] = []

    def start(label: str, *uploads: tuple[bytes, Mapping[str, str] | None]) -> Dataset:
        dataset = client.create_dataset(f"dataset-e2e-{label}-{uuid4().hex[:8]}")
        # Registered before the uploads it might not survive: the dataset exists either way.
        datasets.append(dataset)
        files.extend(dataset.add_from_io(BytesIO(data), "timestamp", "iso_8601", tags=tags) for data, tags in uploads)
        return dataset

    vehicle_a_tags, vehicle_b_tags = {"vehicle": "A"}, {"vehicle": "B"}
    try:
        started = DefaultBackedDatasets(
            two_files=start("two-files", (csv_data, None), (csv_data2, None)),
            vehicle_a=start("vehicle-a", (csv_data, vehicle_a_tags)),
            vehicle_b=start("vehicle-b", (csv_data2, vehicle_b_tags)),
            mixed_vehicles=start("mixed-vehicles", (csv_data, vehicle_a_tags), (csv_data2, vehicle_b_tags)),
        )
        for file in files:
            file.poll_until_ingestion_completed(interval=POLL_INTERVAL)
            assert file.ingest_status == IngestStatus.SUCCESS
        yield started
    finally:
        _archive_all(datasets)


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
