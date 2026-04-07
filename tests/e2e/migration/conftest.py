"""Shared fixtures for migration e2e tests.

Connection setup
----------------
Migration tests require two Nominal clients:
- ``source_client``: the environment to migrate *from* (e.g. production).
  Configured via ``--source-profile`` or ``--source-auth-token`` + ``--source-base-url``.
- ``dest_client``: the environment to migrate *to* (e.g. staging).
  Configured via ``--dest-profile`` or ``--dest-auth-token`` + ``--dest-base-url``.
  Falls back to the global ``--profile`` / ``--auth-token`` + ``--base-url`` options for
  backwards compatibility.

Teardown helpers
----------------
``source_archive`` / ``dest_archive`` — function-scoped helpers that register objects
for cleanup (archive) after each test, even on failure.
"""

from __future__ import annotations

from io import BytesIO
from typing import Callable, Iterator
from unittest import mock
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from tests.e2e import POLL_INTERVAL

ArchiveFn = Callable[[object], None]


def pytest_addoption(parser):
    """Register source and destination environment CLI options."""
    parser.addoption("--source-profile", default=None, help="Source Nominal profile name (e.g. production)")
    parser.addoption("--source-auth-token", default=None, help="Source auth token (used with --source-base-url)")
    parser.addoption(
        "--source-base-url", default="https://api.gov.nominal.io/api", help="Source base URL (default: production)"
    )
    parser.addoption("--dest-profile", default=None, help="Destination Nominal profile name (e.g. staging)")
    parser.addoption("--dest-auth-token", default=None, help="Destination auth token (used with --dest-base-url)")
    parser.addoption(
        "--dest-base-url",
        default="https://api-staging.gov.nominal.io/api",
        help="Destination base URL (default: staging)",
    )


@pytest.fixture(scope="session")
def source_client(pytestconfig) -> NominalClient:
    """Build a NominalClient for the migration source environment (e.g. production)."""
    profile = pytestconfig.getoption("source_profile")
    if profile is not None:
        print(f"Using source NominalClient.from_profile({profile!r})")
        return NominalClient.from_profile(profile)
    auth_token = pytestconfig.getoption("source_auth_token")
    if auth_token is None:
        raise pytest.UsageError(
            "Either --source-profile or --source-auth-token must be provided for migration source environment"
        )
    base_url = pytestconfig.getoption("source_base_url")
    print(f"Using source NominalClient.create(base_url={base_url!r})")
    return NominalClient.create(base_url=base_url, token=auth_token)


@pytest.fixture(scope="session")
def dest_client(pytestconfig) -> NominalClient:
    """Build a NominalClient for the migration destination environment (e.g. staging).

    ``--dest-profile`` takes precedence; falls back to the global ``--profile`` for
    backwards compatibility with existing invocations.
    """
    profile = pytestconfig.getoption("dest_profile") or pytestconfig.getoption("profile")
    if profile is not None:
        print(f"Using dest NominalClient.from_profile({profile!r})")
        return NominalClient.from_profile(profile)
    auth_token = pytestconfig.getoption("dest_auth_token") or pytestconfig.getoption("auth_token")
    if auth_token is None:
        raise pytest.UsageError(
            "Either --dest-profile or --dest-auth-token must be provided for migration destination environment"
        )
    base_url = pytestconfig.getoption("dest_base_url") or pytestconfig.getoption("base_url")
    print(f"Using dest NominalClient.create(base_url={base_url!r})")
    return NominalClient.create(base_url=base_url, token=auth_token)


@pytest.fixture(scope="session", autouse=True)
def set_connection(dest_client: NominalClient) -> Iterator[None]:
    """Override the parent conftest's set_connection to use dest_client.

    The parent fixture requires ``--profile`` which migration tests don't supply;
    this shadows it so the module-level default client points at the destination env.
    """
    with mock.patch("nominal.nominal.get_default_client", return_value=dest_client):
        yield


@pytest.fixture
def source_archive(request) -> ArchiveFn:
    """Register source-environment objects for cleanup after each test."""

    def _register(obj):
        request.addfinalizer(obj.archive)

    return _register


@pytest.fixture
def dest_archive(request) -> ArchiveFn:
    """Register destination-environment objects for cleanup after each test."""

    def _register(obj):
        request.addfinalizer(obj.archive)

    return _register


@pytest.fixture
def migration_ctx(dest_client: NominalClient) -> MigrationContext:
    """A fresh MigrationContext with an empty MigrationState for each test."""
    return MigrationContext(
        destination_client=dest_client,
        migration_state=MigrationState(rid_mapping={}),
    )


@pytest.fixture
def ingested_source_dataset(source_client: NominalClient, csv_data: bytes, source_archive: ArchiveFn) -> Dataset:
    """A dataset on the source client, fully ingested from csv_data."""
    ds = source_client.create_dataset(f"migration-e2e-source-{uuid4().hex[:8]}")
    source_archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(interval=POLL_INTERVAL)
    return ds
