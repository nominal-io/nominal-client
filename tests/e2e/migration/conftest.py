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
``register_cleanup`` — a function-scoped fixture that schedules a callable for execution
after each test, even on failure. Pass any no-arg callable (typically ``obj.archive``) to
register it::

    register_cleanup(obj.archive)

See https://docs.pytest.org/en/stable/reference/reference.html#request for details on the
underlying pytest ``request`` fixture.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from typing import Callable
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from tests.e2e import POLL_INTERVAL

RegisterCleanup = Callable[[Callable[[], None]], None]

# --- channel-sync e2e shared constants ---------------------------------------------------------
# The two-tag source fixture below carries the same channels under two ``asset_id`` tag values.
# csv_data  (18:00-18:09, temperature 20-29) is tagged asset_id=A.
# csv_data2 (18:10-18:19, temperature 30-39) is tagged asset_id=B.
SYNC_TAG_KEY = "asset_id"
SYNC_TAG_A = "A"
SYNC_TAG_B = "B"
# Sync window spanning BOTH files' data. Because the window covers A's and B's sub-ranges, a
# tag-filtered sync can only exclude the other tag via the tag filter (not the time window), and a
# cross-tag leak surfaces as extra points with the wrong tag's distinct values.
SYNC_WINDOW_START = int(datetime(2024, 9, 5, 18, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
SYNC_WINDOW_END = int(datetime(2024, 9, 5, 18, 20, tzinfo=timezone.utc).timestamp()) * 1_000_000_000

# --- channel-sync e2e: adversarial channel-type stress data ------------------------------------
# A generated dataset with the channel types that are annoying to migrate, so the hard export/stream
# code paths run end-to-end (not just the numeric happy path). Generated rather than a literal so the
# high-cardinality column is genuinely high-cardinality.
STRESS_ENUM_VALUES = ("nominal", "warning", "fault")
# Row count: high enough that the unique-per-row STRING column overflows the backend's enum-category
# limit (Compute:TooManyCategories) within a single bucket, forcing that channel onto the non-precise
# recursive-halving export fallback. The default 1-hour detection bucket keeps every row in one bucket
# (the window below is ~33 min) so the categories accumulate there. Bump this if a run shows the
# high-cardinality channel was still detected precise.
STRESS_ROWS = 2000
_STRESS_START = datetime(2024, 9, 5, 18, 0, tzinfo=timezone.utc)
STRESS_WINDOW_START = int(_STRESS_START.timestamp()) * 1_000_000_000
# One second per row; the window is half-open and ends one second past the last row.
STRESS_WINDOW_END = STRESS_WINDOW_START + STRESS_ROWS * 1_000_000_000


def make_stress_csv(rows: int = STRESS_ROWS) -> bytes:
    """Build a CSV exercising each migration-hard channel type, one row per second from 18:00.

    Columns:
    - ``hi_card_str``: a unique value per row (high-cardinality STRING -> TooManyCategories ->
      non-precise presence probe + per-channel recursive-halving export fallback).
    - ``enum_str``: a small repeating label set (low-cardinality enum STRING -> precise bucketed-enum
      counting; numeric-looking labels stay strings on re-read).
    - ``int_ch``: whole numbers (INT -> exercises the Float64->int recast so values land as INT).
    - ``dbl_ch``: integral-looking floats like ``42.0`` (DOUBLE -> exercises the Float64 guard so a
      double is not re-inferred/created as INT in the destination).
    """
    from datetime import timedelta

    lines = ["timestamp,hi_card_str,enum_str,int_ch,dbl_ch"]
    for i in range(rows):
        ts = (_STRESS_START + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        lines.append(f"{ts},id_{i},{STRESS_ENUM_VALUES[i % 3]},{i % 100},{float(i % 50)}")
    return ("\n".join(lines) + "\n").encode()


# Expected destination channel types after a correct round-trip.
STRESS_CHANNEL_TYPES = {
    "hi_card_str": "STRING",
    "enum_str": "STRING",
    "int_ch": "INT",
    "dbl_ch": "DOUBLE",
}


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
    parser.addoption(
        "--impersonation-source-user-rid",
        default=None,
        help="Source user RID for impersonation e2e test (must match the creator of source resources)",
    )
    parser.addoption(
        "--impersonation-dest-user-rid",
        default=None,
        help="Destination user RID to impersonate in impersonation e2e test",
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


@pytest.fixture
def register_cleanup(request) -> RegisterCleanup:
    """Schedule a callable to run after the current test, even on failure.

    Usage::

        register_cleanup(obj.archive)
    """

    def _register(fn: Callable[[], None]) -> None:
        request.addfinalizer(fn)

    return _register


@pytest.fixture
def migration_ctx(dest_client: NominalClient) -> MigrationContext:
    """A fresh MigrationContext with an empty MigrationState for each test."""
    return MigrationContext(
        destination_client=dest_client,
        migration_state=MigrationState(rid_mapping={}),
    )


@pytest.fixture
def ingested_source_dataset(
    source_client: NominalClient, csv_data: bytes, register_cleanup: RegisterCleanup
) -> Dataset:
    """A dataset on the source client, fully ingested from csv_data."""
    ds = source_client.create_dataset(f"migration-e2e-source-{uuid4().hex[:8]}")
    register_cleanup(ds.archive)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(interval=POLL_INTERVAL)
    return ds


@pytest.fixture
def source_dataset_two_tags(
    source_client: NominalClient,
    csv_data: bytes,
    csv_data2: bytes,
    register_cleanup: RegisterCleanup,
) -> Dataset:
    """A source dataset carrying the same channels under two ``asset_id`` tag values.

    ``add_from_io(..., tags=...)`` applies a tag uniformly to every point in a file, so ingesting the
    two CSVs with distinct tag values yields the same channels (temperature, humidity,
    relative_minutes) under ``asset_id=A`` and ``asset_id=B``. The tag values cover disjoint
    sub-ranges of the same window with distinct values (see the constants above), which makes a
    tag-filter leak visible while keeping the window the only thing that *doesn't* do the filtering.
    """
    ds = source_client.create_dataset(f"channel-sync-e2e-source-{uuid4().hex[:8]}")
    register_cleanup(ds.archive)
    ds.add_from_io(
        BytesIO(csv_data), "timestamp", "iso_8601", tags={SYNC_TAG_KEY: SYNC_TAG_A}
    ).poll_until_ingestion_completed(interval=POLL_INTERVAL)
    ds.add_from_io(
        BytesIO(csv_data2), "timestamp", "iso_8601", tags={SYNC_TAG_KEY: SYNC_TAG_B}
    ).poll_until_ingestion_completed(interval=POLL_INTERVAL)
    return ds


@pytest.fixture
def dest_dataset(dest_client: NominalClient, register_cleanup: RegisterCleanup) -> Dataset:
    """An empty dataset on the destination client; the write stream auto-creates series on first write."""
    ds = dest_client.create_dataset(f"channel-sync-e2e-dest-{uuid4().hex[:8]}")
    register_cleanup(ds.archive)
    return ds


@pytest.fixture
def source_dataset_stress(source_client: NominalClient, register_cleanup: RegisterCleanup) -> Dataset:
    """A source dataset of the migration-hard channel types (see :func:`make_stress_csv`).

    Untagged: this fixture exercises the channel-type code paths, while ``source_dataset_two_tags``
    covers the tag-filter dimension. Sync over ``[STRESS_WINDOW_START, STRESS_WINDOW_END)``.
    """
    ds = source_client.create_dataset(f"channel-sync-e2e-stress-{uuid4().hex[:8]}")
    register_cleanup(ds.archive)
    ds.add_from_io(BytesIO(make_stress_csv()), "timestamp", "iso_8601").poll_until_ingestion_completed(
        interval=POLL_INTERVAL
    )
    return ds
