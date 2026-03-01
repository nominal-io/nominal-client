"""End-to-end tests verifying that every supported timestamp column format ingests correctly.

Each test:
  1. Builds a small CSV from a shared set of temperature readings (`temperature_data` fixture)
     using a format-specific timestamp formatter.
  2. Uploads the CSV to a freshly-created dataset via `_upload_and_assert`.
  3. Waits for ingestion to complete, then confirms the dataset metadata (name, description)
     round-trips correctly and the ingest pipeline reports success.

Timestamp formats under test:
  - ISO 8601 strings  (both the string literal "iso_8601" and the typed ISO_8601 constant)
  - Epoch seconds     (both string "epoch_seconds" and typed EPOCH_SECONDS)
  - Epoch milliseconds (string "epoch_milliseconds")
  - Relative offset   (Relative("microseconds", epoch))
  - Custom formats    (ctime-style, IRIG day-of-year with default_year, HH:MM:SS with default date)
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.ts import EPOCH_SECONDS, ISO_8601, Custom, Relative
from tests.e2e import POLL_INTERVAL


@pytest.fixture(scope="module")
def temperature_data() -> list[tuple[int, datetime]]:
    """Eight (temperature, timestamp) samples covering ~700 ms on 2024-09-30."""
    return [
        (20, datetime.fromisoformat("2024-09-30T16:37:36.891349")),
        (21, datetime.fromisoformat("2024-09-30T16:37:36.990262")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.089310")),
        (19, datetime.fromisoformat("2024-09-30T16:37:37.190015")),
        (23, datetime.fromisoformat("2024-09-30T16:37:37.289585")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.388941")),
        (28, datetime.fromisoformat("2024-09-30T16:37:37.491115")),
        (24, datetime.fromisoformat("2024-09-30T16:37:37.590826")),
    ]


def _create_csv_data(data: list[tuple[int, datetime]], formatter) -> bytes:
    """Render `data` as a two-column CSV (temperature, timestamp) using `formatter` for the timestamp."""
    return ("temperature,timestamp\n" + "\n".join(formatter(temp, ts) for temp, ts in data)).encode()


def _upload_and_assert(
    client: NominalClient,
    name: str,
    desc: str,
    csv_bytes: bytes,
    timestamp_type,
) -> Dataset:
    """Create a dataset, upload csv_bytes, wait for ingestion, and assert metadata.

    The dataset is always archived in a finally block so resources are cleaned up even if
    an assertion fails mid-test.

    Args:
        client: Authenticated NominalClient for the test environment.
        name: Dataset name to verify against the returned metadata.
        desc: Dataset description to verify against the returned metadata.
        csv_bytes: Raw CSV bytes to upload via add_from_io.
        timestamp_type: Any value accepted by `add_from_io` (string literal, typed constant, or
                        Relative/Custom instance).
    """
    ds = client.create_dataset(name, description=desc)
    try:
        ds.add_from_io(BytesIO(csv_bytes), "timestamp", timestamp_type).poll_until_ingestion_completed(
            interval=POLL_INTERVAL
        )
        assert ds.name == name
        assert ds.description == desc
        return ds
    finally:
        ds.archive()


def test_iso_8601_str(request, client: NominalClient, temperature_data):
    """The string literal "iso_8601" is accepted as a timestamp type and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")
    _upload_and_assert(client, name, desc, csv_bytes, "iso_8601")


def test_iso_8601_typed(request, client: NominalClient, temperature_data):
    """The typed ISO_8601 constant is accepted as a timestamp type and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")
    _upload_and_assert(client, name, desc, csv_bytes, ISO_8601)


def test_epoch_seconds_str(request, client: NominalClient, temperature_data):
    """The string literal "epoch_seconds" is accepted as a timestamp type and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    _upload_and_assert(client, name, desc, csv_bytes, "epoch_seconds")


def test_epoch_seconds_typed(request, client: NominalClient, temperature_data):
    """The typed EPOCH_SECONDS constant is accepted as a timestamp type and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    _upload_and_assert(client, name, desc, csv_bytes, EPOCH_SECONDS)


def test_epoch_milliseconds_str(request, client: NominalClient, temperature_data):
    """The string literal "epoch_milliseconds" is accepted as a timestamp type and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp() * 1000}")
    _upload_and_assert(client, name, desc, csv_bytes, "epoch_milliseconds")


def test_relative_microseconds(request, client: NominalClient, temperature_data):
    """Relative(microseconds, epoch) translates integer offsets into absolute timestamps and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    start = temperature_data[0][1]

    def fmt(temp: int, ts: datetime) -> str:
        # Offset each sample by 1_000_000 µs (1 second) so the first CSV value is non-zero,
        # then add the actual delta from the start of the series.
        delta = ts - start
        micros = 1_000_000 + int(delta.total_seconds()) + delta.microseconds
        return f"{temp},{micros}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(client, name, desc, csv_bytes, Relative("microseconds", start))


def test_custom_ctime(request, client: NominalClient, temperature_data):
    """A Custom format matching ctime output (e.g. "Mon Sep 30 16:37:36.891349 2024") ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # ctime() returns "Mon Sep 30 16:37:36 2024"; splice in microseconds before the year
        ctime = ts.ctime()
        ctime = ctime[:-5] + f".{ts.microsecond:06d}" + ctime[-5:]
        return f"{temp},{ctime}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(client, name, desc, csv_bytes, Custom(r"EEE MMM dd HH:mm:ss.SSSSSS yyyy"))


def test_custom_irig(request, client: NominalClient, temperature_data):
    """A Custom format using IRIG day-of-year notation (e.g. "274:16:37:36.891349") ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # %j = zero-padded day of year (001–366)
        return f"{temp},{ts.strftime(r'%j:%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(client, name, desc, csv_bytes, Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024))


def test_custom_day_of_year(request, client: NominalClient, temperature_data):
    """A Custom format with only HH:MM:SS uses default_year and default_day_of_year to fill in the date."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # Only the time-of-day component is written to CSV; the date is provided via defaults
        return f"{temp},{ts.strftime(r'%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(
        client, name, desc, csv_bytes, Custom(r"HH:mm:ss.SSSSSS", default_year=2024, default_day_of_year=1)
    )
