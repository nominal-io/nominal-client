"""End-to-end tests verifying that every supported timestamp column format ingests correctly.

Each test:
  1. Builds a small CSV from a shared set of temperature readings (`temperature_data` fixture)
     using a format-specific timestamp formatter.
  2. Uploads the CSV to a freshly-created dataset via `_upload_and_assert`.
  3. Waits for ingestion to complete, then confirms the dataset metadata (name, description)
     and ingested time bounds round-trip correctly.

Timestamp formats under test:
  - ISO 8601 strings  ("iso_8601")
  - Epoch seconds     ("epoch_seconds")
  - Epoch milliseconds ("epoch_milliseconds")
  - Relative offset   (Relative("microseconds", epoch))
  - Custom formats    (ctime-style, IRIG day-of-year with default_year, HH:MM:SS with default date)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Callable

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.ts import Custom, Relative, _SecondsNanos
from tests.e2e import POLL_INTERVAL

Formatter = Callable[[int, datetime], str]


@pytest.fixture(scope="module")
def temperature_data() -> list[tuple[int, datetime]]:
    """Eight (temperature, timestamp) samples covering ~700 ms on 2024-09-30."""
    return [
        (20, datetime.fromisoformat("2024-09-30T16:37:36.891349+00:00")),
        (21, datetime.fromisoformat("2024-09-30T16:37:36.990262+00:00")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.089310+00:00")),
        (19, datetime.fromisoformat("2024-09-30T16:37:37.190015+00:00")),
        (23, datetime.fromisoformat("2024-09-30T16:37:37.289585+00:00")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.388941+00:00")),
        (28, datetime.fromisoformat("2024-09-30T16:37:37.491115+00:00")),
        (24, datetime.fromisoformat("2024-09-30T16:37:37.590826+00:00")),
    ]


def _create_csv_data(data: list[tuple[int, datetime]], formatter: Formatter) -> bytes:
    """Render `data` as a two-column CSV (temperature, timestamp) using `formatter` for the timestamp."""
    return ("temperature,timestamp\n" + "\n".join(formatter(temp, ts) for temp, ts in data)).encode()


def _truncate_to_ms(dt: datetime) -> datetime:
    """Truncate a datetime to millisecond precision (drop sub-millisecond microseconds)."""
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _assert_bounds(
    dataset_file: DatasetFile,
    dataset: Dataset,
    expected_start: datetime,
    expected_end: datetime,
    tolerance_ns: int = 0,
) -> None:
    """Assert ingested bounds at both the DatasetFile and Dataset levels.

    Args:
        dataset_file: The ingested file whose bounds to check (returned by poll_until_ingestion_completed).
        dataset: The parent dataset; refreshed in-place before its bounds are checked.
        expected_start: Expected lower bound of the ingested time range.
        expected_end: Expected upper bound of the ingested time range.
        tolerance_ns: Allowed absolute error in nanoseconds. Use 0 for exact match.
                      Set to ~1000 for epoch_seconds where float64 precision causes
                      sub-microsecond rounding errors.
    """
    expected_start_ns = _SecondsNanos.from_datetime(expected_start).to_nanoseconds()
    expected_end_ns = _SecondsNanos.from_datetime(expected_end).to_nanoseconds()

    assert dataset_file.bounds is not None
    assert dataset_file.bounds.start == pytest.approx(expected_start_ns, abs=tolerance_ns, rel=0)
    assert dataset_file.bounds.end == pytest.approx(expected_end_ns, abs=tolerance_ns, rel=0)

    dataset.refresh()
    assert dataset.bounds is not None
    assert dataset.bounds.start == pytest.approx(expected_start_ns, abs=tolerance_ns, rel=0)
    assert dataset.bounds.end == pytest.approx(expected_end_ns, abs=tolerance_ns, rel=0)


def _upload_and_assert(
    client: NominalClient,
    name: str,
    desc: str,
    csv_bytes: bytes,
    timestamp_type,
    expected_start: datetime,
    expected_end: datetime,
    tolerance_ns: int = 0,
) -> Dataset:
    """Create a dataset, upload csv_bytes, wait for ingestion, and assert metadata and bounds.

    The dataset is always archived in a finally block so resources are cleaned up even if
    an assertion fails mid-test.

    Args:
        client: Authenticated NominalClient for the test environment.
        name: Dataset name to verify against the returned metadata.
        desc: Dataset description to verify against the returned metadata.
        csv_bytes: Raw CSV bytes to upload via add_from_io.
        timestamp_type: Any value accepted by `add_from_io` (string literal, typed constant, or
                        Relative/Custom instance).
        expected_start: Expected ingested start timestamp.
        expected_end: Expected ingested end timestamp.
        tolerance_ns: Passed through to `_assert_bounds`; see that function for details.
    """
    ds = client.create_dataset(name, description=desc)
    try:
        dataset_file = ds.add_from_io(BytesIO(csv_bytes), "timestamp", timestamp_type).poll_until_ingestion_completed(
            interval=POLL_INTERVAL
        )
        assert ds.name == name
        assert ds.description == desc
        _assert_bounds(dataset_file, ds, expected_start, expected_end, tolerance_ns=tolerance_ns)
        return ds
    finally:
        ds.archive()


def test_iso_8601(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """ISO 8601 timestamps ingest correctly with sub-millisecond precision preserved."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}")
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        "iso_8601",
        temperature_data[0][1],
        temperature_data[-1][1],
    )


def test_epoch_seconds(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """Epoch-seconds timestamps ingest correctly; float64 CSV values have sub-microsecond rounding."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        "epoch_seconds",
        temperature_data[0][1],
        temperature_data[-1][1],
        tolerance_ns=1_000,
    )


def test_epoch_milliseconds(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """Epoch-milliseconds timestamps ingest correctly; sub-millisecond precision is truncated."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp() * 1000}")
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        "epoch_milliseconds",
        _truncate_to_ms(temperature_data[0][1]),
        _truncate_to_ms(temperature_data[-1][1]),
    )


def test_relative_microseconds(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """Relative(microseconds, epoch) translates integer offsets into absolute timestamps and ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    start = temperature_data[0][1]

    def _micros(ts: datetime) -> int:
        # Offset each sample by 1_000_000 µs (1 second) so the first CSV value is non-zero,
        # then add the actual delta from the start of the series.
        delta = ts - start
        return 1_000_000 + int(delta.total_seconds()) + delta.microseconds

    def fmt(temp: int, ts: datetime) -> str:
        return f"{temp},{_micros(ts)}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    # expected bounds are start + the integer µs values for the first and last samples
    expected_start = start + timedelta(microseconds=_micros(temperature_data[0][1]))
    expected_end = start + timedelta(microseconds=_micros(temperature_data[-1][1]))
    _upload_and_assert(client, name, desc, csv_bytes, Relative("microseconds", start), expected_start, expected_end)


def test_custom_ctime(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """A Custom format matching ctime output (e.g. "Mon Sep 30 16:37:36.891349 2024") ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # ctime() returns "Mon Sep 30 16:37:36 2024"; splice in microseconds before the year
        ctime = ts.ctime()
        ctime = ctime[:-5] + f".{ts.microsecond:06d}" + ctime[-5:]
        return f"{temp},{ctime}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        Custom(r"EEE MMM dd HH:mm:ss.SSSSSS yyyy"),
        temperature_data[0][1],
        temperature_data[-1][1],
    )


def test_custom_irig(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """A Custom format using IRIG day-of-year notation (e.g. "274:16:37:36.891349") ingests successfully."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # %j = zero-padded day of year (001–366)
        return f"{temp},{ts.strftime(r'%j:%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024),
        temperature_data[0][1],
        temperature_data[-1][1],
    )


def test_custom_day_of_year(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """A Custom format with only HH:MM:SS uses default_year and default_day_of_year to fill in the date."""
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        # Only the time-of-day component is written to CSV; the date is provided via defaults
        return f"{temp},{ts.strftime(r'%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    # default_year=2024, default_day_of_year=1 → Jan 1 2024; time-of-day comes from the CSV
    first_ts, last_ts = temperature_data[0][1], temperature_data[-1][1]
    expected_start = datetime(
        2024, 1, 1, first_ts.hour, first_ts.minute, first_ts.second, first_ts.microsecond, tzinfo=timezone.utc
    )
    expected_end = datetime(
        2024, 1, 1, last_ts.hour, last_ts.minute, last_ts.second, last_ts.microsecond, tzinfo=timezone.utc
    )
    _upload_and_assert(
        client,
        name,
        desc,
        csv_bytes,
        Custom(r"HH:mm:ss.SSSSSS", default_year=2024, default_day_of_year=1),
        expected_start,
        expected_end,
    )
