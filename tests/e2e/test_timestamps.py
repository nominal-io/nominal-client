"""End-to-end tests verifying that every supported timestamp column format ingests correctly.

Each test:
  1. Builds a small CSV from a shared set of temperature readings (`temperature_data` fixture)
     using a format-specific timestamp formatter.
  2. Uploads the CSV to a freshly-created dataset via `_upload_and_assert`.
  3. Waits for ingestion to complete, then confirms the dataset metadata (name, description)
     and ingested time bounds round-trip correctly.

Bounds are asserted in nanoseconds-since-epoch. Tolerances default to zero and are only
applied where the encoding path can't round-trip to ns precision; the parametrize table
on `test_bounds_round_trip` (and the per-case comments alongside each row) explain each
non-zero tolerance.

Timestamp formats under test:
  - ISO 8601 strings  ("iso_8601")
  - Epoch units       ("epoch_days", "epoch_hours", "epoch_minutes", "epoch_seconds",
                       "epoch_milliseconds", "epoch_microseconds", "epoch_nanoseconds")
  - Relative offset   (Relative("microseconds", epoch))
  - Custom formats    (ctime-style, IRIG day-of-year with default_year, HH:MM:SS with default date)
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from typing import Callable

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.ts import Custom, Relative, _SecondsNanos
from tests.e2e import POLL_INTERVAL

Formatter = Callable[[int, datetime], str]

# Float64 ULP at year-2024 timestamp magnitudes (~1.7e18 ns) is ~256 ns. Used for tests
# whose encoding crosses through float64 — either client-side (the float-encoded epoch
# formatters: days, hours, minutes, seconds) or server-side (epoch_nanoseconds, which
# the backend appears to parse through a float despite the integer-encoded input).
NS_PER_US = 1_000

# Backend silently truncates fractional epoch_milliseconds input to integer ms on
# ingest, contradicting the `Epoch` docstring's promise that "the timestamp can be
# integral or floating point". Tolerance reflects the actual ms-precision storage floor.
NS_PER_MS = 1_000_000


def _ns(dt: datetime) -> int:
    """Datetime → nanoseconds-since-epoch (exact for the µs-precision datetimes used here)."""
    return _SecondsNanos.from_datetime(dt).to_nanoseconds()


def _format_ctime_with_micros(temp: int, ts: datetime) -> str:
    """Format as ctime with microseconds spliced in before the year (e.g. 'Mon Sep 30 16:37:36.891349 2024')."""
    base = ts.ctime()
    return f"{temp},{base[:-5]}.{ts.microsecond:06d}{base[-5:]}"


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


def _assert_bounds(
    dataset_file: DatasetFile,
    dataset: Dataset,
    expected_start_ns: int,
    expected_end_ns: int,
    tolerance_ns: int = 0,
) -> None:
    """Assert ingested bounds at both the DatasetFile and Dataset levels match the given ns values.

    Args:
        dataset_file: The ingested file whose bounds to check.
        dataset: The parent dataset; refreshed in-place before its bounds are checked.
        expected_start_ns: Expected lower bound, in nanoseconds-since-epoch.
        expected_end_ns: Expected upper bound, in nanoseconds-since-epoch.
        tolerance_ns: Allowed absolute error in nanoseconds. Defaults to 0 (exact match).
                      Set to the format's intrinsic precision floor for formats whose
                      encoding can't round-trip to nanosecond precision.
    """
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
    expected_start_ns: int,
    expected_end_ns: int,
    tolerance_ns: int = 0,
) -> None:
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
        expected_start_ns: Expected ingested start timestamp, in nanoseconds-since-epoch.
        expected_end_ns: Expected ingested end timestamp, in nanoseconds-since-epoch.
        tolerance_ns: Passed through to `_assert_bounds`; see that function for details.
    """
    ds = client.create_dataset(name, description=desc)
    try:
        dataset_file = ds.add_from_io(BytesIO(csv_bytes), "timestamp", timestamp_type).poll_until_ingestion_completed(
            interval=POLL_INTERVAL
        )
        assert ds.name == name
        assert ds.description == desc
        _assert_bounds(dataset_file, ds, expected_start_ns, expected_end_ns, tolerance_ns=tolerance_ns)
    finally:
        ds.archive()


@pytest.mark.parametrize(
    ("formatter", "timestamp_type", "tolerance_ns"),
    [
        # iso_8601 — round-trips exactly at µs precision.
        pytest.param(
            lambda temp, ts: f"{temp},{ts.isoformat()}",
            "iso_8601",
            0,
            id="iso_8601",
        ),
        # epoch_days/hours/minutes/seconds — backend parses float-encoded epoch input via
        # float64 and stores the rounded value. At year-2024 timestamps (~1.7e18 ns) float64
        # ULP is ~256 ns; NS_PER_US is the next clean ceiling that absorbs that drift.
        pytest.param(
            lambda temp, ts: f"{temp},{ts.timestamp() / 86_400}",
            "epoch_days",
            NS_PER_US,
            id="epoch_days",
        ),
        pytest.param(
            lambda temp, ts: f"{temp},{ts.timestamp() / 3_600}",
            "epoch_hours",
            NS_PER_US,
            id="epoch_hours",
        ),
        pytest.param(
            lambda temp, ts: f"{temp},{ts.timestamp() / 60}",
            "epoch_minutes",
            NS_PER_US,
            id="epoch_minutes",
        ),
        pytest.param(
            lambda temp, ts: f"{temp},{ts.timestamp()}",
            "epoch_seconds",
            NS_PER_US,
            id="epoch_seconds",
        ),
        # epoch_milliseconds — backend silently truncates sub-ms fractional input to integer
        # ms (contradicting Epoch's "integral or floating point" docstring). NS_PER_MS bounds
        # the loss; observed drift in this fixture reaches 826 µs.
        pytest.param(
            lambda temp, ts: f"{temp},{ts.timestamp() * 1000}",
            "epoch_milliseconds",
            NS_PER_MS,
            id="epoch_milliseconds",
        ),
        # epoch_microseconds — integer µs string (~1.7e15) sits below float64's 2^53
        # integer-exact ceiling, so the backend's float funnel doesn't destroy precision;
        # round-trips exactly with integer input.
        pytest.param(
            lambda temp, ts: f"{temp},{int(ts.timestamp()) * 1_000_000 + ts.microsecond}",
            "epoch_microseconds",
            0,
            id="epoch_microseconds",
        ),
        # epoch_nanoseconds — even integer-encoded input is parsed via float64 by the backend
        # and quantized to a ~256 ns grid at year-2024 timestamps; NS_PER_US absorbs the drift.
        pytest.param(
            lambda temp, ts: f"{temp},{int(ts.timestamp()) * 1_000_000_000 + ts.microsecond * 1_000}",
            "epoch_nanoseconds",
            NS_PER_US,
            id="epoch_nanoseconds",
        ),
        # Custom(ctime-style) — full date + time string with µs; round-trips exactly.
        pytest.param(
            _format_ctime_with_micros,
            Custom(r"EEE MMM dd HH:mm:ss.SSSSSS yyyy"),
            0,
            id="custom_ctime",
        ),
        # Custom(IRIG day-of-year) — %j:HH:MM:SS.SSSSSS with default_year; round-trips exactly.
        pytest.param(
            lambda temp, ts: f"{temp},{ts.strftime(r'%j:%H:%M:%S.%f')}",
            Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024),
            0,
            id="custom_irig",
        ),
    ],
)
def test_bounds_round_trip(
    request,
    client: NominalClient,
    temperature_data: list[tuple[int, datetime]],
    formatter: Formatter,
    timestamp_type,
    tolerance_ns: int,
):
    """Upload `temperature_data` through `formatter`/`timestamp_type` and verify the ingested
    bounds match the µs-exact expected ns within `tolerance_ns`. See the parametrize table
    for per-case justifications; see the module docstring for the tolerance convention.
    """
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, formatter)
    _upload_and_assert(
        client=client,
        name=name,
        desc=desc,
        csv_bytes=csv_bytes,
        timestamp_type=timestamp_type,
        expected_start_ns=_ns(temperature_data[0][1]),
        expected_end_ns=_ns(temperature_data[-1][1]),
        tolerance_ns=tolerance_ns,
    )


def test_relative_microseconds(request, client: NominalClient, temperature_data: list[tuple[int, datetime]]):
    """Relative(microseconds, epoch) translates integer µs offsets to absolute timestamps."""
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
    start_ns = _ns(start)
    _upload_and_assert(
        client=client,
        name=name,
        desc=desc,
        csv_bytes=csv_bytes,
        timestamp_type=Relative(unit="microseconds", start=start),
        expected_start_ns=start_ns + _micros(temperature_data[0][1]) * 1_000,
        expected_end_ns=start_ns + _micros(temperature_data[-1][1]) * 1_000,
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
        client=client,
        name=name,
        desc=desc,
        csv_bytes=csv_bytes,
        timestamp_type=Custom(r"HH:mm:ss.SSSSSS", default_year=2024, default_day_of_year=1),
        expected_start_ns=_ns(expected_start),
        expected_end_ns=_ns(expected_end),
    )
