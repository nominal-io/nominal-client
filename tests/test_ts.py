from __future__ import annotations

import time
from datetime import datetime, timedelta

import dateutil.parser
import pytest

from nominal import ts
from nominal.ts import _to_api_duration


@pytest.mark.parametrize(
    "t",
    [
        ts._SecondsNanos.from_nanoseconds(time.time_ns()),
        ts._SecondsNanos.from_datetime(datetime.now()),
    ],
)
def test_time_conversions(t: ts._SecondsNanos):
    assert t.seconds == t.to_nanoseconds() // 1_000_000_000
    assert t.nanos == t.to_nanoseconds() % 1_000_000_000
    assert t == t.from_nanoseconds(t.to_nanoseconds())

    assert t.seconds == t.to_api().seconds
    assert t.nanos == t.to_api().nanos
    assert t == t.from_api(t.to_api())

    assert t.seconds == t.to_scout_run_api().seconds_since_epoch
    assert t.nanos == t.to_scout_run_api().offset_nanoseconds
    assert t == t.from_scout_run_api(t.to_scout_run_api())

    assert t.seconds == t.to_ingest_api().seconds_since_epoch
    assert t.nanos == t.to_ingest_api().offset_nanoseconds
    # no from_ingest_api method

    assert t == t.from_flexible(t.to_nanoseconds())

    # datetime objects don't have nanosecond precision
    assert t.seconds == int(dateutil.parser.parse(t.to_iso8601()).timestamp())
    assert t.seconds == t.from_flexible(dateutil.parser.parse(t.to_iso8601())).seconds
    assert t.seconds == t.from_flexible(t.to_iso8601()).seconds


@pytest.mark.parametrize(("value", "expected"), [("SECONDS", "seconds"), ("Nanoseconds", "nanoseconds")])
def test_str_to_literal_time_unit_normalizes_known_units(value: str, expected: str) -> None:
    """Known time-unit names are accepted case-insensitively and normalized to the SDK literal."""
    assert ts._str_to_literal_time_unit(value) == expected


def test_str_to_literal_time_unit_rejects_unknown_units() -> None:
    """A time-unit name the SDK doesn't recognize raises rather than passing through silently."""
    with pytest.raises(ValueError, match="Unknown time unit"):
        ts._str_to_literal_time_unit("WEEKS")


@pytest.mark.parametrize(
    "typed",
    [
        ts.Iso8601(),
        ts.Epoch("nanoseconds"),
        ts.Epoch("days"),
        ts.Relative("seconds", start=0),
        ts.Relative("microseconds", start=-1_500_000_001),  # negative offsets must survive seconds/nanos split
        ts.Custom("yyyy-DDD HH:mm:ss"),
        ts.Custom("DDD HH:mm:ss", default_year=2024, default_day_of_year=100),
    ],
)
def test_timestamp_type_proto_round_trip(typed: ts.TypedTimestampType) -> None:
    """Every timestamp type survives a lossless round trip through the proto encoding."""
    assert ts._proto_timestamp_type_to_typed_timestamp_type(typed._to_proto()) == typed


def test_epoch_converts_to_a_numeric_avro_timestamp_type() -> None:
    """An epoch type reaches the avro request as an epoch union arm carrying its unit."""
    converted = ts.Epoch("microseconds")._to_conjure_ingest_avro_api()

    assert converted.relative is None
    assert converted.epoch is not None
    assert converted.epoch.time_unit.value == "MICROSECONDS"


def test_relative_converts_to_a_numeric_avro_timestamp_type() -> None:
    """A relative type reaches the avro request as a relative arm carrying its unit and start."""
    converted = ts.Relative("seconds", start=1_700_000_000_000_000_000)._to_conjure_ingest_avro_api()

    assert converted.epoch is None
    assert converted.relative is not None
    assert converted.relative.time_unit.value == "SECONDS"
    assert converted.relative.offset == "2023-11-14T22:13:20.000000000Z"


@pytest.mark.parametrize(
    "typed",
    [
        pytest.param(ts.Iso8601(), id="iso8601"),
        pytest.param(ts.Custom("yyyy-DDD HH:mm:ss"), id="custom"),
    ],
)
def test_string_timestamp_types_cannot_describe_avro_timestamps(typed: ts.TypedTimestampType) -> None:
    """Avro timestamps are integers, so a string-formatted type has no representation there."""
    with pytest.raises(ValueError, match="not supported with .avro files"):
        typed._to_conjure_ingest_avro_api()


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        pytest.param(timedelta(seconds=1, microseconds=500_000), (1, 500_000_000), id="timedelta-positive"),
        pytest.param(1_500_000_000, (1, 500_000_000), id="int-positive"),
        pytest.param(timedelta(seconds=-1, microseconds=-500_000), (-2, 500_000_000), id="timedelta-negative"),
        pytest.param(-1_500_000_000, (-2, 500_000_000), id="int-negative"),
        pytest.param(timedelta(microseconds=-1), (-1, 999_999_000), id="timedelta-negative-sub-second"),
        pytest.param(-1_000, (-1, 999_999_000), id="int-negative-sub-second"),
        pytest.param(timedelta(days=-1), (-86_400, 0), id="timedelta-negative-whole-day"),
        pytest.param(timedelta(0), (0, 0), id="zero"),
    ],
)
def test_to_api_duration_encodes_the_same_split_for_both_input_types(duration, expected: tuple[int, int]) -> None:
    """The same duration must encode to the same (seconds, nanos) split however it is spelled.

    nanos stays in [0, 1e9) and seconds carries the sign, so a negative duration borrows a whole second
    rather than pairing a negative nanos with it.
    """
    api_duration = _to_api_duration(duration)

    assert (api_duration.seconds, api_duration.nanos) == expected
