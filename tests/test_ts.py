from __future__ import annotations

import time
from datetime import datetime

import dateutil.parser
import pytest

from nominal import ts


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
