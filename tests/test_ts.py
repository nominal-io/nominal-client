import time
from datetime import datetime

import pytest

import nominal as nm


@pytest.mark.parametrize(
    "t",
    [
        nm.ts._SecondsNanos.from_nanoseconds(time.time_ns()),
        nm.ts._SecondsNanos.from_datetime(datetime.now()),
    ],
)
def test_time_conversions(t: nm.ts._SecondsNanos):
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

    assert t == t.from_flexible(t.to_iso8601())
    assert t == t.from_flexible(t.to_nanoseconds())

    # datetime objects don't have nanosecond precision
    assert t.seconds == int(datetime.fromisoformat(t.to_iso8601()).timestamp())
    assert t.seconds == t.from_flexible(datetime.fromisoformat(t.to_iso8601())).seconds
