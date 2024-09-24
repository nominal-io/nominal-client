from __future__ import annotations

from datetime import datetime, timezone

import dateutil.parser
import numpy as np

from .ts import IntegralNanosecondsUTC
from ._api.combined import ingest_api, scout_run_api


def _flexible_time_to_conjure_scout_run_api(timestamp: datetime | IntegralNanosecondsUTC) -> scout_run_api.UtcTimestamp:
    seconds, nanos = _flexible_time_to_seconds_nanos(timestamp)
    return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)


def _flexible_time_to_conjure_ingest_api(
    timestamp: datetime | IntegralNanosecondsUTC,
) -> ingest_api.UtcTimestamp:
    seconds, nanos = _flexible_time_to_seconds_nanos(timestamp)
    return ingest_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)


def _flexible_time_to_seconds_nanos(
    timestamp: datetime | IntegralNanosecondsUTC,
) -> tuple[int, int]:
    if isinstance(timestamp, datetime):
        return _datetime_to_seconds_nanos(timestamp)
    elif isinstance(timestamp, IntegralNanosecondsUTC):
        return divmod(timestamp, 1_000_000_000)
    raise TypeError(f"expected {datetime} or {IntegralNanosecondsUTC}, got {type(timestamp)}")


def _conjure_time_to_integral_nanoseconds(ts: scout_run_api.UtcTimestamp) -> IntegralNanosecondsUTC:
    return ts.seconds_since_epoch * 1_000_000_000 + (ts.offset_nanoseconds or 0)


def _datetime_to_seconds_nanos(dt: datetime) -> tuple[int, int]:
    dt = dt.astimezone(timezone.utc)
    seconds = int(dt.timestamp())
    nanos = dt.microsecond * 1000
    return seconds, nanos


def _datetime_to_integral_nanoseconds(dt: datetime) -> IntegralNanosecondsUTC:
    seconds, nanos = _datetime_to_seconds_nanos(dt)
    return seconds * 1_000_000_000 + nanos


def _parse_timestamp(ts: str | datetime | IntegralNanosecondsUTC) -> IntegralNanosecondsUTC:
    if isinstance(ts, int):
        return ts
    if isinstance(ts, str):
        ts = dateutil.parser.parse(ts)
    return _datetime_to_integral_nanoseconds(ts)


def _flexible_to_iso8601(ts: datetime | IntegralNanosecondsUTC) -> str:
    """datetime.datetime objects are only microsecond-precise, so we use numpy's datetime64[ns] for nanosecond precision."""
    if isinstance(ts, datetime):
        return ts.astimezone(tz=timezone.utc).isoformat()
    if isinstance(ts, int):
        # np.datetime64[ns] assumes UTC
        return str(np.datetime64(ts, "ns")) + "Z"
    raise TypeError(f"timestamp {ts} must be a datetime or an integer")
