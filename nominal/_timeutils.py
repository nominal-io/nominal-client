from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Union

import dateutil.parser
from typing_extensions import TypeAlias  # typing.TypeAlias in 3.10+

from ._api.combined import ingest_api, scout_run_api

IntegralNanosecondsUTC = int


@dataclass
class CustomTimestampFormat:
    format: str
    default_year: int = 0


# Using Union rather than the "|" operator due to https://github.com/python/mypy/issues/11665.
TimestampColumnType: TypeAlias = Union[
    Literal[
        "iso_8601",
        "epoch_days",
        "epoch_hours",
        "epoch_minutes",
        "epoch_seconds",
        "epoch_milliseconds",
        "epoch_microseconds",
        "epoch_nanoseconds",
        "relative_days",
        "relative_hours",
        "relative_minutes",
        "relative_seconds",
        "relative_milliseconds",
        "relative_microseconds",
        "relative_nanoseconds",
    ],
    CustomTimestampFormat,
]


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
