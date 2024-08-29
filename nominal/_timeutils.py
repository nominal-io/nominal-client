from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api

IntegralNanosecondsUTC = int


@dataclass
class CustomTimestampFormat:
    format: str
    default_year: int = 0


_TimestampColumnType = (
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
    ]
    | CustomTimestampFormat
)


def _timestamp_type_to_conjure_ingest_api(
    ts_type: _TimestampColumnType,
) -> ingest_api.TimestampType:
    if isinstance(ts_type, CustomTimestampFormat):
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(
                custom_format=ingest_api.CustomTimestamp(format=ts_type.format, default_year=ts_type.default_year)
            )
        )
    elif ts_type == "iso_8601":
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))
    relation, unit = ts_type.split("_", 1)
    time_unit = ingest_api.TimeUnit[unit.upper()]
    if relation == "epoch":
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=ingest_api.EpochTimestamp(time_unit=time_unit))
        )
    elif relation == "relative":
        return ingest_api.TimestampType(relative=ingest_api.RelativeTimestamp(time_unit=time_unit))
    raise ValueError(f"invalid timestamp type: {ts_type}")


def _flexible_time_to_conjure_scout_run_api(
    timestamp: datetime | IntegralNanosecondsUTC,
) -> scout_run_api.UtcTimestamp:
    if isinstance(timestamp, datetime):
        seconds, nanos = _datetime_to_seconds_nanos(timestamp)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    elif isinstance(timestamp, IntegralNanosecondsUTC):
        seconds, nanos = divmod(timestamp, 1_000_000_000)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    raise TypeError(f"expected {datetime} or {IntegralNanosecondsUTC}, got {type(timestamp)}")


def _conjure_time_to_integral_nanoseconds(
    ts: scout_run_api.UtcTimestamp,
) -> IntegralNanosecondsUTC:
    return ts.seconds_since_epoch * 1_000_000_000 + (ts.offset_nanoseconds or 0)


def _datetime_to_seconds_nanos(dt: datetime) -> tuple[int, int]:
    dt = dt.astimezone(timezone.utc)
    seconds = int(dt.timestamp())
    nanos = dt.microsecond * 1000
    return seconds, nanos
