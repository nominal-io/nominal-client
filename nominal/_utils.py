from __future__ import annotations

import logging
import mimetypes
from pathlib import Path
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, TypeVar, Union, Iterable
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api

if sys.version_info >= (3, 11):
    from typing import Self as Self
else:
    from typing_extensions import Self as Self


if sys.version_info >= (3, 10):
    from typing import TypeAlias as TypeAlias
else:
    from typing_extensions import TypeAlias as TypeAlias

logger = logging.getLogger(__name__)

IntegralNanosecondsUTC = int
T = TypeVar("T")


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


def _timestamp_type_to_conjure_ingest_api(
    ts_type: TimestampColumnType,
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


def construct_user_agent_string() -> str:
    """Constructs a user-agent string with system & Python metadata.
    E.g.: nominal-python/1.0.0b0 (macOS-14.4-arm64-arm-64bit) cpython/3.12.4
    """
    import importlib.metadata
    import platform
    import sys

    try:
        v = importlib.metadata.version("nominal")
        p = platform.platform()
        impl = sys.implementation
        py = platform.python_version()
        return f"nominal-python/{v} ({p}) {impl.name}/{py}"
    except Exception as e:
        # I believe all of the above are cross-platform, but just in-case...
        logger.error("failed to construct user-agent string", exc_info=e)
        return "nominal-python/unknown"


def update_dataclass(self: T, other: T, fields: Iterable[str]) -> None:
    """Update dataclass attributes, copying from `other` into `self`.

    Uses __dict__ to update `self` to update frozen dataclasses.
    """
    for field in fields:
        self.__dict__[field] = getattr(other, field)


def use_or_guess_mimetype(mimetype: str | None, path: Path | str, default: str = "application/octet-stream") -> str:
    # https://issues.apache.org/jira/browse/PARQUET-1889
    mimetypes.add_type("application/vnd.apache.parquet", ".parquet")
    if mimetype is None:
        mimetype, _encoding = mimetypes.guess_type(path)
        if mimetype is None:
            mimetype = default
    return mimetype
