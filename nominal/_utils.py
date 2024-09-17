from __future__ import annotations

import logging
import mimetypes
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Literal, NamedTuple, Type, TypeVar, Union

import dateutil.parser
from typing_extensions import TypeAlias  # typing.TypeAlias in 3.10+

from ._api.combined import ingest_api, scout_run_api

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


class FileType(NamedTuple):
    extension: str
    mimetype: str

    @classmethod
    def from_path(cls, path: Path | str, default_mimetype: str = "application/octect-stream") -> FileType:
        ext = "".join(Path(path).suffixes)
        mimetype, _encoding = mimetypes.guess_type(path)
        if mimetype is None:
            return cls(ext, default_mimetype)
        return cls(ext, mimetype)

    @classmethod
    def from_path_dataset(cls, path: Path | str) -> FileType:
        path_string = str(path) if isinstance(path, Path) else path
        if path_string.endswith(".csv"):
            return FileTypes.CSV
        if path_string.endswith(".csv.gz"):
            return FileTypes.CSV_GZ
        if path_string.endswith(".parquet"):
            return FileTypes.PARQUET
        raise ValueError(f"dataset path '{path}' must end in .csv, .csv.gz, or .parquet")


class FileTypes:
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    MP4: FileType = FileType(".mp4", "video/mp4")
    BINARY: FileType = FileType("", "application/octet-stream")


@contextmanager
def reader_writer() -> Iterator[tuple[BinaryIO, BinaryIO]]:
    rd, wd = os.pipe()
    r = open(rd, "rb")
    w = open(wd, "wb")
    try:
        yield r, w
    finally:
        w.close()
        r.close()
