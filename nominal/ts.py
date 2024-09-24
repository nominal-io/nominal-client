from __future__ import annotations

import abc
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Literal, Mapping, NamedTuple, Self, Union

import dateutil.parser
import numpy as np
from typing_extensions import TypeAlias

from ._api.combined import ingest_api, scout_run_api

__all__ = [
    "Iso8601",
    "Epoch",
    "Relative",
    "Custom",
    "ISO_8601",
    "EPOCH_NANOSECONDS",
    "EPOCH_MICROSECONDS",
    "EPOCH_MILLISECONDS",
    "EPOCH_SECONDS",
    "EPOCH_MINUTES",
    "EPOCH_HOURS",
    "TypedTimeDomain",
    "IntegralNanosecondsUTC",
]

IntegralNanosecondsUTC: TypeAlias = int
"""A timestamp in nanoseconds since the Unix epoch, UTC."""


class _ConjureTimestampDomain(abc.ABC):
    @abc.abstractmethod
    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        pass


@dataclass(frozen=True)
class Iso8601(_ConjureTimestampDomain):
    """ISO 8601 timestamp format, e.g. '2021-01-01T00:00:00Z' or '2021-01-01T00:00:00.123+00:00'.
    The time zone must be specified.
    """

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))


@dataclass(frozen=True)
class Epoch(_ConjureTimestampDomain):
    """An absolute timestamp in numeric format representing time since some epoch.
    The timestamp can be integral or floating point, e.g. 1612137600.123 for 2021-02-01T00:00:00.123Z.
    """

    unit: _LiteralTimeUnit

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        epoch = ingest_api.EpochTimestamp(time_unit=_time_unit_to_conjure(self.unit))
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=epoch))


@dataclass(frozen=True)
class Relative(_ConjureTimestampDomain):
    """A relative timestamp in numeric format representing time since some start time.
    The relative timestamp can be integral or floating point, e.g. 12.123 for 12 seconds and 123 milliseconds after start.
    The start time is absolute timestamp format representing time since some epoch.
    """

    unit: _LiteralTimeUnit
    start: datetime | IntegralNanosecondsUTC
    """The starting time to which all relatives times are relative to."""

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        """Note: The offset is a conjure datetime. They are serialized as ISO-8601 strings, with up-to nanosecond precision.
        The Python type for the field is just a str.
        Ref:
        - https://github.com/palantir/conjure/blob/master/docs/concepts.md#built-in-types
        - https://github.com/palantir/conjure/pull/1643
        """
        relative = ingest_api.RelativeTimestamp(
            time_unit=_time_unit_to_conjure(self.unit), offset=_SecondsNanos.from_flexible(self.start).to_iso8601()
        )
        return ingest_api.TimestampType(relative=relative)


@dataclass(frozen=True)
class Custom(_ConjureTimestampDomain):
    """A custom timestamp format. The custom timestamps are expected to be absolute timestamps.

    The format string should be in the format of the `DateTimeFormatter` class in Java.
    """

    format: str
    """Must be in the format of the `DateTimeFormatter` class in Java."""
    default_year: int | None = None
    """Accepted as an optional field for cases like IRIG time format, where the year is not present."""

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        fmt = ingest_api.CustomTimestamp(format=self.format, default_year=self.default_year)
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(custom_format=fmt))


# constants for pedagogy, documentation, default arguments, etc.
ISO_8601 = Iso8601()
EPOCH_NANOSECONDS = Epoch("nanoseconds")
EPOCH_MICROSECONDS = Epoch("microseconds")
EPOCH_MILLISECONDS = Epoch("milliseconds")
EPOCH_SECONDS = Epoch("seconds")
EPOCH_MINUTES = Epoch("minutes")
EPOCH_HOURS = Epoch("hours")

_LiteralTimeUnit: TypeAlias = Literal[
    "nanoseconds",
    "microseconds",
    "milliseconds",
    "seconds",
    "minutes",
    "hours",
]

_LiteralAbsolute: TypeAlias = Literal[
    "iso_8601",
    "epoch_nanoseconds",
    "epoch_microseconds",
    "epoch_milliseconds",
    "epoch_seconds",
    "epoch_minutes",
    "epoch_hours",
]

_LiteralRelativeDeprecated: TypeAlias = Literal[
    "relative_nanoseconds",
    "relative_microseconds",
    "relative_milliseconds",
    "relative_seconds",
    "relative_minutes",
    "relative_hours",
]

TypedTimeDomain: TypeAlias = Union[Iso8601, Epoch, Relative, Custom]
"""Strongly typed time domain types."""

_AnyTimeDomain: TypeAlias = Union[TypedTimeDomain, _LiteralAbsolute, _LiteralRelativeDeprecated]
"""All allowable time domain types, including string representations."""


def _make_typed_time_domain(domain: _AnyTimeDomain) -> TypedTimeDomain:
    if isinstance(domain, (Iso8601, Epoch, Relative, Custom)):
        return domain
    if not isinstance(domain, str):
        raise TypeError(f"timestamp type {domain} must be a string or an instance of one of: {TypedTimeDomain}")
    if domain.startswith("relative_"):
        # until this is completely removed, we implicitly assume offset=None in the APIs
        warnings.warn(
            "specifying 'relative_{unit}' as a string is deprecated and will be removed in a future version: use `nm.timedomain.Relative` instead. "
            "for example: instead of 'relative_seconds', use `nm.timedomain.Relative('seconds', start=datetime.now())`. ",
            UserWarning,
        )
    if domain not in _str_to_type:
        raise ValueError(f"string time domains must be one of: {_str_to_type.keys()}")
    return _str_to_type[domain]


def _time_unit_to_conjure(unit: _LiteralTimeUnit) -> ingest_api.TimeUnit:
    return ingest_api.TimeUnit[unit.upper()]


_str_to_type: Mapping[_LiteralAbsolute | _LiteralRelativeDeprecated, Iso8601 | Epoch | Relative] = MappingProxyType(
    {
        "iso_8601": ISO_8601,
        "epoch_nanoseconds": EPOCH_NANOSECONDS,
        "epoch_microseconds": EPOCH_MICROSECONDS,
        "epoch_milliseconds": EPOCH_MILLISECONDS,
        "epoch_seconds": EPOCH_SECONDS,
        "epoch_minutes": EPOCH_MINUTES,
        "epoch_hours": EPOCH_HOURS,
        "relative_nanoseconds": Relative("nanoseconds", start=0),
        "relative_microseconds": Relative("microseconds", start=0),
        "relative_milliseconds": Relative("milliseconds", start=0),
        "relative_seconds": Relative("seconds", start=0),
        "relative_minutes": Relative("minutes", start=0),
        "relative_hours": Relative("hours", start=0),
    }
)


class _SecondsNanos(NamedTuple):
    """A simple internal timestamp representation that can be converted to/from various formats.

    These represent nanosecond-precision epoch timestamps.
    """

    seconds: int
    nanos: int

    def to_scout_run_api(self) -> scout_run_api.UtcTimestamp:
        return scout_run_api.UtcTimestamp(seconds_since_epoch=self.seconds, offset_nanoseconds=self.nanos)

    def to_ingest_api(self) -> ingest_api.UtcTimestamp:
        return ingest_api.UtcTimestamp(seconds_since_epoch=self.seconds, offset_nanoseconds=self.nanos)

    def to_iso8601(self) -> str:
        """datetime.datetime is only microsecond-precise, so we use np.datetime64[ns] to get nanosecond-precision for printing.
        Note that nanosecond precision is the maximum allowable for conjure datetime fields.
        - https://github.com/palantir/conjure/blob/master/docs/concepts.md#built-in-types
        - https://github.com/palantir/conjure/pull/1643
        """
        return str(np.datetime64(self.to_integral_nanoseconds(), "ns")) + "Z"

    def to_integral_nanoseconds(self) -> IntegralNanosecondsUTC:
        return self.seconds * 1_000_000_000 + self.nanos

    @classmethod
    def from_scout_run_api(cls, ts: scout_run_api.UtcTimestamp) -> Self:
        return cls(seconds=ts.seconds_since_epoch, nanos=ts.offset_nanoseconds or 0)

    @classmethod
    def from_datetime(cls, dt: datetime) -> Self:
        dt = dt.astimezone(timezone.utc)
        seconds = int(dt.timestamp())
        nanos = dt.microsecond * 1000
        return cls(seconds, nanos)

    @classmethod
    def from_integral_nanoseconds(cls, ts: IntegralNanosecondsUTC) -> Self:
        seconds, nanos = divmod(ts, 1_000_000_000)
        return cls(seconds, nanos)

    @classmethod
    def from_flexible(cls, ts: str | datetime | IntegralNanosecondsUTC) -> Self:
        if isinstance(ts, int):
            return cls.from_integral_nanoseconds(ts)
        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)
        return cls.from_datetime(ts)
