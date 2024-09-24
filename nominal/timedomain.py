from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Literal, Mapping, Union

from typing_extensions import TypeAlias

from nominal._timeutils import _flexible_to_iso8601

from ._api.combined import ingest_api
from ._timeutils import IntegralNanosecondsUTC


@dataclass(frozen=True)
class Iso8601:
    pass


@dataclass(frozen=True)
class Epoch:
    unit: _LiteralTimeUnit


@dataclass(frozen=True)
class Relative:
    unit: _LiteralTimeUnit
    start: datetime | IntegralNanosecondsUTC
    """The starting time to which all relatives times are relative to."""


@dataclass(frozen=True)
class Custom:
    format: str
    default_year: int | None = None


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
_AnyTimeDomain: TypeAlias = Union[TypedTimeDomain, _LiteralAbsolute, _LiteralRelativeDeprecated]


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


def _time_domain_to_conjure_ingest_api(domain: TypedTimeDomain) -> ingest_api.TimestampType:
    """
    Note: datetimes are serialized a ISO-8601 strings, with up-to nanosecond precision. Ref:
    - https://github.com/palantir/conjure/blob/master/docs/concepts.md#built-in-types
    - https://github.com/palantir/conjure/pull/1643
    """

    if isinstance(domain, Iso8601):
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))
    if isinstance(domain, Epoch):
        epoch = ingest_api.EpochTimestamp(time_unit=_time_unit_to_conjure(domain.unit))
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=epoch))
    if isinstance(domain, Custom):
        fmt = ingest_api.CustomTimestamp(format=domain.format, default_year=domain.default_year)
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(custom_format=fmt))
    if isinstance(domain, Relative):
        relative = ingest_api.RelativeTimestamp(
            time_unit=_time_unit_to_conjure(domain.unit), offset=_flexible_to_iso8601(domain.start)
        )
        return ingest_api.TimestampType(relative=relative)
    raise TypeError(f"invalid time domain type: {type(domain)}")


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
