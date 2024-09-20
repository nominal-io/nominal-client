"""
# _AnyTimeDomain values

# _LiteralTimeDomain
"iso_8601"
"epoch_nanoseconds"
"epoch_microseconds"
"epoch_milliseconds"
"epoch_seconds"
"epoch_minutes"
"epoch_hours"
"relative_nanoseconds"  # <-- should we allow implicit offset=None?
"relative_microseconds"  # <-- should we allow implicit offset=None?
"relative_milliseconds"  # <-- should we allow implicit offset=None?
"relative_seconds"  # <-- should we allow implicit offset=None?
"relative_minutes"  # <-- should we allow implicit offset=None?
"relative_hours"  # <-- should we allow implicit offset=None?

# TypedTimeDomain constants - are these useful?
ISO_8601
EPOCH_NANOSECONDS
EPOCH_MICROSECONDS
EPOCH_MILLISECONDS
EPOCH_SECONDS
EPOCH_MINUTES
EPOCH_HOURS
RELATIVE_NANOSECONDS  # <-- should we allow implicit offset=None?
RELATIVE_MICROSECONDS  # <-- should we allow implicit offset=None?
RELATIVE_MILLISECONDS  # <-- should we allow implicit offset=None?
RELATIVE_SECONDS  # <-- should we allow implicit offset=None?
RELATIVE_MINUTES  # <-- should we allow implicit offset=None?
RELATIVE_HOURS  # <-- should we allow implicit offset=None?

# TypedTimeDomain
Epoch("nanoseconds")
Epoch("microseconds")
Epoch("milliseconds")
Epoch("seconds")
Epoch("minutes")
Epoch("hours")
Relative("nanoseconds")  # <-- should we allow implicit offset=None?
Relative("microseconds")  # <-- should we allow implicit offset=None?
Relative("milliseconds")  # <-- should we allow implicit offset=None?
Relative("seconds")  # <-- should we allow implicit offset=None?
Relative("minutes")  # <-- should we allow implicit offset=None?
Relative("hours")  # <-- should we allow implicit offset=None?
Relative("nanoseconds", offset=15)
Relative("microseconds", offset=15)
Relative("milliseconds", offset=15)
Relative("seconds", offset=15)
Relative("minutes", offset=15)
Relative("hours", offset=15)
Custom(r"yyyy-MM-dd[T]hh:mm:ss")
Custom(r"MM-dd[T]hh:mm:ss", default_year=2024)
"""

from __future__ import annotations
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping
from typing_extensions import TypeAlias
from nominal._api.combined import ingest_api

IntegralNanosecondsUTC: TypeAlias = int


@dataclass(frozen=True)
class Iso8601:
    pass


@dataclass(frozen=True)
class Epoch:
    unit: _LiteralTimeUnit


@dataclass(frozen=True)
class Relative:
    unit: _LiteralTimeUnit
    offset: int | None = None
    """Offset from the beginning of a data collection. The offset must be in the same units as the timestamp type itself."""
    # TODO(alkasm): is ^ true or is it just nanoseconds?
    # TODO(alkasm): may be okay with offset=0, but may need to detect presence?
    # the backend allows for None on the first upload, but may error on the second one
    # but if we default to 0, it may always work and overwrite - so None helps prevent?
    # or we can just disallow not specifying the offset


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
RELATIVE_NANOSECONDS = Relative("nanoseconds")
RELATIVE_MICROSECONDS = Relative("microseconds")
RELATIVE_MILLISECONDS = Relative("milliseconds")
RELATIVE_SECONDS = Relative("seconds")
RELATIVE_MINUTES = Relative("minutes")
RELATIVE_HOURS = Relative("hours")

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

_LiteralRelative: TypeAlias = Literal[
    "relative_nanoseconds",
    "relative_microseconds",
    "relative_milliseconds",
    "relative_seconds",
    "relative_minutes",
    "relative_hours",
]


TypedTimeDomain: TypeAlias = Iso8601 | Epoch | Relative | Custom
_LiteralTimeDomain: TypeAlias = _LiteralAbsolute | _LiteralRelative
_AnyTimeDomain: TypeAlias = TypedTimeDomain | _LiteralTimeDomain


def _make_typed_time_domain(domain: _AnyTimeDomain) -> TypedTimeDomain:
    if isinstance(domain, TypedTimeDomain):
        return domain
    if not isinstance(domain, str):
        raise TypeError(f"timestamp type {domain} must be a string or an instance of one of: {TypedTimeDomain}")
    if domain not in _str_to_type:
        raise ValueError(f"string time domains must be one of: {_str_to_type.keys()}")
    if domain.startswith("relative_"):
        # see TODO in class Relative if we want to deprecate implicit offset=None
        pass
    return _str_to_type[domain]


def _to_conjure_ingest_api(domain: TypedTimeDomain) -> ingest_api.TimestampType:
    if isinstance(domain, Iso8601):
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))
    if isinstance(domain, Epoch):
        epoch = ingest_api.EpochTimestamp(time_unit=domain.unit)
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=epoch))
    if isinstance(domain, Custom):
        fmt = ingest_api.CustomTimestamp(format=domain.format, default_year=domain.default_year)
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(custom_format=fmt))
    if isinstance(domain, Relative):
        relative = ingest_api.RelativeTimestamp(time_unit=domain.unit, offset=domain.offset)
        return ingest_api.TimestampType(relative=relative)
    raise TypeError(f"invalid time domain type: {type(domain)}")


_str_to_type: Mapping[_LiteralTimeDomain, Iso8601 | Epoch | Relative] = MappingProxyType(
    {
        "iso_8601": ISO_8601,
        "epoch_nanoseconds": EPOCH_NANOSECONDS,
        "epoch_microseconds": EPOCH_MICROSECONDS,
        "epoch_milliseconds": EPOCH_MILLISECONDS,
        "epoch_seconds": EPOCH_SECONDS,
        "epoch_minutes": EPOCH_MINUTES,
        "epoch_hours": EPOCH_HOURS,
        "relative_nanoseconds": RELATIVE_NANOSECONDS,
        "relative_microseconds": RELATIVE_MICROSECONDS,
        "relative_milliseconds": RELATIVE_MILLISECONDS,
        "relative_seconds": RELATIVE_SECONDS,
        "relative_minutes": RELATIVE_MINUTES,
        "relative_hours": RELATIVE_HOURS,
    }
)

"""
Exploration:

# winner: mix of strings + types, but advertise with singletons
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.iso_8601)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.absolute_nanoseconds)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Relative("nanoseconds", offset=15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Custom(r"yyyy-MM-dd[T]hh:mm:ss"))

# current: mix of strings + types: relative offsets not supported yet, would need to create a new type for it
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", "iso_8601")
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", "epoch_nanoseconds")
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", "relative_nanoseconds")
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.CustomTimestampFormat(r"yyyy-MM-dd[T]hh:mm:ss"))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.RelativeTimestampFormat("nanoseconds", offset=15))

# strongly typed: types and docstrings are extremely clear, allows more flexibility per-type
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Iso8601())
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Absolute("ns"))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Relative("nanoseconds", offset=15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Custom(r"yyyy-MM-dd[T]hh:mm:ss"))

# flexible factory function: multiple behaviors depending on how you call it; difficult to document but easy to read
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain("iso_8601"))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain("absolute", "ns"))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain("relative", "ns", offset=15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain("custom", r"yyyy-MM-dd[T]hh:mm:ss"))

# mix of strings + types, but advertise with singletons
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.iso_8601)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.absolute_nanoseconds)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Relative("nanoseconds", offset=15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.Custom(r"yyyy-MM-dd[T]hh:mm:ss"))

# factory functions for each type - pretty chill but wordier than strings
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.iso_8601())
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.absolute_nanoseconds())
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.relative_nanoseconds(offset=15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.custom(r"yyyy-MM-dd[T]hh:mm:ss"))

# singletons (with transforming class methods) - too magic, custom() behaves differently
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.iso_8601)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.absolute_nanoseconds)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.relative_nanoseconds)
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.relative_nanoseconds.offset(15))
dataset = upload_csv("path/to/file.csv", "dataset", "timestamp", nm.time_domain.custom(r"yyyy-MM-dd[T]hh:mm:ss"))
"""
