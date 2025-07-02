"""The `nominal.ts` module provides timestamp format specifications and utilities.

When you _upload_ a dataset to nominal, the dataset may have timestamps in a variety of formats. For example:

- ISO 8601 strings like '2021-01-31T19:00:00Z'
- Epoch timestamps in floating-point seconds since epoch like 1612137600.123
- Epoch timestamps in integer nanoseconds since epoch like 1612137600123000000
- Relative timestamps like 12.123 for 12 seconds and 123 milliseconds after some start time
- Various other string timestamp formats, e.g. 'Sun Jan 31 19:00:00 2021'

All of these may also have different interpretations of the units, epoch, time zone, etc.

To simplify common usages while allowing for the full flexibility of the Nominal platform,
the client library allows you to specify timestamp formats with simple strings and more complex typeful representations.

Wherever you can specify a timestamp format (typically the `timestamp_type` parameter), these are all examples of
valid formats:

```python
"iso_8601"
"epoch_nanoseconds"
"epoch_microseconds"
"epoch_milliseconds"
"epoch_seconds"
"epoch_minutes"
"epoch_hours"
nm.ts.Iso8601()
nm.ts.Epoch("microseconds")
nm.ts.Epoch("seconds")
nm.ts.Epoch("hours")
nm.ts.Relative("nanoseconds", start=datetime.fromisoformat("2021-01-31T19:00:00Z"))
nm.ts.Relative("milliseconds", start=datetime.fromisoformat("2021-01-31T19:00:00Z"))
nm.ts.Relative("seconds", start=datetime.fromisoformat("2021-01-31T19:00:00Z"))
nm.ts.Relative("minutes", start=datetime.fromisoformat("2021-01-31T19:00:00Z"))
nm.ts.Custom(r"yyyy-MM-dd[T]hh:mm:ss")
nm.ts.Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024)
```

The strings `"iso_8601"` and `"epoch_{unit}"` are equivalent to using the types `nm.ts.Iso8601()` and
`nm.ts.Epoch("{unit}")`.

Relative and custom formats require additional parameters, so they can't be specified with a string.
Relative timestamps require a start time that they are relative to, e.g. `nm.ts.Relative("{unit}", start=start_time)`.
Custom timestamp formats require a format string compatible with the `DateTimeFormatter` class in Java: see java
[docs](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns).

## Examples

All of the examples use the same data (timestamp and value) expressed with different timestamp formats,
and showcase how to upload them to Nominal.

### ISO 8601

Nominal requires ISO 8601 timestamps to include the time zone, e.g. `'2021-01-31T19:00:00Z'` or
`'2021-01-31T19:00:00.123+00:00'`. For example:

```csv
temperature,timestamp
20,2024-09-30T16:37:36.891349Z
21,2024-09-30T16:37:36.990262Z
22,2024-09-30T16:37:37.089310Z
19,2024-09-30T16:37:37.190015Z
23,2024-09-30T16:37:37.289585Z
22,2024-09-30T16:37:37.388941Z
28,2024-09-30T16:37:37.491115Z
24,2024-09-30T16:37:37.590826Z
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type="iso_8601"  # or nm.ts.Iso8601()
)
```

### Epoch timestamps

Nominal supports epoch timestamps in different units:
hours, minutes, seconds, milliseconds, microseconds, and nanoseconds.
The values can be integers or floating-point numbers.

#### Floating-point seconds since epoch

```csv
temperature,timestamp
20,1727728656.891349
21,1727728656.990262
22,1727728657.08931
19,1727728657.190015
23,1727728657.289585
22,1727728657.388941
28,1727728657.491115
24,1727728657.590826
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type="epoch_seconds"  # or nm.ts.Epoch("seconds")
)
```

#### Integer nanoseconds since epoch

```csv
temperature,timestamp
20,1727728656891349000
21,1727728656990262000
22,1727728657089310000
19,1727728657190015000
23,1727728657289585000
22,1727728657388941000
28,1727728657491115000
24,1727728657590826000
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type="epoch_nanoseconds"  # or nm.ts.Epoch("nanoseconds")
)
```

### Relative timestamps

Similar to epoch timestamps, Nominal supports relative timestamps in the same units:
hours, minutes, seconds, milliseconds, microseconds, and nanoseconds, and can be integer or floating-point values.
Relative timestamps are _relative to_ a specified start time.

```csv
temperature,timestamp
20,0
21,98913
22,197961
19,298666
23,398236
22,497592
28,599766
24,699477
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type=nm.ts.Relative("microseconds", since=datetime.fromtimestamp(1727728656.891349))
)
```

### Custom Format

Nominal supports custom timestamp formats. The format string should be in the format of the `DateTimeFormatter`
class in Java: see java
[docs](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns).

#### Customized ctime

This time format is similar to the string format from `ctime()`, except with microsecond precision added.

```csv
temperature,timestamp
20,Mon Sep 30 16:37:36.891349 2024
21,Mon Sep 30 16:37:36.990262 2024
22,Mon Sep 30 16:37:37.089310 2024
19,Mon Sep 30 16:37:37.190015 2024
23,Mon Sep 30 16:37:37.289585 2024
22,Mon Sep 30 16:37:37.388941 2024
28,Mon Sep 30 16:37:37.491115 2024
24,Mon Sep 30 16:37:37.590826 2024
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type=nm.ts.Custom("EEE MMM dd HH:mm:ss.SSSSSS yyyy")
)
```

#### IRIG time code

IRIG time codes come in a variety of formats. A common IRIG format specifies a relative timestamp from the
beginning of the year, expressed in `days:hours:minutes:seconds.ms`.

```csv
temperature,timestamp
20,274:16:37:36.891349
21,274:16:37:36.990262
22,274:16:37:37.089310
19,274:16:37:37.190015
23,274:16:37:37.289585
22,274:16:37:37.388941
28,274:16:37:37.491115
24,274:16:37:37.590826
```

```python
nm.upload_csv("temperature.csv", "Exterior Temps", "timestamp",
    timestamp_type=nm.ts.Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024)
)
```
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Literal, Mapping, NamedTuple, Union

import dateutil.parser
from nominal_api import api, ingest_api, scout_run_api
from typing_extensions import Self, TypeAlias

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
    "TypedTimestampType",
    "IntegralNanosecondsUTC",
    "IntegralNanosecondsDuration",
    "LogTimestampType",
]

IntegralNanosecondsUTC: TypeAlias = int
"""Alias for an `int` used in the code for documentation purposes.
This value is a timestamp in nanoseconds since the Unix epoch, UTC."""

IntegralNanosecondsDuration: TypeAlias = int
"""Alias for an `int` used in the code for documentation purposes.
This value is a duration measured in nanoseconds."""

LogTimestampType: TypeAlias = Literal["absolute", "relative"]


class _ConjureTimestampType(abc.ABC):
    @abc.abstractmethod
    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        pass


@dataclass(frozen=True)
class Iso8601(_ConjureTimestampType):
    """ISO 8601 timestamp format, e.g. '2021-01-31T19:00:00Z' or '2021-01-31T19:00:00.123+00:00'.
    The time zone must be specified.
    """

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))


@dataclass(frozen=True)
class Epoch(_ConjureTimestampType):
    """An absolute timestamp in numeric format representing time since some epoch.
    The timestamp can be integral or floating point, e.g. 1612137600.123 for 2021-02-01T00:00:00.123Z.
    """

    unit: _LiteralTimeUnit

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        epoch = ingest_api.EpochTimestamp(time_unit=_time_unit_to_conjure(self.unit))
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=epoch))


@dataclass(frozen=True)
class Relative(_ConjureTimestampType):
    """A relative timestamp in numeric format representing time since some start time.
    The relative timestamp can be integral or floating point, e.g. 12.123 for 12 seconds and 123 ms after start.
    The start time is absolute timestamp format representing time since some epoch.
    """

    unit: _LiteralTimeUnit
    start: datetime | IntegralNanosecondsUTC
    """The starting time to which all relatives times are relative to."""

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        """Note: The offset is a conjure datetime. They are serialized as ISO-8601 strings, with up-to nanosecond prec.
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
class Custom(_ConjureTimestampType):
    """A custom timestamp format. The custom timestamps are expected to be absolute timestamps.

    The format string should be in the format of the `DateTimeFormatter` class in Java.
    Ref: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns
    """

    format: str
    """Must be in the format of the `DateTimeFormatter` class in Java."""
    default_year: int | None = None
    """Accepted as an optional field for cases like IRIG time codes, where the year is not present."""
    default_day_of_year: int | None = None
    """Accepted as an optional field for cases where the day of the year is not present."""

    def _to_conjure_ingest_api(self) -> ingest_api.TimestampType:
        fmt = ingest_api.CustomTimestamp(
            format=self.format,
            default_year=self.default_year,
            default_day_of_year=self.default_day_of_year,
        )
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

TypedTimestampType: TypeAlias = Union[Iso8601, Epoch, Relative, Custom]
"""Type alias for all of the strongly typed timestamp types."""

_AnyTimestampType: TypeAlias = Union[TypedTimestampType, _LiteralAbsolute]
"""Type alias for all of the allowable timestamp types, including string representations."""


def _to_typed_timestamp_type(type_: _AnyTimestampType) -> TypedTimestampType:
    if isinstance(type_, (Iso8601, Epoch, Relative, Custom)):
        return type_
    if not isinstance(type_, str):
        raise TypeError(f"timestamp type {type_} must be a string or an instance of one of: {TypedTimestampType}")
    if type_ not in _str_to_type:
        raise ValueError(f"string timestamp types must be one of: {_str_to_type.keys()}")
    return _str_to_type[type_]


def _time_unit_to_conjure(unit: _LiteralTimeUnit) -> api.TimeUnit:
    return api.TimeUnit[unit.upper()]


_str_to_type: Mapping[_LiteralAbsolute, Iso8601 | Epoch | Relative] = MappingProxyType(
    {
        "iso_8601": ISO_8601,
        "epoch_nanoseconds": EPOCH_NANOSECONDS,
        "epoch_microseconds": EPOCH_MICROSECONDS,
        "epoch_milliseconds": EPOCH_MILLISECONDS,
        "epoch_seconds": EPOCH_SECONDS,
        "epoch_minutes": EPOCH_MINUTES,
        "epoch_hours": EPOCH_HOURS,
    }
)

_InferrableTimestampType: TypeAlias = Union[str, datetime, IntegralNanosecondsUTC]
"""Timestamp types that can be converted to a _SecondsNanos using flexible deduction"""


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

    def to_api(self) -> api.Timestamp:
        return api.Timestamp(seconds=self.seconds, nanos=self.nanos)

    def to_iso8601(self) -> str:
        """To an iso8601 string with nanosecond precision.

        Note that nanos precision is the maximum allowable for conjure datetime fields.
        - https://github.com/palantir/conjure/blob/master/docs/concepts.md#built-in-types
        - https://github.com/palantir/conjure/pull/1643
        """
        # datetimes are only microsecond precise, so manually add in the nanos
        dt_s = datetime.fromtimestamp(self.seconds, timezone.utc)
        return f"{dt_s.strftime('%Y-%m-%dT%H:%M:%S')}.{self.nanos:09d}Z"

    def to_nanoseconds(self) -> IntegralNanosecondsUTC:
        return self.seconds * 1_000_000_000 + self.nanos

    def to_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.seconds + self.nanos * 1e-9, timezone.utc)

    @classmethod
    def from_scout_run_api(cls, ts: scout_run_api.UtcTimestamp) -> Self:
        return cls(seconds=ts.seconds_since_epoch, nanos=ts.offset_nanoseconds or 0)

    @classmethod
    def from_api(cls, timestamp: api.Timestamp) -> Self:
        # TODO(alkasm): warn on pico-second precision loss
        return cls(timestamp.seconds, timestamp.nanos)

    @classmethod
    def from_datetime(cls, dt: datetime) -> Self:
        dt = dt.astimezone(timezone.utc)
        seconds = int(dt.timestamp())
        nanos = dt.microsecond * 1000
        return cls(seconds, nanos)

    @classmethod
    def from_nanoseconds(cls, ts: IntegralNanosecondsUTC) -> Self:
        seconds, nanos = divmod(ts, 1_000_000_000)
        return cls(seconds, nanos)

    @classmethod
    def from_flexible(cls, ts: _InferrableTimestampType) -> Self:
        if isinstance(ts, int):
            return cls.from_nanoseconds(ts)
        if isinstance(ts, str):
            # TODO(drake-nominal): by involving dateutil, this chops off any nano level precision provided
            #                      in the timestamp. Update to not lose precision when converting to absolute
            #                      nanos.
            ts = dateutil.parser.parse(ts)
        return cls.from_datetime(ts)


_MIN_TIMESTAMP = _SecondsNanos(seconds=0, nanos=0)
_MAX_TIMESTAMP = _SecondsNanos(seconds=9223372036, nanos=854775807)
"""
The maximum valid timestamp that can be represented in the APIs: 2262-04-11 19:47:16.854775807.
The backend converts to long nanoseconds, and the maximum long (int64) value is 9,223,372,036,854,775,807.
"""


def _to_api_duration(duration: timedelta | IntegralNanosecondsDuration) -> scout_run_api.Duration:
    if isinstance(duration, timedelta):
        return scout_run_api.Duration(seconds=int(duration.total_seconds()), nanos=duration.microseconds * 1000)
    else:
        seconds, nanos = divmod(duration, 1_000_000_000)
        return scout_run_api.Duration(seconds=seconds, nanos=nanos)
