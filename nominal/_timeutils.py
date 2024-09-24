from __future__ import annotations

from datetime import datetime, timezone
from typing import NamedTuple, TypeAlias

import dateutil.parser
import numpy as np
from typing_extensions import Self

from ._api.combined import ingest_api, scout_run_api

# defined here rather than ts.py to avoid circular imports
IntegralNanosecondsUTC: TypeAlias = int


class SecondsNanos(NamedTuple):
    seconds: int
    nanos: int

    def to_scout_run_api(self) -> scout_run_api.UtcTimestamp:
        return scout_run_api.UtcTimestamp(seconds_since_epoch=self.seconds, offset_nanoseconds=self.nanos)

    def to_ingest_api(self) -> ingest_api.UtcTimestamp:
        return ingest_api.UtcTimestamp(seconds_since_epoch=self.seconds, offset_nanoseconds=self.nanos)

    def to_iso8601(self) -> str:
        """datetime.datetime objects are only microsecond-precise, so we use numpy's datetime64[ns] for nanosecond precision."""
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
