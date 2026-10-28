from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api import datasource, scout_catalog

from nominal.protos.datasets.v1 import dataset_files_pb2
from nominal.ts import (
    IntegralNanosecondsUTC,
    _SecondsNanos,
)


@dataclass(frozen=True)
class Bounds:
    """Start and end timestamps of a time interval, in UTC nanoseconds."""

    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC

    @classmethod
    def _from_conjure(cls, bounds: scout_catalog.Bounds) -> Self:
        return cls(
            start=_SecondsNanos.from_api(bounds.start).to_nanoseconds(),
            end=_SecondsNanos.from_api(bounds.end).to_nanoseconds(),
        )

    def _to_conjure(self) -> scout_catalog.Bounds:
        return scout_catalog.Bounds(
            type=datasource.TimestampType.ABSOLUTE,
            start=_SecondsNanos.from_nanoseconds(self.start).to_api(),
            end=_SecondsNanos.from_nanoseconds(self.end).to_api(),
        )

    @classmethod
    def _from_proto(cls, bounds: dataset_files_pb2.DataBounds) -> Self:
        return cls(start=bounds.start.ToNanoseconds(), end=bounds.end.ToNanoseconds())

    def _to_proto(self) -> dataset_files_pb2.DataBounds:
        start = Timestamp()
        start.FromNanoseconds(self.start)
        end = Timestamp()
        end.FromNanoseconds(self.end)
        return dataset_files_pb2.DataBounds(start=start, end=end, time_base=dataset_files_pb2.TIME_BASE_ABSOLUTE)
