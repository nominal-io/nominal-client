from __future__ import annotations

from dataclasses import dataclass

from nominal_api import scout_catalog
from typing_extensions import Self

from nominal.ts import (
    IntegralNanosecondsUTC,
    _SecondsNanos,
)


@dataclass(frozen=True)
class Bounds:
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC

    @classmethod
    def _from_conjure(cls, bounds: scout_catalog.Bounds) -> Self:
        return cls(
            start=_SecondsNanos.from_api(bounds.start).to_nanoseconds(),
            end=_SecondsNanos.from_api(bounds.end).to_nanoseconds(),
        )
