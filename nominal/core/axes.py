from __future__ import annotations

import operator
from collections.abc import Sequence
from dataclasses import dataclass

from google.protobuf.timestamp_pb2 import Timestamp

from nominal.protos.domains.v1 import axes_pb2
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class TimeAxes:
    """Absolute time coordinates for `Dataset.write_points`.

    Args:
        timestamps: Integral UTC epoch nanoseconds, in the same order as the values.
            Duplicate timestamps are allowed.

    Example:
        ```python
        from nominal.core import TimeAxes

        axes = TimeAxes(timestamps=[1767268800000000123])
        ```
    """

    timestamps: Sequence[IntegralNanosecondsUTC]

    def _to_proto(self) -> axes_pb2.AxisValues:
        """Convert coordinates without rounding integral nanoseconds."""
        return axes_pb2.AxisValues(time=axes_pb2.TimeAxisValues(timestamps=_timestamps_to_proto(self.timestamps)))


@dataclass(frozen=True)
class TimeFrequencyAxes:
    """Capture times and frequency bins for `Dataset.write_points`.

    Args:
        timestamps: Integral UTC epoch nanoseconds, one per frequency bin.
            Repeat a capture timestamp for all bins in that capture.
        frequencies_hz: Frequencies in hertz, paired by index with timestamps and values.

    Example:
        ```python
        from nominal.core import TimeFrequencyAxes

        axes = TimeFrequencyAxes(
            timestamps=[1767268800000000123] * 3,
            frequencies_hz=[10.0, 20.0, 30.0],
        )
        ```
    """

    timestamps: Sequence[IntegralNanosecondsUTC]
    frequencies_hz: Sequence[float]

    def _to_proto(self) -> axes_pb2.AxisValues:
        """Convert aligned capture times and frequency bins to shared coordinates."""
        if len(self.timestamps) != len(self.frequencies_hz):
            raise ValueError("timestamps and frequencies_hz must have equal lengths")
        return axes_pb2.AxisValues(
            time_frequency=axes_pb2.TimeFrequencyAxisValues(
                timestamps=_timestamps_to_proto(self.timestamps), frequencies=self.frequencies_hz
            )
        )


def _timestamps_to_proto(timestamps: Sequence[IntegralNanosecondsUTC]) -> list[Timestamp]:
    """Preserve integral nanoseconds, including normalized negative timestamps."""
    result = []
    for timestamp in timestamps:
        if isinstance(timestamp, bool):
            raise TypeError("timestamps must be integral UTC nanoseconds, not booleans")
        seconds_nanos = _SecondsNanos.from_nanoseconds(operator.index(timestamp))
        result.append(Timestamp(seconds=seconds_nanos.seconds, nanos=seconds_nanos.nanos))
    return result
