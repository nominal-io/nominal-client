from __future__ import annotations

import math
import operator
from collections.abc import Mapping, Sequence

from google.protobuf.timestamp_pb2 import Timestamp

from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2 as writer_pb2
from nominal.protos.direct_channel_writer_common.v2.direct_channel_writer_common_pb2 import DoublePoints
from nominal.protos.domains.v1.axes_pb2 import AxisValues, TimeFrequencyAxisValues
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def _frequency_request(
    dataset_rid: str,
    channel_name: str,
    timestamps: Sequence[IntegralNanosecondsUTC],
    frequencies_hz: Sequence[float],
    values: Sequence[float],
    unit: str | None,
    tags: Mapping[str, str] | None,
) -> writer_pb2.WriteColumnBatchesRequest:
    """Build one bounded Frequency request without losing integer timestamp precision."""
    if not 1 <= len(timestamps) <= 10_000:
        raise ValueError("A Frequency write requires 1 to 10,000 points")
    if len(timestamps) != len(frequencies_hz) or len(timestamps) != len(values):
        raise ValueError("timestamps, frequencies_hz and values must have equal lengths")
    timestamps = list(timestamps)
    try:
        frequencies = [float(value) for value in frequencies_hz]
        points = [float(value) for value in values]
    except OverflowError as error:
        raise ValueError("frequencies_hz and values must be finite float64 values") from error
    if not all(math.isfinite(value) for value in frequencies + points):
        raise ValueError("frequencies_hz and values must be finite float64 values")

    proto_timestamps = []
    for timestamp in timestamps:
        if isinstance(timestamp, bool):
            raise TypeError("timestamps must be integral UTC nanoseconds, not booleans")
        nanos = operator.index(timestamp)
        if not 0 <= nanos <= 2**63 - 1:
            raise ValueError("timestamps must be between 0 and 2**63 - 1 UTC nanoseconds")
        seconds_nanos = _SecondsNanos.from_nanoseconds(nanos)
        proto_timestamps.append(Timestamp(seconds=seconds_nanos.seconds, nanos=seconds_nanos.nanos))

    batch = writer_pb2.ColumnBatch(
        channel=channel_name,
        tags=None if tags is None else dict(tags),
        unit=unit,
        canonical_axes=AxisValues(
            time_frequency=TimeFrequencyAxisValues(timestamps=proto_timestamps, frequencies=frequencies)
        ),
        values=writer_pb2.ColumnValues(doubles=DoublePoints(points=points)),
    )
    request = writer_pb2.WriteColumnBatchesRequest(data_source_rid=dataset_rid, batches=[batch])
    if request.ByteSize() > 262_144:
        raise ValueError("Frequency request exceeds 262,144 serialized bytes; send fewer points or less metadata")
    return request
