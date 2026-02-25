from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from typing import Sequence, cast

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core._columnar_write_pb2 import (
    DoublePoints,
    IntPoints,
    Points,
    RecordsBatch,
    StringPoints,
    Timestamp,
    WriteBatchesRequest,
)
from nominal.core._stream.write_stream import BatchItem, DataItem, PointType, StreamValueType
from nominal.core._utils.queueing import Batch
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class SerializedBatch:
    """Result of batch serialization containing the protobuf data and timestamp bounds."""

    data: bytes  # Serialized protobuf data
    oldest_timestamp: IntegralNanosecondsUTC  # Oldest timestamp in the batch
    newest_timestamp: IntegralNanosecondsUTC  # Newest timestamp in the batch


def make_points_proto(api_batch: Sequence[DataItem]) -> Points:
    """Create columnar Points protobuf for a batch of items with the same value type.

    Uses the centralized PointType inference from BatchItem.get_point_type().
    All items in the batch are assumed to have the same type (enforced by grouping).
    """
    point_type = api_batch[0].get_point_type()
    timestamps = [_make_timestamp(item.timestamp) for item in api_batch]

    match point_type:
        case PointType.DOUBLE:
            return Points(
                timestamps=timestamps,
                double_points=DoublePoints(points=[cast(float, item.value) for item in api_batch]),
            )
        case PointType.STRING:
            return Points(
                timestamps=timestamps,
                string_points=StringPoints(points=[cast(str, item.value) for item in api_batch]),
            )
        case PointType.INT:
            return Points(
                timestamps=timestamps,
                int_points=IntPoints(points=[cast(int, item.value) for item in api_batch]),
            )
        case PointType.DOUBLE_ARRAY | PointType.STRING_ARRAY:
            raise ValueError(
                f"Array types ({point_type}) are not supported by the columnar protobuf endpoint. "
                "Use data_format='json' for array streaming."
            )
        case _:
            raise ValueError(f"Unsupported point type: {point_type}")


def create_write_request(batch: Sequence[DataItem], data_source_rid: str = "") -> WriteBatchesRequest:
    """Create a WriteBatchesRequest in columnar format from batches of items."""
    api_batched = groupby(sorted(batch, key=BatchItem.sort_key), key=BatchItem.sort_key)
    api_batches = [list(api_batch) for _, api_batch in api_batched]
    return WriteBatchesRequest(
        data_source_rid=data_source_rid,
        batches=[
            RecordsBatch(
                channel=api_batch[0].channel_name,
                points=make_points_proto(api_batch),
                tags=api_batch[0].tags or {},
            )
            for api_batch in api_batches
        ],
    )


def process_batch(
    batch: Sequence[DataItem],
    nominal_data_source_rid: str | None,
    auth_header: str,
    proto_write: ProtoWriteService,
) -> None:
    """Process a batch of data items to write via the columnar endpoint."""
    if nominal_data_source_rid is None:
        raise ValueError("Writing not implemented for this connection type")

    request = create_write_request(batch, nominal_data_source_rid)

    proto_write.write_nominal_columnar_batches(
        auth_header=auth_header,
        request=request.SerializeToString(),
    )


def serialize_batch(batch: Batch[StreamValueType], data_source_rid: str = "") -> SerializedBatch:
    """Process a batch of items and return serialized request."""
    request = create_write_request(batch.items, data_source_rid)
    return SerializedBatch(
        data=request.SerializeToString(),
        oldest_timestamp=batch.oldest_timestamp,
        newest_timestamp=batch.newest_timestamp,
    )


def _make_timestamp(timestamp: str | datetime | IntegralNanosecondsUTC) -> Timestamp:
    """Convert timestamp to columnar Timestamp format."""
    seconds_nanos = _SecondsNanos.from_flexible(timestamp)
    return Timestamp(seconds=seconds_nanos.seconds, nanos=seconds_nanos.nanos)
