from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from typing import Sequence, cast

from nominal.core._columnar_write_pb2 import (
    DoublePoints,
    Points,
    RecordsBatch,
    StringPoints,
    Timestamp,
    WriteBatchesRequest,
)
from nominal.core._clientsbunch import ProtoWriteService
from nominal.core._queueing import Batch
from nominal.core._utils import _to_api_batch_key
from nominal.core.stream import BatchItem
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class SerializedBatch:
    """Result of batch serialization containing the protobuf data and timestamp bounds."""

    data: bytes  # Serialized protobuf data
    oldest_timestamp: IntegralNanosecondsUTC  # Oldest timestamp in the batch
    newest_timestamp: IntegralNanosecondsUTC  # Newest timestamp in the batch


def _make_columnar_points(api_batch: Sequence[BatchItem]) -> Points:
    """Create columnar Points from a batch of items with the same channel/tags/type."""
    timestamps = [_make_timestamp(item.timestamp) for item in api_batch]
    sample_value = api_batch[0].value
    if isinstance(sample_value, str):
        return Points(
            timestamps=timestamps,
            string_points=StringPoints(points=[cast(str, item.value) for item in api_batch]),
        )
    elif isinstance(sample_value, float):
        return Points(
            timestamps=timestamps,
            double_points=DoublePoints(points=[cast(float, item.value) for item in api_batch]),
        )
    else:
        raise ValueError("only float and string are supported types for value")


def create_write_request(batch: Sequence[BatchItem], nominal_data_source_rid: str = "") -> WriteBatchesRequest:
    """Create a WriteBatchesRequest in columnar format from batches of items."""
    api_batched = groupby(sorted(batch, key=_to_api_batch_key), key=_to_api_batch_key)
    api_batches = [list(api_batch) for _, api_batch in api_batched]
    return WriteBatchesRequest(
        data_source_rid=nominal_data_source_rid,
        batches=[
            RecordsBatch(
                channel=api_batch[0].channel_name,
                points=_make_columnar_points(api_batch),
                tags=api_batch[0].tags or {},
            )
            for api_batch in api_batches
        ],
    )


def process_batch(
    batch: Sequence[BatchItem],
    nominal_data_source_rid: str | None,
    auth_header: str,
    proto_write: ProtoWriteService,
) -> None:
    """Process a batch of items to write."""
    if nominal_data_source_rid is None:
        raise ValueError("Writing not implemented for this connection type")

    request = create_write_request(batch, nominal_data_source_rid)

    proto_write.write_nominal_columnar_batches(
        auth_header=auth_header,
        request=request.SerializeToString(),
    )


def serialize_batch(batch: Batch, data_source_rid: str = "") -> SerializedBatch:
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
