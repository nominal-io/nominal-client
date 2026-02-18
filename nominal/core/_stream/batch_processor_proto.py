from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from typing import Any, Sequence, cast

from google.protobuf.timestamp_pb2 import Timestamp

try:
    from nominal_api_protos.nominal_write_pb2 import (
        ArrayPoints,
        DoubleArrayPoint,
        DoubleArrayPoints,
        DoublePoint,
        DoublePoints,
        IntegerPoint,
        IntegerPoints,
        Points,
        Series,
        StringArrayPoint,
        StringArrayPoints,
        StringPoint,
        StringPoints,
        StructPoint,
        StructPoints,
        WriteRequestNominal,
    )
    from nominal_api_protos.nominal_write_pb2 import (
        Channel as NominalChannel,
    )
except ModuleNotFoundError:
    raise ImportError("nominal[protos] is required to use the protobuf-based streaming API")

from nominal.core._clientsbunch import ProtoWriteService
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
    """Create Points protobuf for a batch of items with the same value type.

    Uses the centralized PointType inference from BatchItem.get_point_type().
    All items in the batch are assumed to have the same type (enforced by grouping).
    """
    # Get point type from the first item (all items in batch have same type due to grouping)
    point_type = api_batch[0].get_point_type()

    match point_type:
        case PointType.STRING_ARRAY:
            return Points(
                array_points=ArrayPoints(
                    string_array_points=StringArrayPoints(
                        points=[
                            StringArrayPoint(
                                timestamp=_make_timestamp(item.timestamp),
                                value=cast(list[str], item.value),
                            )
                            for item in api_batch
                        ]
                    )
                )
            )
        case PointType.DOUBLE_ARRAY:
            return Points(
                array_points=ArrayPoints(
                    double_array_points=DoubleArrayPoints(
                        points=[
                            DoubleArrayPoint(
                                timestamp=_make_timestamp(item.timestamp),
                                value=cast(list[float], item.value),
                            )
                            for item in api_batch
                        ]
                    )
                )
            )
        case PointType.STRING:
            return Points(
                string_points=StringPoints(
                    points=[
                        StringPoint(
                            timestamp=_make_timestamp(item.timestamp),
                            value=cast(str, item.value),
                        )
                        for item in api_batch
                    ]
                )
            )
        case PointType.DOUBLE:
            return Points(
                double_points=DoublePoints(
                    points=[
                        DoublePoint(
                            timestamp=_make_timestamp(item.timestamp),
                            value=cast(float, item.value),
                        )
                        for item in api_batch
                    ]
                )
            )
        case PointType.INT:
            return Points(
                integer_points=IntegerPoints(
                    points=[
                        IntegerPoint(
                            timestamp=_make_timestamp(item.timestamp),
                            value=cast(int, item.value),
                        )
                        for item in api_batch
                    ]
                )
            )
        case PointType.STRUCT:
            return Points(
                struct_points=StructPoints(
                    points=[
                        StructPoint(
                            timestamp=_make_timestamp(item.timestamp),
                                json_string=json.dumps(cast(dict[str, Any], item.value)),
                        )
                        for item in api_batch
                    ]
                )
            )
        case _:
            raise ValueError(f"Unsupported point type: {point_type}")


def create_write_request(batch: Sequence[DataItem]) -> WriteRequestNominal:
    """Create a WriteRequestNominal from batches of items."""
    api_batched = groupby(sorted(batch, key=BatchItem.sort_key), key=BatchItem.sort_key)
    api_batches = [list(api_batch) for _, api_batch in api_batched]
    return WriteRequestNominal(
        series=[
            Series(
                channel=NominalChannel(name=api_batch[0].channel_name),
                points=make_points_proto(api_batch),
                tags=api_batch[0].tags or {},
            )
            for api_batch in api_batches
        ]
    )


def process_batch(
    batch: Sequence[DataItem],
    nominal_data_source_rid: str | None,
    auth_header: str,
    proto_write: ProtoWriteService,
) -> None:
    """Process a batch of data items (scalars or arrays) to write."""
    if nominal_data_source_rid is None:
        raise ValueError("Writing not implemented for this connection type")

    request = create_write_request(batch)

    proto_write.write_nominal_batches(
        auth_header=auth_header,
        data_source_rid=nominal_data_source_rid,
        request=request.SerializeToString(),
    )


def serialize_batch(batch: Batch[StreamValueType]) -> SerializedBatch:
    """Process a batch of items and return serialized request."""
    request = create_write_request(batch.items)
    return SerializedBatch(
        data=request.SerializeToString(),
        oldest_timestamp=batch.oldest_timestamp,
        newest_timestamp=batch.newest_timestamp,
    )


def _make_timestamp(timestamp: str | datetime | IntegralNanosecondsUTC) -> Timestamp:
    """Convert timestamp to protobuf Timestamp format."""
    seconds_nanos = _SecondsNanos.from_flexible(timestamp)
    ts = Timestamp(seconds=seconds_nanos.seconds, nanos=seconds_nanos.nanos)
    return ts
