from __future__ import annotations

from datetime import datetime
from itertools import groupby
from typing import Sequence, cast

from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api_protos.nominal_write_pb2 import (
    Channel as NominalChannel,
)
from nominal_api_protos.nominal_write_pb2 import (
    DoublePoint,
    DoublePoints,
    Points,
    Series,
    StringPoint,
    StringPoints,
    WriteRequestNominal,
)

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core._utils import _to_api_batch_key
from nominal.core.stream import BatchItem
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def make_points_proto(api_batch: Sequence[BatchItem]) -> Points:
    # Check first value to determine type
    sample_value = api_batch[0].value
    if isinstance(sample_value, str):
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
    elif isinstance(sample_value, float):
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
    else:
        raise ValueError("only float and string are supported types for value")


def create_write_request(api_batches: list[list[BatchItem]]) -> WriteRequestNominal:
    """Create a WriteRequestNominal from batches of items."""
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
    batch: Sequence[BatchItem],
    nominal_data_source_rid: str | None,
    auth_header: str,
    proto_write: ProtoWriteService,
) -> None:
    """Process a batch of items to write."""
    api_batched = groupby(sorted(batch, key=_to_api_batch_key), key=_to_api_batch_key)

    api_batches = [list(api_batch) for _, api_batch in api_batched]

    if nominal_data_source_rid is None:
        raise ValueError("Writing not implemented for this connection type")

    request = create_write_request(api_batches)

    proto_write.write_nominal_batches(
        auth_header=auth_header,
        data_source_rid=nominal_data_source_rid,
        request=request,
    )


def _make_timestamp(timestamp: str | datetime | IntegralNanosecondsUTC) -> Timestamp:
    """Convert timestamp to protobuf Timestamp format."""
    seconds_nanos = _SecondsNanos.from_flexible(timestamp)
    ts = Timestamp(seconds=seconds_nanos.seconds, nanos=seconds_nanos.nanos)
    return ts
