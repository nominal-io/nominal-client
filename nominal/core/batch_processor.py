from __future__ import annotations

from datetime import datetime
from itertools import groupby
from typing import Sequence

from nominal_api_protos.nominal_write_pb2 import (
    Channel as NominalChannel,
    DoublePoint,
    DoublePoints,
    Points,
    Series,
    StringPoint,
    StringPoints,
    WriteRequestNominal,
)

from nominal.core.stream import BatchItem
from nominal.ts import _SecondsNanos, IntegralNanosecondsUTC


def _to_api_batch_key(item: BatchItem) -> tuple[str, Sequence[tuple[str, str]], str]:
    return item.channel_name, sorted(item.tags.items()) if item.tags is not None else [], type(item.value).__name__


def process_batch(batch: Sequence[BatchItem], nominal_data_source_rid: str | None, auth_header: str, proto_write_service) -> None:
    """Process a batch of items to write."""
    api_batched = groupby(sorted(batch, key=_to_api_batch_key), key=_to_api_batch_key)

    if nominal_data_source_rid is None:
        raise ValueError("Writing not implemented for this connection type")
    api_batches = [list(api_batch) for _, api_batch in api_batched]

    def make_points_proto(api_batch: Sequence[BatchItem]) -> Points:
        # Check first value to determine type
        sample_value = api_batch[0].value
        if isinstance(sample_value, str):
            return Points(
                string_points=StringPoints(
                    points=[
                        StringPoint(timestamp=_make_timestamp(item.timestamp), value=item.value)
                        for item in api_batch
                    ]
                )
            )
        elif isinstance(sample_value, float):
            return Points(
                double_points=DoublePoints(
                    points=[
                        DoublePoint(timestamp=_make_timestamp(item.timestamp), value=item.value)
                        for item in api_batch
                    ]
                )
            )
        else:
            raise ValueError("only float and string are supported types for value")

    request = WriteRequestNominal(
        series=[
            Series(
                channel=NominalChannel(name=api_batch[0].channel_name),
                points=make_points_proto(api_batch),
                tags=api_batch[0].tags or {},
            )
            for api_batch in api_batches
        ]
    )

    proto_write_service.write_nominal_batches(
        auth_header=auth_header,
        data_source_rid=nominal_data_source_rid,
        request=request,
    ) 

def _make_timestamp(timestamp: str | datetime | IntegralNanosecondsUTC) -> dict[str, int]:
    """Convert timestamp to protobuf Timestamp format manually."""
    seconds_nanos = _SecondsNanos.from_flexible(timestamp)
    return {"seconds": seconds_nanos.seconds, "nanos": seconds_nanos.nanos}
