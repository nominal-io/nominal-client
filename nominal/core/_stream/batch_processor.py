from __future__ import annotations

import itertools
from typing import Sequence, cast

from nominal_api import storage_writer_api

from nominal.core._stream.write_stream import BatchItem, DataItem, LogItem, PointType
from nominal.ts import _SecondsNanos


def make_points(api_batch: Sequence[DataItem]) -> storage_writer_api.PointsExternal:
    """Create PointsExternal for a batch of items with the same value type.

    Uses the centralized PointType inference from BatchItem.get_point_type().
    All items in the batch are assumed to have the same type (enforced by grouping).
    """
    # Get point type from the first item (all items in batch have same type due to grouping)
    point_type = api_batch[0].get_point_type()

    match point_type:
        case PointType.STRING_ARRAY:
            return storage_writer_api.PointsExternal(
                array=storage_writer_api.ArrayPoints(
                    string=[
                        storage_writer_api.StringArrayPoint(
                            timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                            value=cast(list[str], item.value),
                        )
                        for item in api_batch
                    ]
                )
            )
        case PointType.DOUBLE_ARRAY:
            return storage_writer_api.PointsExternal(
                array=storage_writer_api.ArrayPoints(
                    double=[
                        storage_writer_api.DoubleArrayPoint(
                            timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                            value=cast(list[float], item.value),
                        )
                        for item in api_batch
                    ]
                )
            )
        case PointType.STRING:
            return storage_writer_api.PointsExternal(
                string=[
                    storage_writer_api.StringPoint(
                        timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                        value=cast(str, item.value),
                    )
                    for item in api_batch
                ]
            )
        case PointType.DOUBLE:
            return storage_writer_api.PointsExternal(
                double=[
                    storage_writer_api.DoublePoint(
                        timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                        value=cast(float, item.value),
                    )
                    for item in api_batch
                ]
            )
        case PointType.INT:
            return storage_writer_api.PointsExternal(
                int_=[
                    storage_writer_api.IntPoint(
                        timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                        value=cast(int, item.value),
                    )
                    for item in api_batch
                ]
            )
        case _:
            raise ValueError(f"Unsupported point type: {point_type}")


def process_batch_legacy(
    batch: Sequence[DataItem],
    nominal_data_source_rid: str,
    auth_header: str,
    storage_writer: storage_writer_api.NominalChannelWriterService,
) -> None:
    """Process a batch of data items (scalars or arrays) using the legacy JSON API."""
    api_batched = itertools.groupby(sorted(batch, key=BatchItem.sort_key), key=BatchItem.sort_key)

    api_batches = [list(api_batch) for _, api_batch in api_batched]
    request = storage_writer_api.WriteBatchesRequestExternal(
        batches=[
            storage_writer_api.RecordsBatchExternal(
                channel=api_batch[0].channel_name,
                points=make_points(api_batch),
                tags=dict(api_batch[0].tags) if api_batch[0].tags is not None else {},
            )
            for api_batch in api_batches
        ],
        data_source_rid=nominal_data_source_rid,
    )
    storage_writer.write_batches(
        auth_header,
        request,
    )


def process_log_batch(
    batch: Sequence[LogItem],
    nominal_data_source_rid: str,
    auth_header: str,
    storage_writer: storage_writer_api.NominalChannelWriterService,
) -> None:
    def _get_channel_name(batch_item: LogItem) -> str:
        return batch_item.channel_name

    # Not using BatchItem.sort_key, as we don't need to group by tags-- each log
    # has its own set of args when streamed.
    batches_by_channel = itertools.groupby(sorted(batch, key=_get_channel_name), key=_get_channel_name)
    requests = [
        storage_writer_api.WriteLogsRequest(
            logs=[
                storage_writer_api.LogPoint(
                    timestamp=_SecondsNanos.from_nanoseconds(batch_item.timestamp).to_api(),
                    value=storage_writer_api.LogValue(
                        message=batch_item.value,
                        args={k: v for k, v in (batch_item.tags or {}).items()},
                    ),
                )
                for batch_item in batch_by_channel
            ],
            channel=channel,
        )
        for channel, batch_by_channel in batches_by_channel
    ]
    for request in requests:
        storage_writer.write_logs(auth_header, nominal_data_source_rid, request)
