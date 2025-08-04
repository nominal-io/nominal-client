from __future__ import annotations

import itertools
from typing import Sequence, cast

from nominal_api import storage_writer_api

from nominal.core._stream.write_stream import BatchItem, LogItem
from nominal.ts import _SecondsNanos


def make_points(api_batch: Sequence[BatchItem[str | float | int]]) -> storage_writer_api.PointsExternal:
    if isinstance(api_batch[0].value, str):
        return storage_writer_api.PointsExternal(
            string=[
                storage_writer_api.StringPoint(
                    timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                    value=cast(str, item.value),
                )
                for item in api_batch
            ]
        )
    elif isinstance(api_batch[0].value, float):
        return storage_writer_api.PointsExternal(
            double=[
                storage_writer_api.DoublePoint(
                    timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                    value=cast(float, item.value),
                )
                for item in api_batch
            ]
        )
    elif isinstance(api_batch[0].value, int):
        return storage_writer_api.PointsExternal(
            int_=[
                storage_writer_api.IntPoint(
                    timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(), value=cast(int, item.value)
                )
                for item in api_batch
            ]
        )
    else:
        raise ValueError("only float and string are supported types for value")


def process_batch_legacy(
    batch: Sequence[BatchItem[str | float | int]],
    nominal_data_source_rid: str,
    auth_header: str,
    storage_writer: storage_writer_api.NominalChannelWriterService,
) -> None:
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
