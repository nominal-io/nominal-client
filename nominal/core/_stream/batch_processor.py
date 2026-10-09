from __future__ import annotations

import itertools
from typing import Sequence

from nominal_api import storage_writer_api

from nominal.core._stream.write_stream import LogItem
from nominal.ts import _SecondsNanos


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
