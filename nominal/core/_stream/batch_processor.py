from __future__ import annotations

import itertools
from typing import Sequence

from nominal.core._stream.write_stream import LogItem
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.log import LogPoint
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2 as writer_pb
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2_grpc


def process_log_batch(
    batch: Sequence[LogItem],
    nominal_data_source_rid: str,
    storage_writer: nominal_channel_writer_pb2_grpc.NominalChannelWriterServiceStub,
) -> None:
    def _get_channel_name(batch_item: LogItem) -> str:
        return batch_item.channel_name

    # Not using BatchItem.sort_key, as we don't need to group by tags-- each log
    # has its own set of args when streamed.
    batches_by_channel = itertools.groupby(sorted(batch, key=_get_channel_name), key=_get_channel_name)
    requests = [
        writer_pb.WriteLogsRequest(
            data_source_rid=nominal_data_source_rid,
            logs=[
                LogPoint(batch_item.timestamp, batch_item.value, batch_item.tags or {})._to_proto()
                for batch_item in batch_by_channel
            ],
            channel=channel,
        )
        for channel, batch_by_channel in batches_by_channel
    ]
    for request in requests:
        with translate_grpc_errors():
            storage_writer.WriteLogs(request)
