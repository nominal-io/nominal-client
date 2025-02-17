from datetime import timedelta
from nominal.core.connection import StreamingConnection

from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.experimental.stream_v2._write_stream import WriteStreamV2


def create_write_stream(
    streaming_connection: StreamingConnection,
    max_batch_size: int = 30_000,
    max_wait: timedelta = timedelta(seconds=1),
    max_queue_size: int = 0,
    write_thread_workers: int | None = 10,
    serialize_process_workers: int = 2,
) -> WriteStreamV2:
    """Writer for a streaming data source in Nominal.

    Utilizes multiple processes to serialize batches of protobufs, and a thread pool to write to Nominal.

    Args:
        streaming_connection: The nominal streaming connection to write to.
        max_batch_size: How big the batch can get before writing to Nominal.
        max_wait: How long a batch can exist before being flushed to Nominal.
        max_queue_size: Maximum number of items that can be queued (0 for unlimited).
    """
    serializer = BatchSerializer.create(max_workers=serialize_process_workers)
    return WriteStreamV2.create(
        streaming_connection._clients,
        serializer,
        streaming_connection.nominal_data_source_rid,
        max_batch_size,
        max_wait,
        max_queue_size,
        write_thread_workers,
    )
