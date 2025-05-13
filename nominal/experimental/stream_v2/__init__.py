from __future__ import annotations

import contextlib
import os  # Added OS import for cpu_count
from datetime import timedelta
from typing import Generator

from nominal.core.connection import StreamingConnection
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.experimental.stream_v2._write_stream import WriteStreamV2
from nominal.experimental.stream_v3._write_stream import WriteStreamV3


@contextlib.contextmanager
def create_write_stream_v3(
    streaming_connection: StreamingConnection,
    max_batch_size: int = 50000,
    write_thread_workers: int | None = 10,
    serialize_process_workers: int | None = None,
) -> Generator[WriteStreamV3, None, None]:
    """Writer for a streaming data source in Nominal.

    Utilizes multiple processes to serialize batches of protobufs, and a thread pool to write to Nominal.

    Use as a context manager to ensure resources are cleaned up.

    Args:
        streaming_connection: The nominal streaming connection to write to.
        max_batch_size: How big the batch can get before writing to Nominal.
        max_wait: How long a batch can exist before being flushed to Nominal.
        max_queue_size: Maximum number of items that can be queued (0 for unlimited).
        write_thread_workers: Number of threads to use for writing to Nominal.
        serialize_process_workers: Number of processes to use for serializing batches of protobufs.
            If None, will use (CPU count - 1) or at least 2 processes.
        track_metrics: Whether to publish metrics on latency to nominal channels on the connection
    Example:
        ```python
        connection = client.get_connection(connection_rid)
        with nominal.experimental.stream_v2.create_write_stream(connection) as stream:
            stream.enqueue("temperature", 42.0, timestamp="2021-01-01T00:00:00Z", tags={"thermocouple": "A"})
            stream.enqueue("temperature", 43.0, timestamp="2021-01-01T00:00:00Z", tags={"thermocouple": "B"})
        ```
    """
    serializer = BatchSerializer.create(
        max_workers=serialize_process_workers,
    )
    
    with WriteStreamV3.create(
        streaming_connection._clients,
        serializer,
        streaming_connection.nominal_data_source_rid,
        write_thread_workers,
    ) as stream:
        yield stream


@contextlib.contextmanager
def create_write_stream(
    streaming_connection: StreamingConnection,
    max_batch_size: int = 50000,
    max_wait: timedelta = timedelta(seconds=1),
    max_queue_size: int = 0,
    write_thread_workers: int | None = 10,
    serialize_process_workers: int | None = None,
    track_metrics: bool = True,
) -> Generator[WriteStreamV2, None, None]:
    """Writer for a streaming data source in Nominal.

    Utilizes multiple processes to serialize batches of protobufs, and a thread pool to write to Nominal.

    Use as a context manager to ensure resources are cleaned up.

    Args:
        streaming_connection: The nominal streaming connection to write to.
        max_batch_size: How big the batch can get before writing to Nominal.
        max_wait: How long a batch can exist before being flushed to Nominal.
        max_queue_size: Maximum number of items that can be queued (0 for unlimited).
        write_thread_workers: Number of threads to use for writing to Nominal.
        serialize_process_workers: Number of processes to use for serializing batches of protobufs.
            If None, will use (CPU count - 1) or at least 2 processes.
        track_metrics: Whether to publish metrics on latency to nominal channels on the connection
    Example:
        ```python
        connection = client.get_connection(connection_rid)
        with nominal.experimental.stream_v2.create_write_stream(connection) as stream:
            stream.enqueue("temperature", 42.0, timestamp="2021-01-01T00:00:00Z", tags={"thermocouple": "A"})
            stream.enqueue("temperature", 43.0, timestamp="2021-01-01T00:00:00Z", tags={"thermocouple": "B"})
        ```
    """
    serializer = BatchSerializer.create(
        max_workers=serialize_process_workers,
    )
    
    with WriteStreamV2.create(
        streaming_connection._clients,
        serializer,
        streaming_connection.nominal_data_source_rid,
        max_batch_size,
        max_wait,
        max_queue_size,
        track_metrics,
        write_thread_workers,
    ) as stream:
        yield stream
