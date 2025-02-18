from __future__ import annotations

import concurrent.futures
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from queue import Queue
from types import TracebackType
from typing import Protocol, Sequence, Type

from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._queueing import QueueShutdown, ReadQueue, iter_queue, spawn_batching_thread
from nominal.core.stream import BatchItem
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriteStreamV2:
    _item_queue: Queue[BatchItem | QueueShutdown]
    _batch_thread: threading.Thread
    _write_pool: ThreadPoolExecutor
    _batch_serialize_thread: threading.Thread
    _serializer: BatchSerializer
    _clients: _Clients

    class _Clients(HasAuthHeader, Protocol):
        @property
        def proto_write(self) -> ProtoWriteService: ...

    @classmethod
    def create(
        cls,
        clients: _Clients,
        serializer: BatchSerializer,
        nominal_data_source_rid: str,
        max_batch_size: int,
        max_wait: timedelta,
        max_queue_size: int,
        max_workers: int | None,
    ) -> Self:
        write_pool = ThreadPoolExecutor(max_workers=max_workers)
        item_maxsize = max_queue_size if max_queue_size > 0 else 0
        batch_queue_maxsize = (max_queue_size // max_batch_size) if max_queue_size > 0 else 0

        item_queue: Queue[BatchItem | QueueShutdown] = Queue(maxsize=item_maxsize)
        batch_thread, batch_queue = spawn_batching_thread(
            item_queue,
            max_batch_size,
            max_wait,
            max_queue_size=batch_queue_maxsize,
        )
        batch_serialize_thread = spawn_batch_serialize_thread(
            write_pool, clients, serializer, nominal_data_source_rid, batch_queue
        )
        return cls(
            _write_pool=write_pool,
            _item_queue=item_queue,
            _batch_thread=batch_thread,
            _batch_serialize_thread=batch_serialize_thread,
            _serializer=serializer,
            _clients=clients,
        )

    def close(self) -> None:
        self._item_queue.put(QueueShutdown())
        self._batch_thread.join()
        self._batch_serialize_thread.join()
        self._serializer.close()
        self._write_pool.shutdown(cancel_futures=True)

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        item = BatchItem(channel_name, timestamp, value, tags)
        self._item_queue.put(item)

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write multiple values."""
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! Received: {len(timestamps)} vs. {len(values)}"
            )
        for timestamp, value in zip(timestamps, values):
            self.enqueue(channel_name, timestamp, value, tags)

    def enqueue_from_dict(
        self,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        channel_values: dict[str, float | str],
    ) -> None:
        """Write multiple channel values at a single timestamp using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and
        the corresponding value is enqueued with the provided timestamp.

        Args:
            timestamp: The common timestamp to use for all enqueued items.
            channel_values: A dictionary mapping channel names to their values.
        """
        for channel, value in channel_values.items():
            self.enqueue(channel, timestamp, value)

    def __enter__(self) -> WriteStreamV2:
        """Create the stream as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def _write_serialized_batch(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    nominal_data_source_rid: str,
    future: concurrent.futures.Future[bytes],
) -> None:
    try:
        serialized_data = future.result()
        pool.submit(
            clients.proto_write.write_nominal_batches,
            clients.auth_header,
            nominal_data_source_rid,
            serialized_data,
        )
    except Exception as e:
        logger.error(f"Error processing batch: {e}", exc_info=True)
        raise e


def serialize_and_write_batches(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    batch_queue: ReadQueue[Sequence[BatchItem]],
) -> None:
    """Worker that processes batches."""
    callback = partial(_write_serialized_batch, pool, clients, nominal_data_source_rid)
    for batch in iter_queue(batch_queue):
        future = serializer.serialize(batch)
        future.add_done_callback(callback)


def spawn_batch_serialize_thread(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    batch_queue: ReadQueue[Sequence[BatchItem]],
) -> threading.Thread:
    thread = threading.Thread(
        target=serialize_and_write_batches,
        args=(pool, clients, serializer, nominal_data_source_rid, batch_queue),
    )
    thread.start()
    return thread
