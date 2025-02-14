from __future__ import annotations

import concurrent.futures
import logging
import threading
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Queue
from types import TracebackType
from typing import Callable, Sequence, Type

from typing_extensions import Self

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core.queueing import (
    QueueShutdown,
    ReadQueue,
    iter_queue,
    spawn_batching_thread,
)
from nominal.core.stream import BatchItem

# if TYPE_CHECKING:
from nominal.core.worker_pool import WorkerContext, worker_init
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass()
class WriteStreamV2:
    _process_batch: Callable[[Sequence[BatchItem]], None]
    _max_batch_size: int
    _max_wait: timedelta
    _max_queue_size: int
    _item_queue: Queue[BatchItem | QueueShutdown]
    _batch_queue: ReadQueue[Sequence[BatchItem]]
    _batch_thread: threading.Thread
    _process_pool: ProcessPoolExecutor
    _io_pool: concurrent.futures.ThreadPoolExecutor
    _client_factory: Callable[[], ProtoWriteService]
    _auth_header: str
    _nominal_data_source_rid: str
    _process_thread: threading.Thread | None = field(default=None)
   

    @classmethod
    def create(
        cls,
        nominal_data_source_rid: str,
        process_batch: Callable[[Sequence[BatchItem]], None],
        max_batch_size: int,
        max_wait: timedelta,
        max_queue_size: int,
        client_factory: Callable[[], ProtoWriteService],
        auth_header: str,
        max_workers: int,
    ) -> Self:
        """Create a new WriteStreamV2 instance.

        Args:
            process_batch: Function to process batches of items
            max_batch_size: How many items to accumulate before processing
            max_wait: Maximum time to wait before processing a partial batch
            max_queue_size: Maximum number of items that can be queued (0 for unlimited)
            backpressure_mode: How to handle queue overflow:
                - BLOCK: Block until space is available (default)
                - DROP_NEWEST: Drop new items when queue is full
                - DROP_OLDEST: Drop oldest items when queue is full (ring buffer)
            executor: executor for parallel batch processing.
            client_factory: Factory function to create ProtoWriteService instances
            nominal_data_source_rid: Nominal data source rid
            auth_header: Authentication header
            max_workers: Maximum number of worker threads for parallel processing
        """
        context = WorkerContext(
            nominal_data_source_rid=nominal_data_source_rid,
            auth_header=auth_header,
            proto_write=client_factory(),
        )

        stream = ProcessPoolExecutor(max_workers=max_workers, initializer=worker_init, initargs=(context,))
        io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

        item_maxsize = max_queue_size if max_queue_size > 0 else 0
        batch_maxsize = (max_queue_size // max_batch_size) if max_queue_size > 0 else 0

        item_queue: Queue[BatchItem | QueueShutdown] = Queue(maxsize=item_maxsize)

        batch_thread, batch_queue = spawn_batching_thread(
            item_queue,
            max_batch_size,
            max_wait,
            max_queue_size=batch_maxsize,
        )

        instance = cls(
            _process_batch=process_batch,
            _max_batch_size=max_batch_size,
            _max_wait=max_wait,
            _max_queue_size=max_queue_size,
            _process_pool=stream,
            _io_pool=io_pool,
            _item_queue=item_queue,
            _batch_thread=batch_thread,
            _batch_queue=batch_queue,
            _client_factory=client_factory,
            _auth_header=auth_header,
            _nominal_data_source_rid=nominal_data_source_rid,
        )

        process_thread = threading.Thread(target=instance._process_worker, daemon=True)
        instance._process_thread = process_thread
        instance._process_thread.start()

        return instance

    def close(self, wait: bool = True) -> None:
        """Stop the streaming threads."""
        if self._item_queue:
            self._item_queue.put(QueueShutdown())

        if wait and self._batch_thread and self._process_thread:
            self._batch_thread.join()
            self._process_thread.join()

            self._process_pool.shutdown(wait=True)
            self._io_pool.shutdown(wait=True)
            self._item_queue = Queue()

    def _process_worker(self) -> None:
        """Worker that processes batches."""
        if not self._batch_queue:
            return

        proto_write = self._client_factory()
        
        def handle_serialized_data(future: concurrent.futures.Future) -> None:
            try:
                serialized_data = future.result()
                self._io_pool.submit(proto_write.write_nominal_batches, self._auth_header, self._nominal_data_source_rid, serialized_data)
            except Exception as e:
                logger.error(f"Error processing batch: {e}")

        for batch in iter_queue(self._batch_queue):
            future = self._process_pool.submit(self._process_batch, batch)
            future.add_done_callback(handle_serialized_data)

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
        flattened_dict: dict[str, float | str],
    ) -> None:
        """Write multiple channel values using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and
        the corresponding value is enqueued with the provided timestamp.

        Args:
            timestamp: The common timestamp to use for all enqueued items.
            flattened_dict: A dictionary mapping channel names to their values.
        """
        for channel, value in flattened_dict.items():
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
        """Leave the context manager."""
        self.close()
