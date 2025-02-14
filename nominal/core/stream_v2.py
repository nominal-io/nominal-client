from __future__ import annotations

import concurrent.futures
import enum
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import TracebackType
from typing import Callable, Sequence, Type

from typing_extensions import Self

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core.queueing import (
    BackpressureQueue,
    BlockingQueue,
    DropNewestQueue,
    DropOldestQueue,
    ReadQueue,
    iter_queue,
    spawn_batching_thread,
)
from nominal.core.stream import BatchItem

# if TYPE_CHECKING:
from nominal.core.worker_pool import ProcessPoolManager
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


class BackpressureMode(enum.Enum):
    BLOCK = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"


@dataclass()
class WriteStreamV2:
    _process_batch: Callable[[Sequence[BatchItem]], None]
    max_batch_size: int = 50_000
    max_wait: timedelta = timedelta(seconds=1)
    max_queue_size: int = 0  # Default to unlimited queue size
    _item_queue: BackpressureQueue[BatchItem] = field(init=False)
    _batch_queue: ReadQueue[Sequence[BatchItem]] | None = field(default=None)
    _batch_thread: threading.Thread | None = field(default=None)
    _process_thread: threading.Thread | None = field(default=None)
    _executor: concurrent.futures.Executor | None = field(default=None)
    backpressure_mode: BackpressureMode = field(default=BackpressureMode.BLOCK)

    @classmethod
    def create(
        cls,
        nominal_data_source_rid: str,
        process_batch: Callable[[Sequence[BatchItem]], None],
        max_batch_size: int,
        max_wait: timedelta,
        max_queue_size: int,
        backpressure_mode: BackpressureMode,
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
        executor = ProcessPoolManager(
            max_workers=max_workers,
            client_factory=client_factory,
            nominal_data_source_rid=nominal_data_source_rid,
            auth_header=auth_header,
        )

        instance = cls(
            _process_batch=process_batch,
            max_batch_size=max_batch_size,
            max_wait=max_wait,
            max_queue_size=max_queue_size,
            _executor=executor,
            backpressure_mode=backpressure_mode,
        )

        # Initialize queues
        item_maxsize = max_queue_size if max_queue_size > 0 else 0
        batch_maxsize = (max_queue_size // max_batch_size) if max_queue_size > 0 else 0

        # Choose the correct queue according to backpressure_mode.
        if backpressure_mode == BackpressureMode.DROP_NEWEST:
            instance._item_queue = DropNewestQueue(maxsize=item_maxsize)
        elif backpressure_mode == BackpressureMode.DROP_OLDEST:
            instance._item_queue = DropOldestQueue(maxsize=item_maxsize)
        else:  # BLOCK mode
            instance._item_queue = BlockingQueue(maxsize=item_maxsize)

        # Start the streaming threads for batching and processing.
        instance._batch_thread, instance._batch_queue = spawn_batching_thread(
            instance._item_queue,
            instance.max_batch_size,
            instance.max_wait,
            max_queue_size=batch_maxsize,
        )
        instance._process_thread = threading.Thread(target=instance._process_worker, daemon=True)
        instance._process_thread.start()

        return instance

    def close(self, wait: bool = True) -> None:
        """Stop the streaming threads."""
        if self._item_queue:
            self._item_queue.shutdown()

        if wait and self._batch_thread and self._process_thread:
            self._batch_thread.join()
            self._process_thread.join()

            if isinstance(self._executor, ProcessPoolManager):
                self._executor.shutdown()
            elif self._executor is not None:
                self._executor.shutdown(wait=True)

            self._batch_thread = None
            self._process_thread = None
            self._batch_queue = None
            self._item_queue = BlockingQueue()
            self._executor = None

    def _process_worker(self) -> None:
        """Worker that processes batches."""
        if not self._batch_queue:
            return

        futures = []
        for batch in iter_queue(self._batch_queue):
            if self._executor is not None:
                future = self._executor.submit(self._process_batch, batch)
                futures.append(future)
            else:
                try:
                    self._process_batch(batch)
                except Exception as e:
                    logger.error(f"Batch processing failed: {e}")
        # Wait for any remaining futures to complete
        if futures:
            concurrent.futures.wait(futures)
            for future in futures:
                try:
                    future.result()  # Raise any exceptions that occurred
                except Exception as e:
                    logger.error(f"Batch processing failed: {e}")

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        item = BatchItem(channel_name, timestamp, value, tags)
        # Simply use the queue's built-in backpressure logic.
        self._item_queue.put_with_backpressure(item)

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
