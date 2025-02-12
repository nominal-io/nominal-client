from __future__ import annotations

import concurrent.futures
import enum
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import TracebackType
from typing import Callable, Self, Sequence, Type

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

    @classmethod
    def create(
        cls,
        process_batch: Callable[[Sequence[BatchItem]], None],
        executor: concurrent.futures.Executor | None = None,
        max_batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        max_queue_size: int = 0,
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
    ) -> Self:
        """Create a new WriteStreamV2 instance.

        Args:
            process_batch: Function to process batches of items
            batch_size: How many items to accumulate before processing
            max_wait: Maximum time to wait before processing a partial batch
            maxsize: Maximum number of items that can be queued (0 for unlimited)
            backpressure_mode: How to handle queue overflow:
                - BLOCK: Block until space is available (default)
                - DROP_NEWEST: Drop new items when queue is full
                - DROP_OLDEST: Drop oldest items when queue is full (ring buffer)
            executor: executor for parallel batch processing.
        """
        instance = cls(
            _process_batch=process_batch,
            max_batch_size=max_batch_size,
            max_wait=max_wait,
            max_queue_size=max_queue_size,
            _executor=executor,
        )

        # Initialize queues - if maxsize=0, both queues will be unlimited
        item_maxsize = max_queue_size if max_queue_size > 0 else 0
        batch_maxsize = (max_queue_size // max_batch_size) if max_queue_size > 0 else 0

        # Create the appropriate queue type based on backpressure mode
        queue_classes: Mapping[BackpressureMode, Type[BackpressureQueue[BatchItem]]] = {
            BackpressureMode.BLOCK: BlockingQueue[BatchItem],
            BackpressureMode.DROP_NEWEST: DropNewestQueue[BatchItem],
            BackpressureMode.DROP_OLDEST: DropOldestQueue[BatchItem],
        }
        queue_class: Type[BackpressureQueue[BatchItem]] = queue_classes[backpressure_mode]

        instance._item_queue = queue_class(maxsize=item_maxsize)

        # Start the streaming threads
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
            self._item_queue.shutdown()  # Graceful shutdown

        if wait and self._batch_thread and self._process_thread:
            self._batch_thread.join()
            self._process_thread.join()
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
