from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Queue
from types import TracebackType
from typing import Callable, Self, Sequence, Type

from nominal.core.queueing import ReadQueue, iter_queue, spawn_batching_thread
from nominal.core.stream import BatchItem
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass()
class WriteStreamV2:
    _process_batch: Callable[[Sequence[BatchItem]], None]
    batch_size: int = 50_000
    max_wait: timedelta = timedelta(seconds=1)
    _item_queue: Queue[BatchItem] = field(default_factory=lambda: Queue[BatchItem]())
    _batch_queue: ReadQueue[Sequence[BatchItem]] | None = None
    _batch_thread: threading.Thread | None = None
    _process_thread: threading.Thread | None = None

    @classmethod
    def create(
        cls,
        process_batch: Callable[[Sequence[BatchItem]], None],
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
    ) -> Self:
        """Create a new WriteStreamV2 instance.

        Args:
            process_batch: Function to process batches of items
            batch_size: How many items to accumulate before processing
            max_wait: Maximum time to wait before processing a partial batch
        """
        instance = cls(
            _process_batch=process_batch,
            batch_size=batch_size,
            max_wait=max_wait,
        )

        # Start the streaming threads
        instance._batch_thread, instance._batch_queue = spawn_batching_thread(
            instance._item_queue, instance.batch_size, instance.max_wait
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
            self._item_queue = Queue[BatchItem]()  # Reset to clean state

    def _process_worker(self) -> None:
        """Worker that processes batches."""
        if not self._batch_queue:
            return

        for batch in iter_queue(self._batch_queue):
            try:
                self._process_batch(batch)
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                continue

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
