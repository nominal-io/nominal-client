from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Queue
from types import TracebackType
from typing import Callable, Literal, Sequence, Type

from nominal.core.queueing import iter_queue, spawn_batching_thread, ReadQueue
from nominal.ts import IntegralNanosecondsUTC
from nominal.core.stream import BatchItem

logger = logging.getLogger(__name__)




@dataclass()
class StreamingManager:
    processor: Callable[[Sequence[BatchItem], Literal["json", "protobuf"]], None]
    batch_size: int = 50_000
    max_wait: timedelta = timedelta(seconds=1)
    _item_queue: Queue[BatchItem] = Queue()
    _batch_queue: ReadQueue[Sequence[BatchItem]] | None = None
    _batch_thread: threading.Thread | None = None
    _process_thread: threading.Thread | None = None

    def start(self, data_format: Literal["json", "protobuf"] = "json") -> None:
        """Start the streaming threads."""
        if self._batch_thread is not None or self._process_thread is not None:
            raise RuntimeError("Streaming already started")

        self._item_queue = Queue[BatchItem]()

        self._batch_thread, self._batch_queue = spawn_batching_thread(self._item_queue, self.batch_size, self.max_wait)

        self._process_thread = threading.Thread(target=self._process_worker, args=(data_format,), daemon=True)
        self._process_thread.start()

    def stop(self, wait: bool = True) -> None:
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

    def _process_worker(self, data_format: Literal["json", "protobuf"]) -> None:
        """Worker that processes batches."""
        if not self._batch_queue:
            return

        for batch in iter_queue(self._batch_queue):
            try:
                self.processor(batch, data_format)
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                continue

    def write(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        if self._batch_thread is None:
            raise RuntimeError("Streaming not started. Call start() first")
        item = BatchItem(channel_name, timestamp, value, tags)
        self._item_queue.put(item)

    def write_batch(
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
            self.write(channel_name, timestamp, value, tags)

    def __enter__(self) -> StreamingManager:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()
