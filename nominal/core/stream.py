from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Callable, Dict, Sequence, Type

from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class BatchItem:
    channel_name: str
    timestamp: str | datetime | IntegralNanosecondsUTC
    value: float
    tags: Dict[str, str] | None = None


class NominalWriteStream:
    def __init__(
        self, process_batch: Callable[[Sequence[BatchItem]], None], batch_size: int = 10, max_wait_sec: int = 5
    ):
        """Create the stream."""
        self._process_batch = process_batch
        self.batch_size = batch_size
        self.max_wait_sec = max_wait_sec
        self._executor = ThreadPoolExecutor()
        self._batch: list[BatchItem] = []
        self._batch_lock = threading.Lock()
        self._last_batch_time = time.time()
        self._running = True

        self._timeout_thread = threading.Thread(target=self._process_timeout_batches, daemon=True)
        self._timeout_thread.start()

    def __enter__(self) -> "NominalWriteStream":
        """Create the stream as a context manager."""
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Leave the context manager. Close all running threads."""
        self.close()

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float,
        tags: Dict[str, str] | None = None,
    ) -> None:
        """Add a message to the queue.

        The message will not be immediately sent to Nominal. Only after the batch size is full or the timeout occurs.
        """
        with self._batch_lock:
            self._batch.append(BatchItem(channel_name, timestamp, value, tags))

            if len(self._batch) >= self.batch_size:
                self._flush_batch()

    def _flush_batch(self) -> None:
        if self._batch:
            self._executor.submit(self._process_batch, self._batch)
            self._batch = []
            self._last_batch_time = time.time()

    def _process_timeout_batches(self) -> None:
        while self._running:
            time.sleep(self.max_wait_sec / 10)
            with self._batch_lock:
                if self._batch and (time.time() - self._last_batch_time) >= self.max_wait_sec:
                    self._flush_batch()

    def close(self, wait: bool = True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread
        Flush any remaining batches
        """
        self._running = False
        self._timeout_thread.join()

        with self._batch_lock:
            self._flush_batch()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)
