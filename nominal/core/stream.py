from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Callable, Dict, Sequence, Type

from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchItem:
    channel_name: str
    timestamp: str | datetime | IntegralNanosecondsUTC
    value: float
    tags: Dict[str, str] | None = None


class NominalWriteStream:
    def __init__(
        self,
        process_batch: Callable[[Sequence[BatchItem]], None],
        batch_size: int = 10,
        max_wait_sec: int = 5,
        max_workers: int | None = None,
    ):
        """Create the stream."""
        self._process_batch = process_batch
        self.batch_size = batch_size
        self.max_wait_sec = max_wait_sec
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
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
        self.enqueue_batch(channel_name, [timestamp], [value], tags)

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[float],
        tags: Dict[str, str] | None = None,
    ) -> None:
        """Add a sequence of messages to the queue.

        The messages will not be immediately sent to Nominal. Only after the batch size is full or the timeout occurs.
        """
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! Received: {len(timestamps)} vs. {len(values)}"
            )

        with self._batch_lock:
            for timestamp, value in zip(timestamps, values):
                self._batch.append(BatchItem(channel_name, timestamp, value, tags))

            if len(self._batch) >= self.batch_size:
                self.flush()

    def flush(self, wait: bool = False, timeout: float | None = None) -> None:
        """Flush current batch of records to nominal in a background thread.

        Args:
        ----
            wait: If true, wait for the batch to complete uploading before returning
            timeout: If wait is true, the time to wait for flush completion.
                     NOTE: If none, waits indefinitely.

        """
        if not self._batch:
            logger.debug("Not flushing... no enqueued batch")
            return

        def process_future(fut: concurrent.futures.Future) -> None:  # type: ignore[type-arg]
            """Callback to print errors to the console if a batch upload fails."""
            maybe_ex = fut.exception()
            if maybe_ex is not None:
                logger.error("Batched upload task failed with exception", exc_info=maybe_ex)
            else:
                logger.debug("Batched upload task succeeded")

        logger.debug(f"Starting flush with {len(self._batch)} records")
        future = self._executor.submit(self._process_batch, self._batch)
        future.add_done_callback(process_future)

        # Clear metadata
        self._batch = []
        self._last_batch_time = time.time()

        # Synchronously wait, if requested
        if wait:
            # Warn user if timeout is too short
            _, pending = concurrent.futures.wait([future], timeout)
            if pending:
                logger.warning("Upload task still pending after flushing batch... increase timeout or setting to None")

    def _process_timeout_batches(self) -> None:
        while self._running:
            time.sleep(self.max_wait_sec / 10)
            with self._batch_lock:
                if self._batch and (time.time() - self._last_batch_time) >= self.max_wait_sec:
                    self.flush()

    def close(self, wait: bool = True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread
        Flush any remaining batches
        """
        self._running = False
        self._timeout_thread.join()

        with self._batch_lock:
            self.flush()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)
