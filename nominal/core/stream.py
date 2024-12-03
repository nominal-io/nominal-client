from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import TracebackType
from typing import Callable, Sequence, Type

from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchItem:
    channel_name: str
    timestamp: str | datetime | IntegralNanosecondsUTC
    value: float
    tags: dict[str, str] | None = None


class WriteStream:
    def __init__(
        self,
        process_batch: Callable[[Sequence[BatchItem]], None],
        batch_size: int = 10,
        max_wait: timedelta = timedelta(seconds=5),
        max_workers: int | None = None,
    ):
        """Create the stream."""
        self._process_batch = process_batch
        self.batch_size = batch_size
        self.max_wait = max_wait
        self.max_workers = max_workers
        self._batch: list[BatchItem] = []
        self._batch_lock = threading.RLock()
        self._last_batch_time = time.time()
        self._running = True
        self._max_wait_event = threading.Event()
        self._pending_jobs = threading.BoundedSemaphore(3)

    def start(self) -> None:
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self._timeout_thread = threading.Thread(target=self._process_timeout_batches, daemon=True)
        self._timeout_thread.start()

    def __enter__(self) -> WriteStream:
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
        tags: dict[str, str] | None = None,
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
        tags: dict[str, str] | None = None,
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
        with self._batch_lock:
            if not self._batch:
                logger.debug("Not flushing... no enqueued batch")
                self._last_batch_time = time.time()
                return

        self._pending_jobs.acquire()

        def process_future(fut: concurrent.futures.Future) -> None:  # type: ignore[type-arg]
            """Callback to print errors to the console if a batch upload fails."""
            self._pending_jobs.release()
            maybe_ex = fut.exception()
            if maybe_ex is not None:
                logger.error("Batched upload task failed with exception", exc_info=maybe_ex)
            else:
                logger.debug("Batched upload task succeeded")

        with self._batch_lock:
            batch = self._batch
            # Clear metadata
            self._batch = []
            self._last_batch_time = time.time()

        logger.debug(f"Starting flush with {len(batch)} records")
        future = self._executor.submit(self._process_batch, batch)
        future.add_done_callback(process_future)

        # Synchronously wait, if requested
        if wait:
            # Warn user if timeout is too short
            _, pending = concurrent.futures.wait([future], timeout)
            if pending:
                logger.warning("Upload task still pending after flushing batch... increase timeout or setting to None")

    def _process_timeout_batches(self) -> None:
        while self._running:
            now = time.time()
            with self._batch_lock:
                last_batch_time = self._last_batch_time
            timeout = max(self.max_wait.seconds - (now - last_batch_time), 0)
            self._max_wait_event.wait(timeout=timeout)

            with self._batch_lock:
                # check if flush has been called in the mean time
                if self._last_batch_time > last_batch_time:
                    continue
            self.flush()

    def close(self, wait: bool = True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread
        Flush any remaining batches
        """
        self._running = False

        self._max_wait_event.set()
        self._timeout_thread.join()

        self.flush()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)


NominalWriteStream = WriteStream
