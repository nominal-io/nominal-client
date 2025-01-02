from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any, Callable, Sequence, Type

from typing_extensions import Self

from nominal.ts import IntegralNanosecondsUTC


def __getattr__(name: str) -> Any:
    if name == "NominalWriteStream":
        warnings.warn(
            "NominalWriteStream is deprecated, use WriteStream instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return WriteStream
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchItem:
    channel_name: str
    timestamp: str | datetime | IntegralNanosecondsUTC
    value: float | str
    tags: dict[str, str] | None = None


@dataclass(frozen=True)
class WriteStream:
    batch_size: int
    max_wait: timedelta
    _process_batch: Callable[[Sequence[BatchItem]], None]
    _executor: concurrent.futures.ThreadPoolExecutor
    _thread_safe_batch: ThreadSafeBatch
    _stop: threading.Event
    _pending_jobs: threading.BoundedSemaphore

    @classmethod
    def create(
        cls,
        batch_size: int,
        max_wait: timedelta,
        process_batch: Callable[[Sequence[BatchItem]], None],
    ) -> Self:
        """Create the stream."""
        executor = concurrent.futures.ThreadPoolExecutor()

        instance = cls(
            batch_size,
            max_wait,
            process_batch,
            executor,
            ThreadSafeBatch(),
            threading.Event(),
            threading.BoundedSemaphore(3),
        )

        executor.submit(instance._process_timeout_batches)

        return instance

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
        value: float | str,
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
        values: Sequence[float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Add a sequence of messages to the queue.

        The messages will not be immediately sent to Nominal. Only after the batch size is full or the timeout occurs.
        """
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! Received: {len(timestamps)} vs. {len(values)}"
            )

        self._thread_safe_batch.add(
            [BatchItem(channel_name, timestamp, value, tags) for timestamp, value in zip(timestamps, values)]
        )
        self._flush(condition=lambda size: size >= self.batch_size)

    def _flush(self, condition: Callable[[int], bool] | None = None) -> concurrent.futures.Future[None] | None:
        batch = self._thread_safe_batch.swap(condition)

        if batch is None:
            return None
        if not batch:
            logger.debug("Not flushing... no enqueued batch")
            return None

        self._pending_jobs.acquire()

        def process_future(fut: concurrent.futures.Future) -> None:  # type: ignore[type-arg]
            """Callback to print errors to the console if a batch upload fails."""
            self._pending_jobs.release()
            maybe_ex = fut.exception()
            if maybe_ex is not None:
                logger.error("Batched upload task failed with exception", exc_info=maybe_ex)
            else:
                logger.debug("Batched upload task succeeded")

        logger.debug(f"Starting flush with {len(batch)} records")
        future = self._executor.submit(self._process_batch, batch)
        future.add_done_callback(process_future)
        return future

    def flush(self, wait: bool = False, timeout: float | None = None) -> None:
        """Flush current batch of records to nominal in a background thread.

        Args:
        ----
            wait: If true, wait for the batch to complete uploading before returning
            timeout: If wait is true, the time to wait for flush completion.
                     NOTE: If none, waits indefinitely.

        """
        future = self._flush()

        # Synchronously wait, if requested
        if wait and future is not None:
            # Warn user if timeout is too short
            _, pending = concurrent.futures.wait([future], timeout)
            if pending:
                logger.warning("Upload task still pending after flushing batch... increase timeout or setting to None")

    def _process_timeout_batches(self) -> None:
        while not self._stop.is_set():
            now = time.time()

            last_batch_time = self._thread_safe_batch.last_time
            timeout = max(self.max_wait.seconds - (now - last_batch_time), 0)
            self._stop.wait(timeout=timeout)

            # check if flush has been called in the mean time
            if self._thread_safe_batch.last_time > last_batch_time:
                continue

            self._flush()

    def close(self, wait: bool = True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread
        Flush any remaining batches
        """
        self._stop.set()

        self._flush()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)


class ThreadSafeBatch:
    def __init__(self) -> None:
        """Thread-safe access to batch and last swap time."""
        self._batch: list[BatchItem] = []
        self._last_time = time.time()
        self._lock = threading.Lock()

    def swap(self, condition: Callable[[int], bool] | None = None) -> list[BatchItem] | None:
        """Swap the current batch with an empty one and return the old batch.

        If condition is provided, the swap will only occur if the condition is met, otherwise None is returned.
        """
        with self._lock:
            if condition and not condition(len(self._batch)):
                return None
            batch = self._batch
            self._batch = []
            self._last_time = time.time()
        return batch

    def add(self, items: Sequence[BatchItem]) -> None:
        with self._lock:
            self._batch.extend(items)

    @property
    def last_time(self) -> float:
        with self._lock:
            return self._last_time
