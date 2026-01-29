from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import TracebackType
from typing import Callable, Generic, Mapping, Sequence, Type, TypeAlias

from typing_extensions import Self

from nominal.core._stream.write_stream_base import StreamType, WriteStreamBase
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


def _get_value_type_name(value: object) -> str:
    """Get a type name for batch grouping that distinguishes list element types."""
    if isinstance(value, list) and len(value) > 0:
        # For non-empty lists, include the element type
        return f"list_{type(value[0]).__name__}"
    return type(value).__name__


@dataclass(frozen=True)
class BatchItem(Generic[StreamType]):
    channel_name: str
    timestamp: IntegralNanosecondsUTC
    value: StreamType
    tags: Mapping[str, str] | None = None

    def _to_api_batch_key(self) -> tuple[str, Sequence[tuple[str, str]], str]:
        return (
            self.channel_name,
            sorted(self.tags.items()) if self.tags is not None else [],
            _get_value_type_name(self.value),
        )

    @classmethod
    def sort_key(cls, item: Self) -> tuple[str, Sequence[tuple[str, str]], str]:
        return item._to_api_batch_key()


ScalarType: TypeAlias = str | float | int
"""Scalar value types supported for streaming."""

ArrayType: TypeAlias = list[float] | list[str]
"""Array value types supported for streaming."""

StreamValueType: TypeAlias = ScalarType | ArrayType
"""All value types supported for streaming (scalars and arrays)."""

DataStream: TypeAlias = WriteStreamBase[StreamValueType]
"""Stream type for asynchronously sending timeseries data to the Nominal backend."""

DataItem: TypeAlias = BatchItem[StreamValueType]
"""Individual item of timeseries data to stream to Nominal."""

LogStream: TypeAlias = WriteStreamBase[str]
"""Stream type for asynchronously sending log data to the Nominal backend."""

LogItem: TypeAlias = BatchItem[str]
"""Individual item of log data to stream to Nominal."""


@dataclass(frozen=True)
class WriteStream(WriteStreamBase[StreamType]):
    batch_size: int
    max_wait: timedelta
    _process_batch: Callable[[Sequence[BatchItem[StreamType]]], None]
    _executor: concurrent.futures.ThreadPoolExecutor
    _thread_safe_batch: ThreadSafeBatch[StreamType]
    _stop: threading.Event
    _pending_jobs: threading.BoundedSemaphore

    @classmethod
    def create(
        cls,
        batch_size: int,
        max_wait: timedelta,
        process_batch: Callable[[Sequence[BatchItem[StreamType]]], None],
    ) -> Self:
        """Create the stream.

        Args:
            batch_size: Maximum number of items to batch before flushing.
            max_wait: Maximum time to wait before flushing a batch.
            process_batch: Callable to process batches of items.
        """
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

    def __enter__(self) -> Self:
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
        value: StreamType,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a message to the queue after normalizing the timestamp to IntegralNanosecondsUTC.

        The message is added to the thread-safe batch and flushed if the batch
        size is reached.
        """
        dt_timestamp = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()
        item = BatchItem(channel_name, dt_timestamp, value, tags)
        self._thread_safe_batch.add([item])
        self._flush(condition=lambda size: size >= self.batch_size)

    def enqueue_float_array(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: Sequence[float],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add an array of floats to the queue after normalizing the timestamp.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: Array of float values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self.enqueue(channel_name, timestamp, list(value), tags)  # type: ignore[arg-type]

    def enqueue_string_array(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: Sequence[str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add an array of strings to the queue after normalizing the timestamp.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: Array of string values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self.enqueue(channel_name, timestamp, list(value), tags)  # type: ignore[arg-type]

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
            timeout: If wait is true, the time to wait for flush completion in seconds.
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
            now = time.monotonic()

            last_batch_time = self._thread_safe_batch.last_time
            timeout = max(self.max_wait.seconds - (now - last_batch_time), 0)
            self._stop.wait(timeout=timeout)

            # check if flush has been called in the mean time
            if self._thread_safe_batch.last_time > last_batch_time:
                continue

            self._flush()

    def close(self, wait: bool = True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread and flush any remaining batches.
        """
        self._stop.set()

        self._flush()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)


class ThreadSafeBatch(Generic[StreamType]):
    def __init__(self) -> None:
        """Thread-safe access to batch and last swap time."""
        self._batch: list[BatchItem[StreamType]] = []
        self._last_time = time.monotonic()
        self._lock = threading.Lock()

    def swap(self, condition: Callable[[int], bool] | None = None) -> list[BatchItem[StreamType]] | None:
        """Swap the current batch with an empty one and return the old batch.

        If condition is provided, the swap will only occur if the condition is met, otherwise None is returned.
        """
        with self._lock:
            if condition and not condition(len(self._batch)):
                return None
            batch = self._batch
            self._batch = []
            self._last_time = time.monotonic()
        return batch

    def add(self, items: Sequence[BatchItem[StreamType]]) -> None:
        with self._lock:
            self._batch.extend(items)

    @property
    def last_time(self) -> float:
        with self._lock:
            return self._last_time
