from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from types import TracebackType
from typing import Any, Callable, Generic, Mapping, Self, Sequence, Type, TypeAlias

from nominal.core._stream.write_stream_base import StreamType, WriteStreamBase
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


class PointType(Enum):
    """Enumeration of all supported point value types for streaming."""

    STRING = "STRING"
    """Scalar string value."""

    DOUBLE = "DOUBLE"
    """Scalar float/double value."""

    INT = "INT"
    """Scalar integer value."""

    DOUBLE_ARRAY = "DOUBLE_ARRAY"
    """Array of float/double values."""

    STRING_ARRAY = "STRING_ARRAY"
    """Array of string values."""

    STRUCT = "STRUCT"
    """JSON-serializable struct (dict)."""

    def to_array_type(self) -> PointType:
        """Convert a scalar PointType to its corresponding array type.

        Raises:
            ValueError: If the PointType is already an array type, INT, or unsupported.
        """
        match self:
            case PointType.STRING:
                return PointType.STRING_ARRAY
            case PointType.DOUBLE:
                return PointType.DOUBLE_ARRAY
            case PointType.INT:
                raise ValueError(
                    "Integer arrays are not supported. Use enqueue_float_array() with float values, "
                    "or convert your integers to floats before streaming."
                )
            case _:
                raise ValueError(f"Cannot convert {self} to array type (already an array or unsupported)")


def _infer_scalar_type(value: object) -> PointType:
    """Infer the scalar PointType from a single value.

    Args:
        value: A scalar value (str, float, or int).

    Returns:
        The inferred scalar PointType.

    Raises:
        ValueError: If the value type is unsupported.
    """
    if isinstance(value, str):
        return PointType.STRING
    elif isinstance(value, float):
        return PointType.DOUBLE
    elif isinstance(value, int):
        return PointType.INT
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")


def infer_point_type(value: object, explicit_type: PointType | None = None) -> PointType:
    """Infer the point type from a value, or use the explicit type if provided.

    Args:
        value: The value to infer the type from.
        explicit_type: If provided, this type is returned directly (used for empty arrays).

    Returns:
        The inferred or explicit PointType.

    Raises:
        ValueError: If the type cannot be inferred (e.g., empty list without explicit type)
                   or if the value type is unsupported.
    """
    # If explicit type is provided, use it (allows empty arrays)
    if explicit_type is not None:
        return explicit_type

    # Handle struct types
    if isinstance(value, dict):
        return PointType.STRUCT

    # Handle list types (arrays) - recurse to infer element type, then convert to array type
    if isinstance(value, list):
        if len(value) == 0:
            raise ValueError(
                "Cannot infer type from empty array. Use enqueue_float_array() or "
                "enqueue_string_array() to explicitly specify the array type."
            )
        # Recurse to get the scalar type of the first element, then convert to array type
        element_type = _infer_scalar_type(value[0])
        return element_type.to_array_type()

    # Handle scalar types
    return _infer_scalar_type(value)


@dataclass(frozen=True)
class BatchItem(Generic[StreamType]):
    """A single item in a batch to be written to the stream.

    Attributes:
        channel_name: Name of the channel.
        timestamp: Timestamp in nanoseconds.
        value: The value to write.
        tags: Optional key-value tags.
        point_type_override: Explicit point type override, used when the type cannot be
            inferred from the value (e.g., empty arrays). If None, the type is inferred.
    """

    channel_name: str
    timestamp: IntegralNanosecondsUTC
    value: StreamType
    tags: Mapping[str, str] | None = None
    point_type_override: PointType | None = None

    def get_point_type(self) -> PointType:
        """Get the point type, using override if set, otherwise inferring from value."""
        return infer_point_type(self.value, self.point_type_override)

    def _to_api_batch_key(self) -> tuple[str, Sequence[tuple[str, str]], str]:
        """Generate a key for grouping batch items by channel, tags, and type."""
        return (
            self.channel_name,
            sorted(self.tags.items()) if self.tags is not None else [],
            self.get_point_type().name,
        )

    @classmethod
    def sort_key(cls, item: Self) -> tuple[str, Sequence[tuple[str, str]], str]:
        return item._to_api_batch_key()


ScalarType: TypeAlias = str | float | int
"""Scalar value types supported for streaming."""

ArrayType: TypeAlias = list[float] | list[str]
"""Array value types supported for streaming."""

StructType: TypeAlias = dict[str, Any]
"""Struct value type supported for streaming."""

StreamValueType: TypeAlias = ScalarType | ArrayType | StructType
"""All value types supported for streaming (scalars and arrays)."""

DataStream: TypeAlias = WriteStreamBase[ScalarType]
"""Stream type for asynchronously sending timeseries data to the Nominal backend."""

DataItem: TypeAlias = BatchItem[StreamValueType]
"""Individual item of timeseries data to stream to Nominal (scalars or arrays)."""

LogStream: TypeAlias = WriteStreamBase[str]
"""Stream type for asynchronously sending log data to the Nominal backend."""

LogItem: TypeAlias = BatchItem[str]
"""Individual item of log data to stream to Nominal."""


@dataclass
class _UploadState:
    """Track outstanding uploads and the first unreported failure under one condition."""

    pending: int = 0
    error: BaseException | None = None
    condition: threading.Condition = field(default_factory=threading.Condition)


@dataclass(frozen=True)
class WriteStream(WriteStreamBase[StreamType]):
    """A buffered asynchronous writer for time-series or log data."""

    batch_size: int
    max_wait: timedelta
    _process_batch: Callable[[Sequence[BatchItem[StreamType]]], None]
    _executor: concurrent.futures.ThreadPoolExecutor
    _thread_safe_batch: ThreadSafeBatch[StreamType]
    _stop: threading.Event
    _pending_jobs: threading.BoundedSemaphore
    _uploads: _UploadState = field(default_factory=_UploadState, repr=False)

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
        """Close the stream, preserving an exception raised by the context body."""
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise

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

    def _enqueue_array(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: list[float] | list[str],
        tags: Mapping[str, str] | None,
        point_type: PointType,
    ) -> None:
        """Internal helper to enqueue an array value with explicit type."""
        dt_timestamp = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()
        item: DataItem = BatchItem(channel_name, dt_timestamp, value, tags, point_type_override=point_type)
        self._thread_safe_batch.add([item])  # type: ignore[list-item]
        self._flush(condition=lambda size: size >= self.batch_size)

    def enqueue_float_array(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: list[float],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add an array of floats to the queue after normalizing the timestamp.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: List of float values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self._enqueue_array(channel_name, timestamp, value, tags, PointType.DOUBLE_ARRAY)

    def enqueue_string_array(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: list[str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add an array of strings to the queue after normalizing the timestamp.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: List of string values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self._enqueue_array(channel_name, timestamp, value, tags, PointType.STRING_ARRAY)

    def enqueue_struct(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: dict[str, Any],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a struct (dict) to the queue after normalizing the timestamp.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: Dict to write to the specified channel. Must be JSON-serializable.
            tags: Key-value tags associated with the data being uploaded.
        """
        dt_timestamp = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()
        item: DataItem = BatchItem(channel_name, dt_timestamp, value, tags, point_type_override=PointType.STRUCT)
        self._thread_safe_batch.add([item])  # type: ignore[list-item]
        self._flush(condition=lambda size: size >= self.batch_size)

    def _flush(self, condition: Callable[[int], bool] | None = None) -> concurrent.futures.Future[None] | None:
        with self._uploads.condition:
            batch = self._thread_safe_batch.swap(condition)
            if not batch:
                return None
            # Register before submission or backpressure can hide a timeout-worker batch from flush().
            self._uploads.pending += 1

        acquired = False
        try:
            acquired = self._pending_jobs.acquire()
            future = self._executor.submit(self._process_batch, batch)
        except BaseException:
            if acquired:
                self._pending_jobs.release()
            with self._uploads.condition:
                self._uploads.pending -= 1
                self._uploads.condition.notify_all()
            raise

        def process_future(fut: concurrent.futures.Future[None]) -> None:
            """Retain the first upload failure and signal completion, including cancellation."""
            cancelled = fut.cancelled()
            error = None if cancelled else fut.exception()
            with self._uploads.condition:
                if error is not None and self._uploads.error is None:
                    self._uploads.error = error
                self._uploads.pending -= 1
                self._uploads.condition.notify_all()
            self._pending_jobs.release()
            if cancelled:
                return
            if error is not None:
                logger.error("Batched upload task failed with exception", exc_info=error)
            else:
                logger.debug("Batched upload task completed")

        logger.debug("Starting flush with %s records", len(batch))
        future.add_done_callback(process_future)
        return future

    def flush(self, wait: bool = False, timeout: float | None = None) -> None:
        """Submit buffered records and optionally wait for all outstanding uploads.

        Stop enqueueing before calling a blocking flush. Upload completion does not
        guarantee that the data is already visible to queries. Uploads are not retried here.

        Args:
            wait: Wait for outstanding uploads, including batches submitted earlier. Raise
                the first unreported upload exception after they finish. Other failures
                are logged. A reported exception is cleared, so a later wait can succeed.
            timeout: Maximum wait in seconds. If uploads are still pending, log a warning
                and retain any failure for the next blocking flush or close.

                **Note:** Passing ``None`` waits indefinitely. Ignored unless ``wait=True``.
                Time spent submitting the buffered batch is not included in this limit.

        Raises:
            Exception: The first unreported upload exception, when ``wait=True`` and
                all outstanding uploads finish within the timeout.
        """
        self._flush()
        if not wait:
            return

        with self._uploads.condition:
            completed = self._uploads.condition.wait_for(lambda: self._uploads.pending == 0, timeout)
            if completed:
                error = self._uploads.error
                self._uploads.error = None
        if not completed:
            logger.warning("Upload task still pending after flushing batch... increase timeout or setting to None")
        elif error is not None:
            raise error

    def _process_timeout_batches(self) -> None:
        max_wait_seconds = self.max_wait.total_seconds()

        while not self._stop.is_set():
            now = time.monotonic()

            last_batch_time = self._thread_safe_batch.last_time
            timeout = max(max_wait_seconds - (now - last_batch_time), 0)
            self._stop.wait(timeout=timeout)

            # check if flush has been called in the mean time
            if self._thread_safe_batch.last_time > last_batch_time:
                continue

            self._flush()

    def close(self, wait: bool = True) -> None:
        """Stop periodic flushing, submit buffered records, and shut down the workers.

        Stop enqueueing before closing. Workers are released even if an upload fails.
        Successful upload completion does not guarantee immediate query visibility.

        Args:
            wait: Wait for all uploads and raise the first unreported upload exception.
                If false, return without reporting failures and cancel uploads that
                have not started; running uploads continue.

        Raises:
            Exception: The first unreported upload exception, when ``wait=True``.
        """
        self._stop.set()
        try:
            self.flush(wait=wait)
        finally:
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
