from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from queue import Empty, Queue
from typing import Generic, Iterable, List, Protocol, TypeVar

from nominal.core._stream.write_stream import BatchItem
from nominal.ts import IntegralNanosecondsUTC

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

# Maximum value for a 64-bit signed integer
MAX_INT64 = 2**63 - 1


@dataclass(frozen=True)
class Batch(Generic[_T]):
    items: List[BatchItem[_T]]
    oldest_timestamp: IntegralNanosecondsUTC
    newest_timestamp: IntegralNanosecondsUTC


class ReadQueue(Protocol[_T_co]):
    def get(self, block: bool = True, timeout: float | None = None) -> _T_co: ...
    def task_done(self) -> None: ...


class WriteQueue(Protocol[_T_contra]):
    def put(self, item: _T_contra) -> None: ...
    def shutdown(self, immediate: bool = False) -> None: ...


def _timed_batch(
    q: ReadQueue[BatchItem[_T]], max_batch_size: int, max_batch_duration: timedelta
) -> Iterable[Batch[_T]]:
    """Yield batches of items from a queue, either when the batch size is reached or the batch window expires.

    Will not yield empty batches.
    """
    batch: list[BatchItem[_T]] = []
    oldest_timestamp: IntegralNanosecondsUTC = MAX_INT64
    newest_timestamp: IntegralNanosecondsUTC = 0
    next_batch_time = time.monotonic() + max_batch_duration.total_seconds()
    while True:
        now = time.monotonic()
        try:
            item = q.get(timeout=max(0, next_batch_time - now))
            if isinstance(item, QueueShutdown):
                if batch:
                    yield Batch(batch, oldest_timestamp, newest_timestamp)
                return

            oldest_timestamp = min(oldest_timestamp, item.timestamp)
            newest_timestamp = max(newest_timestamp, item.timestamp)

            batch.append(item)
            q.task_done()
        except Empty:  # timeout
            pass
        if len(batch) >= max_batch_size or time.monotonic() >= next_batch_time:
            if batch:
                yield Batch(batch, oldest_timestamp, newest_timestamp)
                oldest_timestamp = MAX_INT64
                newest_timestamp = 0
                batch = []
            next_batch_time = now + max_batch_duration.total_seconds()


def _enqueue_timed_batches(
    items: ReadQueue[BatchItem[_T]],
    batches: WriteQueue[Batch[_T] | QueueShutdown],
    max_batch_size: int,
    max_batch_duration: timedelta,
) -> None:
    """Enqueue items from a queue into batches."""
    for batch in _timed_batch(items, max_batch_size, max_batch_duration):
        batches.put(batch)
    batches.put(QueueShutdown())


def spawn_batching_thread(
    items: ReadQueue[BatchItem[_T] | QueueShutdown],
    max_batch_size: int,
    max_batch_duration: timedelta,
    max_queue_size: int = 0,
) -> tuple[threading.Thread, ReadQueue[Batch[_T]]]:
    """Enqueue items from a queue into batches in a separate thread."""
    batches: Queue[Batch[_T]] = Queue(maxsize=max_queue_size)
    batching_thread = threading.Thread(
        target=_enqueue_timed_batches, args=(items, batches, max_batch_size, max_batch_duration), daemon=True
    )
    batching_thread.start()
    return batching_thread, batches


class QueueShutdown:
    """Sentinel class to signal queue shutdown."""

    pass


def iter_queue(q: ReadQueue[_T]) -> Iterable[_T]:
    """Iterate over items in a queue."""
    while True:
        item = q.get()
        if isinstance(item, QueueShutdown):
            break
        yield item
        q.task_done()
