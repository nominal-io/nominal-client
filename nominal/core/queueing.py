from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import timedelta
from queue import Empty, Queue
from typing import Iterable, Protocol, TypeVar

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

logger = logging.getLogger(__name__)


class ReadQueue(Protocol[_T_co]):
    def get(self, block: bool = True, timeout: float | None = None) -> _T_co: ...
    def task_done(self) -> None: ...


class WriteQueue(Protocol[_T_contra]):
    def put(self, item: _T_contra) -> None: ...
    def shutdown(self, immediate: bool = False) -> None: ...


def _timed_batch(q: ReadQueue[_T], max_batch_size: int, max_batch_duration: timedelta) -> Iterable[Sequence[_T]]:
    """Yield batches of items from a queue, either when the batch size is reached or the batch window expires.

    Will not yield empty batches.
    """
    batch: list[_T] = []
    next_batch_time = time.time() + max_batch_duration.total_seconds()
    while True:
        now = time.time()
        try:
            item = q.get(timeout=max(0, next_batch_time - now))
            if isinstance(item, QueueShutdown):
                if batch:
                    yield batch
                return
            batch.append(item)
            q.task_done()
        except Empty:  # timeout
            pass
        if len(batch) >= max_batch_size or time.time() >= next_batch_time:
            if batch:
                yield batch
                batch = []
            next_batch_time = now + max_batch_duration.total_seconds()


def _enqueue_timed_batches(
    items: ReadQueue[_T],
    batches: WriteQueue[Sequence[_T] | QueueShutdown],
    max_batch_size: int,
    max_batch_duration: timedelta,
) -> None:
    """Enqueue items from a queue into batches."""
    for batch in _timed_batch(items, max_batch_size, max_batch_duration):
        batches.put(batch)
    batches.put(QueueShutdown())


def spawn_batching_thread(
    items: ReadQueue[_T | QueueShutdown],
    max_batch_size: int,
    max_batch_duration: timedelta,
    max_queue_size: int = 0,
) -> tuple[threading.Thread, ReadQueue[Sequence[_T]]]:
    """Enqueue items from a queue into batches in a separate thread."""
    batches = Queue[Sequence[_T]](maxsize=max_queue_size)
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


class BackpressureQueue(ABC, Queue[_T]):
    """Abstract base class for queues with different backpressure strategies."""

    @abstractmethod
    def put_with_backpressure(self, item: _T) -> None:
        """Put an item in the queue using the specific backpressure strategy."""
        pass


class BlockingQueue(BackpressureQueue[_T]):
    """Queue that blocks when full."""

    def put_with_backpressure(self, item: _T) -> None:
        self.put(item)
