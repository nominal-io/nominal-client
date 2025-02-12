from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from datetime import timedelta
from queue import Empty, Queue, ShutDown
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


def _timed_batch(q: ReadQueue[_T], max_batch_size: int, max_batch_duration: timedelta) -> Iterable[list[_T]]:
    """Yield batches of items from a queue, either when the batch size is reached or the batch window expires.

    Will not yield empty batches.
    """
    batch = []
    next_batch_time = time.time() + max_batch_duration.total_seconds()
    while True:
        now = time.time()
        try:
            item = q.get(timeout=max(0, next_batch_time - now))
            batch.append(item)
            q.task_done()
        except Empty:  # timeout
            pass
        except ShutDown:
            if batch:
                yield batch
            return
        if len(batch) >= max_batch_size or time.time() >= next_batch_time:
            if batch:
                yield batch
                batch = []
            next_batch_time = now + max_batch_duration.total_seconds()


def _enqueue_timed_batches(
    items: ReadQueue[_T], batches: WriteQueue[list[_T]], max_batch_size: int, max_batch_duration: timedelta
) -> None:
    """Enqueue items from a queue into batches."""
    for batch in _timed_batch(items, max_batch_size, max_batch_duration):
        batches.put(batch)
    batches.shutdown()


def spawn_batching_thread(
    items: ReadQueue[_T],
    max_batch_size: int,
    max_batch_duration: timedelta,
    maxsize: int = 0,
) -> tuple[threading.Thread, ReadQueue[list[_T]]]:
    """Enqueue items from a queue into batches in a separate thread.

    Args:
        items: input queue
        max_batch_size: maximum number of items in a batch
        max_batch_duration: maximum time between items in a batch
        maxsize: maximum size of the batch queue (0 for unlimited)
    """
    batches: Queue[list[_T]] = Queue(maxsize=maxsize)
    batching_thread = threading.Thread(
        target=_enqueue_timed_batches, args=(items, batches, max_batch_size, max_batch_duration), daemon=True
    )
    batching_thread.start()
    return batching_thread, batches


def iter_queue(q: ReadQueue[_T]) -> Iterable[_T]:
    """Iterate over items in a queue.

    Marks items as done only _after_ they are yielded (i.e. after a consumer uses them and pulls for the next item).
    """
    try:
        while True:
            yield q.get()
            q.task_done()
    except ShutDown:
        pass


def enqueue_iterable(iterable: Iterable[_T], q: WriteQueue[_T]) -> None:
    """Enqueue items from an iterable into a queue.

    Closes the queue when the iterable is exhausted.
    """
    for item in iterable:
        q.put(item)
    q.shutdown()


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


class DropNewestQueue(BackpressureQueue[_T]):
    """Queue that drops new items when full."""

    def put_with_backpressure(self, item: _T) -> None:
        """Put an item in the queue, dropping it if the queue is full."""
        with self.mutex:  # should I use self.not_full (the condition lock)?
            if self.is_shutdown:
                raise ShutDown
            if self._qsize() < self.maxsize:
                self._put(item)
                self.unfinished_tasks += 1
                self.not_empty.notify()
            else:
                logger.warning("Queue full, dropping new item")


class DropOldestQueue(BackpressureQueue[_T]):
    """Queue that drops oldest items when full (ring buffer)."""

    def put_with_backpressure(self, item: _T) -> None:
        """Put an item in the queue, removing oldest items if the queue is full."""
        with self.mutex:
            if self.is_shutdown:
                raise ShutDown
            while self._qsize() >= self.maxsize:
                self._get()
                self.unfinished_tasks -= 1
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
