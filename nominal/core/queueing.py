from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence
from datetime import timedelta
from typing import Iterable, Protocol, TypeVar

from nominal.core.streaming_queue import Empty, ShutDown, StreamingQueue

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
    items: ReadQueue[_T], batches: WriteQueue[Sequence[_T]], max_batch_size: int, max_batch_duration: timedelta
) -> None:
    """Enqueue items from a queue into batches."""
    for batch in _timed_batch(items, max_batch_size, max_batch_duration):
        batches.put(batch)
    batches.shutdown()


def spawn_batching_thread(
    items: ReadQueue[_T],
    max_batch_size: int,
    max_batch_duration: timedelta,
    max_queue_size: int = 0,
) -> tuple[threading.Thread, ReadQueue[Sequence[_T]]]:
    """Enqueue items from a queue into batches in a separate thread."""
    batches = StreamingQueue[Sequence[_T]](maxsize=max_queue_size)
    batching_thread = threading.Thread(
        target=_enqueue_timed_batches, args=(items, batches, max_batch_size, max_batch_duration), daemon=True
    )
    batching_thread.start()
    return batching_thread, batches


def iter_queue(q: ReadQueue[_T]) -> Iterable[_T]:
    """Iterate over items in a queue."""
    try:
        while True:
            yield q.get()
            q.task_done()
    except ShutDown:
        pass


def enqueue_iterable(iterable: Iterable[_T], q: WriteQueue[_T]) -> None:
    """Enqueue items from an iterable into a queue."""
    for item in iterable:
        q.put(item)
    q.shutdown()
