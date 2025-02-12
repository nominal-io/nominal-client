import logging
import threading
import time
from datetime import timedelta
from queue import Empty, Full, Queue, ShutDown
from typing import Iterable, Protocol, TypeVar

from nominal.ts import BackpressureMode

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


class BackpressureQueue(Queue[_T]):
    """A queue that implements different backpressure strategies."""

    def __init__(self, maxsize: int = 0, mode: BackpressureMode = BackpressureMode.BLOCK):
        """Initialize the queue with a maximum size and a backpressure mode."""
        super().__init__(maxsize=maxsize)
        self.mode = mode

    def put_with_backpressure(self, item: _T) -> None:
        """Put an item in the queue using the configured backpressure strategy."""
        if self.maxsize == 0 or self.mode == BackpressureMode.BLOCK:
            self.put(item)
            return

        try:
            if self.mode == BackpressureMode.DROP_NEWEST:
                try:
                    self.put_nowait(item)
                except Full:
                    logger.warning("Queue full, dropping new item due to backpressure mode")
            elif self.mode == BackpressureMode.DROP_OLDEST:
                while True:
                    try:
                        self.put_nowait(item)
                        break
                    except Full:
                        try:
                            self.get_nowait()
                        except Empty:
                            continue
        except Exception as e:
            logger.error(f"Unexpected error during enqueue: {e}")
            raise
