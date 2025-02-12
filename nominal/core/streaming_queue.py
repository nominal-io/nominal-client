from __future__ import annotations

import logging
from queue import Queue
from time import monotonic as time
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Empty(Exception):
    """Exception raised when attempting to get an item from an empty queue."""

    pass


class Full(Exception):
    """Exception raised when attempting to put an item on a full queue in non-blocking mode."""

    pass


class ShutDown(Exception):
    """Exception raised when operations are attempted on a shut-down queue."""

    pass


class StreamingQueue(Queue[T], Generic[T]):
    """A thread-safe queue that supports standard blocking get/put as well as
    additional backpressure strategies via:
      - put_drop_newest: drop the new item if the queue is full.
      - put_drop_oldest: drop one or more oldest items until space is available.

    If maxsize is <= 0, the queue is considered unbounded.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """Initialize the queue with a maximum size."""
        super().__init__(maxsize=maxsize)
        self._shutdown = False

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        """Put an item into the queue.

        If block is True and timeout is None, block until a free slot is available.
        If the queue is full and non-blocking mode is requested, raise Full.
        """
        with self.not_full:
            if self._shutdown:
                raise ShutDown("Queue has been shut down")
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full("Queue is full")
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                        if self._shutdown:
                            raise ShutDown("Queue has been shut down")
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full("Queue is full")
                        self.not_full.wait(remaining)
                        if self._shutdown:
                            raise ShutDown("Queue has been shut down")
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        """Remove and return an item from the queue.

        If block is True and timeout is None, block until an item is available.
        If no item is available in non-blocking mode, raise Empty.
        """
        with self.not_empty:
            if self._shutdown and self._qsize() == 0:
                raise ShutDown("Queue has been shut down")
            if not block:
                if not self._qsize():
                    raise Empty("Queue is empty")
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
                    if self._shutdown and self._qsize() == 0:
                        raise ShutDown("Queue has been shut down")
            else:
                if timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty("Queue is empty")
                    self.not_empty.wait(remaining)
                    if self._shutdown and self._qsize() == 0:
                        raise ShutDown("Queue has been shut down")
            item = self._get()
            self.not_full.notify()
            return item

    def shutdown(self, immediate: bool = False) -> None:
        """Shut down the queue, causing further put/get calls to raise ShutDown.

        If immediate is True, any queued tasks are dropped (with unfinished task
        counts adjusted) and threads waiting in join() are released.
        """
        with self.mutex:
            self._shutdown = True
            if immediate:
                while self._qsize():
                    self._get()
                    if self.unfinished_tasks > 0:
                        self.unfinished_tasks -= 1
                self.all_tasks_done.notify_all()
            self.not_empty.notify_all()
            self.not_full.notify_all()
