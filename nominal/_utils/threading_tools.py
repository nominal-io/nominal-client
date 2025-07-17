from __future__ import annotations

import multiprocessing
import queue
import time
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event, Lock
from typing import Generic, TypeVar

from typing_extensions import Self


class SharedCounter:
    """Multi-processing friendly atomic counter."""

    def __init__(self, lock: Lock, initial_value: Synchronized[float]):
        self._lock = lock
        self._value = initial_value

    @classmethod
    def from_value(cls, initial_value: float = 0.0) -> Self:
        """Create a SharedCounter with a starting value."""
        return cls(
            multiprocessing.Lock(),
            multiprocessing.Value("d", initial_value),
        )

    def increment(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value.value += amount

    def value(self) -> float:
        with self._lock:
            return self._value.value


class StopWorking:
    """Sentinel value to tell task workers to stop working."""


T = TypeVar("T")


class StoppableQueue(Generic[T]):
    def __init__(
        self,
        queue: multiprocessing.Queue[T | StopWorking],
        stop_flag: Event,
        interrupt_flag: Event,
    ):
        self._queue = queue
        self.stop_flag = stop_flag
        self.interrupt_flag = interrupt_flag

    @classmethod
    def from_size(cls, queue_size: int = 0) -> Self:
        stop_flag = multiprocessing.Event()
        interrupt_flag = multiprocessing.Event()

        return cls(
            multiprocessing.Queue(maxsize=queue_size),
            stop_flag,
            interrupt_flag,
        )

    def stop(self) -> None:
        """Immediately stop all processes blocking on the queue and stop future enqueues or dequeues."""
        self.stop_flag.set()

    def interrupt(self, num_stops: int | None = None) -> None:
        """Prevent new items from being added to the queue, useful during shutdown operations"""
        self.interrupt_flag.set()
        if num_stops:
            for _ in range(num_stops):
                self.put(StopWorking())

    def wait(self) -> None:
        """Blocks until all tasks are completed within the queue.

        Should be called after using interrupt(), but unecessary after a stop()
        """
        while not self.stop_flag.is_set():
            if self._queue.empty():
                return

            time.sleep(0.25)

    def get(self) -> T | None:
        """Block until stop is signalled or data is received and return the oldest member of the queue."""
        while not self.stop_flag.is_set():
            try:
                item = self._queue.get(timeout=0.1)
                if isinstance(item, StopWorking):
                    return None
                else:
                    return item
            except queue.Empty:
                continue

        return None

    def put(self, item: T | StopWorking) -> None:
        """Block until stop is signalled or space is available and insert an element in the queue"""
        # If stop flag is set, or the interrupt flag is set and the item isn't a stop work order,
        # stop trying to add to the queue
        while not (self.stop_flag.is_set() or (self.interrupt_flag.is_set() and not isinstance(item, StopWorking))):
            try:
                self._queue.put(item, timeout=0.1)
                return
            except queue.Full:
                continue
