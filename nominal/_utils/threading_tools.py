from __future__ import annotations

from multiprocessing.managers import SyncManager, ValueProxy
from threading import Lock

from typing_extensions import Self


class SharedCounter:
    """Multi-processing friendly atomic counter."""

    def __init__(self, lock: Lock, initial_value: ValueProxy):
        self._lock = lock
        self._value = initial_value

    @classmethod
    def from_manager(cls, manager: SyncManager, initial_value: float = 0.0) -> Self:
        """Create a SharedCounter using a lock from a multiprocessing.Manager().

        NOTE: the shared counter instance is valid only so long as the provided manager is valid.
        """
        return cls(
            manager.Lock(),
            manager.Value("d", initial_value),
        )

    def increment(self, amount: float = 1.0):
        with self._lock:
            self._value.value += amount

    def value(self) -> float:
        return self._value.value
