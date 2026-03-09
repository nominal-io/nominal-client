from __future__ import annotations

from threading import Lock
from typing import Callable, Generic, Iterable, TypeVar, cast

T = TypeVar("T")
_UNSET = object()


class LazyField(Generic[T]):
    """Thread-safe container for lazily initialized dataclass state."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._value: T | object = _UNSET

    def is_initialized(self) -> bool:
        """Return whether this field has been initialized."""
        return self._value is not _UNSET

    def get(self) -> T:
        """Return the cached value if initialized, otherwise raise LookupError."""
        if self._value is _UNSET:
            raise LookupError("LazyField has not been initialized")

        return cast(T, self._value)

    def get_or_init(self, factory: Callable[[], T]) -> T:
        """Return the cached value, initializing it exactly once on first access."""
        with self._lock:
            if self._value is _UNSET:
                self._value = factory()

            return cast(T, self._value)


def update_dataclass(self: T, other: T, fields: Iterable[str]) -> None:
    """Update dataclass attributes, copying from `other` into `self`.

    Uses __dict__ to update `self` to update frozen dataclasses.
    """
    for field in fields:
        self.__dict__[field] = getattr(other, field)
