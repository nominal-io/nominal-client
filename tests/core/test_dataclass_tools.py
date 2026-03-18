from __future__ import annotations

import time
from threading import Event, Lock, Thread

import pytest

from nominal._utils.dataclass_tools import LazyField


def test_lazy_field_caches_the_initialized_value():
    """LazyField should reuse the first computed value on later accesses."""
    lazy_value = LazyField[int]()
    call_count = 0

    def factory() -> int:
        nonlocal call_count
        call_count += 1
        return 42

    assert lazy_value.get_or_init(factory) == 42
    assert lazy_value.get_or_init(factory) == 42
    assert call_count == 1
    assert lazy_value.is_initialized()
    assert lazy_value.get() == 42


def test_lazy_field_get_raises_before_initialization():
    """LazyField.get should fail before any value has been initialized."""
    lazy_value = LazyField[int]()

    assert not lazy_value.is_initialized()

    with pytest.raises(LookupError, match="not been initialized"):
        lazy_value.get()


def test_lazy_field_initializes_once_across_threads():
    """LazyField should run its initializer at most once even when multiple threads race to read it."""
    lazy_value = LazyField[int]()
    initializer_started = Event()
    release_initializer = Event()
    initializer_count = 0
    initializer_count_lock = Lock()
    results: list[int] = []
    results_lock = Lock()

    def factory() -> int:
        nonlocal initializer_count
        with initializer_count_lock:
            initializer_count += 1
            initializer_started.set()

        release_initializer.wait(timeout=1)
        return 42

    def worker() -> None:
        value = lazy_value.get_or_init(factory)
        with results_lock:
            results.append(value)

    threads = [Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()

    assert initializer_started.wait(timeout=1)
    time.sleep(0.05)
    assert initializer_count == 1

    release_initializer.set()

    for thread in threads:
        thread.join()

    assert results == [42, 42, 42, 42]
