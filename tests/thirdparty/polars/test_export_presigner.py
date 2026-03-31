from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from nominal.thirdparty.polars.export_presigner import ExportPresigner, SignedExport


@pytest.fixture
def make_sign_fn():
    """Factory fixture for creating test sign functions with optional delay."""

    def _make(delay: float = 0.0):
        call_count = 0
        lock = threading.Lock()

        def sign(job) -> SignedExport:
            nonlocal call_count
            if delay > 0:
                time.sleep(delay)
            with lock:
                call_count += 1
            return SignedExport(job=job, url=f"https://s3.example.com/{job}", file_size_bytes=1024)

        sign.call_count = lambda: call_count  # type: ignore[attr-defined]
        return sign

    return _make


def test_sign_all_yields_in_input_order(make_sign_fn):
    """Results are yielded in input order despite parallel signing with variable latency."""
    import random

    def random_delay_sign(job) -> SignedExport:
        time.sleep(random.uniform(0, 0.05))
        return SignedExport(job=job, url=f"https://s3/{job}", file_size_bytes=100)

    presigner = ExportPresigner(sign_fn=random_delay_sign, max_ahead=4)
    jobs = list(range(20))
    results = list(presigner.sign_all(jobs))

    assert [r.job for r in results] == jobs


def test_sign_all_empty_input():
    """An empty iterable produces an empty iterator."""
    presigner = ExportPresigner(sign_fn=MagicMock(), max_ahead=4)
    assert list(presigner.sign_all([])) == []


def test_sign_all_propagates_errors():
    """If sign_fn raises, the exception propagates to the caller."""

    def failing_sign(job):
        raise RuntimeError("API error")

    presigner = ExportPresigner(sign_fn=failing_sign, max_ahead=4)
    with pytest.raises(RuntimeError, match="API error"):
        list(presigner.sign_all([1]))


def test_sign_all_respects_max_ahead():
    """Never more than max_ahead jobs are in-flight concurrently."""
    max_concurrent = 0
    current = 0
    lock = threading.Lock()

    def tracking_sign(job) -> SignedExport:
        nonlocal max_concurrent, current
        with lock:
            current += 1
            max_concurrent = max(max_concurrent, current)
        time.sleep(0.02)
        with lock:
            current -= 1
        return SignedExport(job=job, url="https://s3/x", file_size_bytes=100)

    presigner = ExportPresigner(sign_fn=tracking_sign, max_ahead=3)
    list(presigner.sign_all(range(10)))

    assert max_concurrent <= 3


def test_backpressure_pauses_signing(make_sign_fn):
    """When the consumer stops pulling, no more than max_ahead + consumed jobs are signed."""
    sign_fn = make_sign_fn(delay=0.01)
    presigner = ExportPresigner(sign_fn=sign_fn, max_ahead=3)

    it = presigner.sign_all(range(20))
    # Consume only 2
    next(it)
    next(it)
    time.sleep(0.1)  # let the window fill

    # Should have signed at most max_ahead + 2 consumed = 5
    # (3 in window + 2 already yielded)
    assert sign_fn.call_count() <= 5
