"""Tests for rust_experimental write stream threading behavior.

Verifies the signal handler bug fix in nominal-streaming 0.7.11:
- Bug: ValueError("signal only works in main thread of the main interpreter")
- Fixed: Stream context can be entered from worker threads without error
"""

from __future__ import annotations

import multiprocessing
import threading

import pytest

nominal_streaming = pytest.importorskip("nominal_streaming")

SIGNAL_ERROR_MSG = "signal only works in main thread"


def _enter_stream_context() -> None:
    """Create and enter a stream context (exercises signal handler registration)."""
    from nominal_streaming import NominalDatasetStream

    stream = NominalDatasetStream.create(
        "test-api-key",
        "https://test-base-url",
    ).with_core_consumer("ri.test.test.datasource.test")

    with stream:
        pass


def test_rust_stream_worker_thread():
    # Test that stream context can be entered from a worker thread
    error: BaseException | None = None

    def worker():
        nonlocal error
        try:
            _enter_stream_context()
        except BaseException as e:
            error = e

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "Worker thread timed out"
    if error is not None:
        if isinstance(error, ValueError) and SIGNAL_ERROR_MSG in str(error):
            pytest.fail(f"Signal handler bug: {error}")
        raise error


def _subprocess_worker(result_queue):
    """Subprocess target: enter stream context from a worker thread."""
    import threading

    from nominal_streaming import NominalDatasetStream

    result = {"error_type": None, "error_msg": None}

    def worker():
        try:
            stream = NominalDatasetStream.create(
                "test-api-key",
                "https://test-base-url",
            ).with_core_consumer("ri.test.test.datasource.test")
            with stream:
                pass
        except BaseException as e:
            result["error_type"] = type(e).__name__
            result["error_msg"] = str(e)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=10)

    if thread.is_alive():
        result = {"error_type": "TimeoutError", "error_msg": "Worker thread timed out"}
    result_queue.put(result)


def test_rust_stream_subprocess_worker_thread():
    # Test that stream context can be entered from a worker thread in a spawned subprocess
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()

    proc = ctx.Process(target=_subprocess_worker, args=(result_queue,))
    proc.start()
    proc.join(timeout=15)

    assert not proc.is_alive(), "Subprocess timed out"
    assert not result_queue.empty(), "No result from subprocess"

    result = result_queue.get()
    if result["error_type"] is not None:
        if result["error_type"] == "ValueError" and SIGNAL_ERROR_MSG in result["error_msg"]:
            pytest.fail(f"Signal handler bug: {result['error_msg']}")
        pytest.fail(f"{result['error_type']}: {result['error_msg']}")
