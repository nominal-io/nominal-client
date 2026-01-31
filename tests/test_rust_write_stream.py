"""Tests for rust_experimental write stream creation in different threading contexts."""

from __future__ import annotations

import multiprocessing
import sys
import threading
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.connection import StreamingConnection


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.storage_writer = MagicMock()
    clients.auth_header = "test-auth-header"
    return clients


@pytest.fixture
def mock_connection(mock_clients):
    return StreamingConnection(
        rid="test-connection-rid",
        name="Test Connection",
        description="A connection for testing",
        _clients=mock_clients,
        nominal_data_source_rid="test-datasource-rid",
    )


def _create_mock_rust_stream_class():
    """Create a mock RustWriteStream class."""
    mock_stream = MagicMock()
    mock_class = MagicMock()
    mock_class._from_datasource = MagicMock(return_value=mock_stream)
    return mock_class, mock_stream


def test_get_write_stream_rust_experimental_main_thread(mock_connection):
    # Test that rust_experimental stream can be created from the main thread
    mock_class, _ = _create_mock_rust_stream_class()
    mock_module = MagicMock()
    mock_module.RustWriteStream = mock_class

    with patch.dict(sys.modules, {
        "nominal.experimental.rust_streaming.rust_write_stream": mock_module,
    }):
        mock_connection.get_write_stream(
            data_format="rust_experimental",
            batch_size=1000,
            max_wait=timedelta(seconds=1),
        )


def test_get_write_stream_rust_experimental_worker_thread(mock_connection):
    # Test that rust_experimental stream can be created from a worker thread
    result = {"success": False, "error": None}

    def worker():
        try:
            mock_class, _ = _create_mock_rust_stream_class()
            mock_module = MagicMock()
            mock_module.RustWriteStream = mock_class

            with patch.dict(sys.modules, {
                "nominal.experimental.rust_streaming.rust_write_stream": mock_module,
            }):
                mock_connection.get_write_stream(
                    data_format="rust_experimental",
                    batch_size=1000,
                    max_wait=timedelta(seconds=1),
                )
                result["success"] = True
        except Exception as e:
            result["error"] = e

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive(), "Worker thread timed out"
    assert result["error"] is None, f"Worker thread raised exception: {result['error']}"
    assert result["success"]


def _subprocess_test_main_thread(result_queue):
    """Run in subprocess: test stream creation in main thread."""
    try:
        from unittest.mock import MagicMock, patch
        import sys
        from datetime import timedelta
        from nominal.core.connection import StreamingConnection

        # Set up mocks within the subprocess
        mock_stream = MagicMock()
        mock_class = MagicMock()
        mock_class._from_datasource = MagicMock(return_value=mock_stream)
        mock_module = MagicMock()
        mock_module.RustWriteStream = mock_class

        # Create mock connection
        mock_clients = MagicMock()
        mock_clients.storage_writer = MagicMock()
        mock_clients.auth_header = "test-auth-header"

        connection = StreamingConnection(
            rid="test-connection-rid",
            name="Test Connection",
            description="A connection for testing",
            _clients=mock_clients,
            nominal_data_source_rid="test-datasource-rid",
        )

        with patch.dict(sys.modules, {
            "nominal.experimental.rust_streaming.rust_write_stream": mock_module,
        }):
            connection.get_write_stream(
                data_format="rust_experimental",
                batch_size=1000,
                max_wait=timedelta(seconds=1),
            )

        result_queue.put({"success": True, "error": None})
    except Exception as e:
        result_queue.put({"success": False, "error": str(e)})


def _subprocess_test_worker_thread(result_queue):
    """Run in subprocess: test stream creation in worker thread."""
    try:
        from unittest.mock import MagicMock, patch
        import sys
        import threading
        from datetime import timedelta
        from nominal.core.connection import StreamingConnection

        # Create mock connection
        mock_clients = MagicMock()
        mock_clients.storage_writer = MagicMock()
        mock_clients.auth_header = "test-auth-header"

        connection = StreamingConnection(
            rid="test-connection-rid",
            name="Test Connection",
            description="A connection for testing",
            _clients=mock_clients,
            nominal_data_source_rid="test-datasource-rid",
        )

        thread_result = {"success": False, "error": None}

        def worker():
            try:
                # Set up mocks within the worker thread
                mock_stream = MagicMock()
                mock_class = MagicMock()
                mock_class._from_datasource = MagicMock(return_value=mock_stream)
                mock_module = MagicMock()
                mock_module.RustWriteStream = mock_class

                with patch.dict(sys.modules, {
                    "nominal.experimental.rust_streaming.rust_write_stream": mock_module,
                }):
                    connection.get_write_stream(
                        data_format="rust_experimental",
                        batch_size=1000,
                        max_wait=timedelta(seconds=1),
                    )
                thread_result["success"] = True
            except Exception as e:
                thread_result["error"] = str(e)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=5)

        if thread.is_alive():
            result_queue.put({"success": False, "error": "Thread timed out"})
        else:
            result_queue.put(thread_result)
    except Exception as e:
        result_queue.put({"success": False, "error": str(e)})


def test_get_write_stream_rust_experimental_subprocess_main_thread():
    # Test that rust_experimental stream can be created from main thread of subprocess
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()

    proc = ctx.Process(target=_subprocess_test_main_thread, args=(result_queue,))
    proc.start()
    proc.join(timeout=10)

    assert not proc.is_alive(), "Subprocess timed out"
    assert not result_queue.empty(), "No result from subprocess"

    result = result_queue.get()
    assert result["error"] is None, f"Subprocess raised exception: {result['error']}"
    assert result["success"]


def test_get_write_stream_rust_experimental_subprocess_worker_thread():
    # Test that rust_experimental stream can be created from worker thread within subprocess
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()

    proc = ctx.Process(target=_subprocess_test_worker_thread, args=(result_queue,))
    proc.start()
    proc.join(timeout=10)

    assert not proc.is_alive(), "Subprocess timed out"
    assert not result_queue.empty(), "No result from subprocess"

    result = result_queue.get()
    assert result["error"] is None, f"Subprocess raised exception: {result['error']}"
    assert result["success"]
