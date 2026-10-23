from __future__ import annotations

import io
import logging
import weakref
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import timedelta
from logging.handlers import MemoryHandler
from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._stream.write_stream import BatchItem, WriteStream
from nominal.core.dataset import Dataset
from nominal.experimental.logging import NominalLogHandler


@pytest.mark.parametrize("batch_size", [1, 10], ids=["already-submitted", "buffered"])
@pytest.mark.parametrize("completion", ["flush", "close"])
def test_blocking_completion_raises_upload_failure(batch_size: int, completion: str):
    """Blocking completion reports failures from earlier uploads and the final buffered batch."""
    failure = RuntimeError("upload rejected")

    def fail_upload(batch: Sequence[BatchItem[float]]) -> None:
        """Reject a synthetic upload at the processor boundary."""
        raise failure

    stream = WriteStream.create(batch_size, timedelta(hours=1), fail_upload)
    try:
        stream.enqueue("temperature", 0, 1.0)
        with pytest.raises(RuntimeError, match="upload rejected") as raised:
            if completion == "flush":
                stream.flush(wait=True)
            else:
                stream.close()
        assert raised.value is failure
    finally:
        stream.close()


def test_nonblocking_flush_retains_failure_for_blocking_wait():
    """An asynchronous flush leaves upload errors observable at the next blocking completion."""
    processor = MagicMock(side_effect=ValueError("invalid point"))
    stream = WriteStream.create(10, timedelta(hours=1), processor)
    try:
        stream.enqueue("temperature", 0, 1.0)
        stream.flush()
        with pytest.raises(ValueError, match="invalid point"):
            stream.flush(wait=True)
        stream.flush(wait=True)
    finally:
        stream.close()


def test_flush_waits_for_previously_submitted_batch(caplog: pytest.LogCaptureFixture):
    """A timeout still reports pending uploads when there is no current buffered batch."""
    started = Event()
    release = Event()
    uploaded = []

    def process_batch(batch: Sequence[BatchItem[float]]) -> None:
        """Hold the real worker until the test releases its upload."""
        started.set()
        assert release.wait(5)
        uploaded.extend(batch)

    stream = WriteStream.create(1, timedelta(hours=1), process_batch)
    try:
        stream.enqueue("temperature", 0, 1.0)
        assert started.wait(5)
        stream.flush(wait=True, timeout=0)
        assert "Upload task still pending" in caplog.text
        release.set()
        stream.flush(wait=True)
        assert [point.value for point in uploaded] == [1.0]
    finally:
        release.set()
        stream.close()


def test_timeout_upload_failure_is_observable():
    """An upload submitted by the timeout worker is reported by a later blocking flush."""
    attempted = Event()

    def fail_upload(batch: Sequence[BatchItem[float]]) -> None:
        """Signal that the timeout worker submitted the failing batch."""
        attempted.set()
        raise RuntimeError("timeout upload rejected")

    stream = WriteStream.create(10, timedelta(milliseconds=1), fail_upload)
    try:
        stream.enqueue("temperature", 0, 1.0)
        assert attempted.wait(5)
        with pytest.raises(RuntimeError, match="timeout upload rejected"):
            stream.flush(wait=True)
    finally:
        stream.close()


def test_failed_close_still_shuts_down_workers():
    """A failed close releases the executor before propagating the upload exception."""
    stream = WriteStream.create(10, timedelta(hours=1), MagicMock(side_effect=RuntimeError("upload rejected")))
    stream.enqueue("temperature", 0, 1.0)
    with pytest.raises(RuntimeError, match="upload rejected"):
        stream.close()
    with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
        stream._executor.submit(lambda: None)
    stream.close()


def test_context_exit_raises_upload_failure():
    """A successful context body cannot conceal an upload failure on exit."""
    stream = WriteStream.create(10, timedelta(hours=1), MagicMock(side_effect=RuntimeError("upload rejected")))
    try:
        with pytest.raises(RuntimeError, match="upload rejected"):
            with stream:
                stream.enqueue("temperature", 0, 1.0)
    finally:
        stream.close()


@pytest.mark.parametrize("upload_error", [RuntimeError("upload rejected"), SystemExit(2), KeyboardInterrupt()])
def test_context_exit_preserves_body_exception(upload_error: BaseException):
    """Closing after an upload failure does not replace an exception from the context body."""
    original = ValueError("application failed")
    stream = WriteStream.create(10, timedelta(hours=1), MagicMock(side_effect=upload_error))
    with pytest.raises(ValueError, match="application failed") as raised:
        with stream:
            stream.enqueue("temperature", 0, 1.0)
            raise original
    assert raised.value is original
    stream.close()


@pytest.fixture
def dataset(mock_clients: MagicMock) -> Dataset:
    """Build a real dataset wrapper with inert service boundaries."""
    return Dataset(
        rid="ri.catalog.test.dataset.example",
        _clients=mock_clients,
        name="test dataset",
        description=None,
        properties={},
        labels=[],
        bounds=None,
        is_archived=False,
    )


def test_python_dataset_stream_propagates_upload_rejection(dataset: Dataset, mock_clients: MagicMock):
    """The Python dataset writer reports a service rejection when it is closed."""
    mock_clients.proto_write.write_nominal_batches.side_effect = RuntimeError("write rejected")
    stream = dataset.get_write_stream(implementation="python")
    try:
        stream.enqueue("temperature", 0, 1.0)
        with pytest.raises(RuntimeError, match="write rejected"):
            stream.close()
    finally:
        stream.close()


def test_log_handler_close_propagates_upload_rejection(dataset: Dataset, mock_clients: MagicMock):
    """Explicit log-handler close reports a rejected log upload and closes the handler."""
    mock_clients.storage_writer.write_logs.side_effect = RuntimeError("log upload rejected")
    handler = NominalLogHandler(dataset)
    try:
        handler.emit(logging.LogRecord("test", logging.INFO, "example.py", 1, "test message", (), None))
        with pytest.raises(RuntimeError, match="log upload rejected"):
            handler.close(raise_on_error=True)
        assert handler._closed
    finally:
        with suppress(Exception):
            handler.close()


def test_interrupted_submission_does_not_leave_pending_uploads():
    """Interrupting backpressure does not strand a later blocking flush."""
    stream = WriteStream.create(1, timedelta(hours=1), MagicMock())
    try:
        with patch.object(stream._pending_jobs, "acquire", side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                stream.enqueue("temperature", 0, 1.0)
        assert stream._uploads.pending == 0
        stream.flush(wait=True)
    finally:
        stream.close()


def test_nonblocking_close_leaves_running_failure_observable():
    """A nonblocking close does not wait, while a later blocking close reports running-task failure."""
    started = Event()
    release = Event()

    def fail_upload(batch: Sequence[BatchItem[float]]) -> None:
        """Keep an upload running beyond nonblocking shutdown."""
        started.set()
        assert release.wait(5)
        raise RuntimeError("late upload failure")

    stream = WriteStream.create(1, timedelta(hours=1), fail_upload)
    try:
        stream.enqueue("temperature", 0, 1.0)
        assert started.wait(5)
        stream.close(wait=False)
        release.set()
        with pytest.raises(RuntimeError, match="late upload failure"):
            stream.close()
    finally:
        release.set()
        stream.close()


@pytest.mark.parametrize("upload_error", [RuntimeError("log upload rejected"), SystemExit(2), KeyboardInterrupt()])
def test_log_handler_exit_preserves_body_exception(
    dataset: Dataset, mock_clients: MagicMock, upload_error: BaseException
):
    """Log-handler cleanup reports failures without replacing an application exception."""
    mock_clients.storage_writer.write_logs.side_effect = upload_error
    handler = NominalLogHandler(dataset)
    original = ValueError("application failed")
    try:
        with pytest.raises(ValueError) as raised:
            try:
                handler.emit(logging.LogRecord("test", logging.INFO, "example.py", 1, "test message", (), None))
                raise original
            finally:
                handler.__exit__(ValueError, original, None)
        assert raised.value is original
        assert handler._closed
    finally:
        handler.close()


def test_nonblocking_close_cancels_queued_uploads():
    """Cancelling queued uploads releases their completion tracking without callback errors."""
    release = Event()
    processor = MagicMock()
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(release.wait, 5)
    with patch("nominal.core._stream.write_stream.concurrent.futures.ThreadPoolExecutor", return_value=executor):
        stream = WriteStream.create(1, timedelta(hours=1), processor)
    try:
        stream.enqueue("temperature", 0, 1.0)
        stream.close(wait=False)
        processor.assert_not_called()
        assert stream._uploads.pending == 0
    finally:
        release.set()
        stream.close()


def test_logging_shutdown_finishes_other_handlers_after_upload_failure(dataset: Dataset, mock_clients: MagicMock):
    """Default handler close does not interrupt logging.shutdown or lose earlier buffered output."""
    output = io.StringIO()
    buffered = MemoryHandler(10, target=logging.StreamHandler(output))
    mock_clients.storage_writer.write_logs.side_effect = RuntimeError("log upload rejected")
    handler = NominalLogHandler(dataset)
    record = logging.LogRecord("test", logging.INFO, "example.py", 1, "buffered output", (), None)
    try:
        diagnostics_output = io.StringIO()
        diagnostics = logging.Logger("nominal.core._stream.write_stream")
        diagnostics.addHandler(handler)
        diagnostics.addHandler(logging.StreamHandler(diagnostics_output))
        with patch("nominal.core._stream.write_stream.logger", diagnostics):
            buffered.emit(record)
            handler.emit(record)
            logging.shutdown([weakref.ref(buffered), weakref.ref(handler)])
        assert "Batched upload task failed" in diagnostics_output.getvalue()
        assert output.getvalue() == "buffered output\n"
        assert buffered._closed and handler._closed
    finally:
        handler.close()
        buffered.close()
