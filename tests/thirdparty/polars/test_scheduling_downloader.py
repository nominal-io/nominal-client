from __future__ import annotations

from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from nominal.thirdparty.polars.scheduling_downloader import DownloadTicket, SchedulingDownloader


@pytest.fixture
def mock_downloader():
    """A mock MultipartFileDownloader."""
    dl = MagicMock()
    return dl


@pytest.fixture
def tmp_output(tmp_path):
    """Output directory for downloads."""
    out = tmp_path / "downloads"
    out.mkdir()
    return out


@pytest.fixture
def scheduler(tmp_output, mock_downloader):
    """A SchedulingDownloader with a mocked MultipartFileDownloader."""
    return SchedulingDownloader(output_dir=tmp_output, downloader=mock_downloader)


def _make_done_future(result=None):
    """Create a Future that is already resolved."""
    f = Future()
    f.set_result(result)
    return f


def _make_failed_future(exception):
    """Create a Future that has already failed."""
    f = Future()
    f.set_exception(exception)
    return f


def test_submit_returns_ticket_immediately(scheduler, mock_downloader):
    """submit() returns a DownloadTicket without blocking on download completion."""
    # Make submit_download return pending futures
    pending = Future()
    mock_downloader.submit_download.return_value = [pending]

    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=1024, filename="test.csv.gz")

    assert isinstance(ticket, DownloadTicket)
    assert not ticket.done
    assert ticket.file_size_bytes == 1024


def test_ticket_done_when_all_parts_complete(scheduler, mock_downloader):
    """A ticket is done when all its part futures have resolved."""
    mock_downloader.submit_download.return_value = [_make_done_future(), _make_done_future()]

    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=2048, filename="test.csv.gz")

    assert ticket.done


def test_ticket_result_returns_destination(scheduler, mock_downloader, tmp_output):
    """ticket.result() returns the destination path after all parts complete."""
    mock_downloader.submit_download.return_value = [_make_done_future()]

    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=1024, filename="out.csv.gz")
    path = ticket.result()

    assert path == tmp_output / "out.csv.gz"


def test_ticket_result_raises_on_part_failure(scheduler, mock_downloader):
    """ticket.result() raises the exception from the first failed part."""
    mock_downloader.submit_download.return_value = [
        _make_done_future(),
        _make_failed_future(RuntimeError("download failed")),
    ]

    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=2048, filename="test.csv.gz")

    with pytest.raises(RuntimeError, match="download failed"):
        ticket.result()


def test_cleanup_deletes_file(scheduler, mock_downloader, tmp_output):
    """cleanup() removes the downloaded file from disk."""
    mock_downloader.submit_download.return_value = [_make_done_future()]

    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=1024, filename="test.csv.gz")
    # Create the file so cleanup has something to delete
    ticket.destination.touch()
    assert ticket.destination.exists()

    scheduler.cleanup(ticket)
    assert not ticket.destination.exists()


def test_cleanup_is_idempotent(scheduler, mock_downloader):
    """cleanup() does not raise if the file doesn't exist."""
    mock_downloader.submit_download.return_value = [_make_done_future()]
    ticket = scheduler.submit("https://s3/file.csv.gz", file_size_bytes=1024, filename="test.csv.gz")

    # File was never actually created on disk
    scheduler.cleanup(ticket)  # should not raise


def test_multiple_submits_share_pool(scheduler, mock_downloader):
    """Multiple submit() calls all delegate to the same underlying downloader."""
    mock_downloader.submit_download.return_value = [_make_done_future()]

    scheduler.submit("https://s3/a.csv.gz", file_size_bytes=100, filename="a.csv.gz")
    scheduler.submit("https://s3/b.csv.gz", file_size_bytes=200, filename="b.csv.gz")
    scheduler.submit("https://s3/c.csv.gz", file_size_bytes=300, filename="c.csv.gz")

    assert mock_downloader.submit_download.call_count == 3


def test_context_manager_calls_close(tmp_output, mock_downloader):
    """Exiting the context manager shuts down the underlying downloader."""
    with SchedulingDownloader(output_dir=tmp_output, downloader=mock_downloader):
        pass

    mock_downloader.close.assert_called_once()
