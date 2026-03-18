from __future__ import annotations

from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.dataset_file import (
    DatasetFile,
    IngestStatus,
    IngestWaitType,
    _batch_refresh_files,
    as_files_ingested,
    filename_from_uri,
    wait_for_files_to_ingest,
)
from nominal.core.exceptions import NominalIngestError


def _make_file(file_id: str, statuses: list[IngestStatus]) -> DatasetFile:
    """Create a mock DatasetFile whose ingest_status advances through statuses on each poll."""
    mock = MagicMock()
    mock.id = file_id
    mock.ingest_status = statuses[0]

    status_iter = iter(statuses)

    def refresh(_: object) -> DatasetFile:
        mock.ingest_status = next(status_iter, statuses[-1])
        return cast(DatasetFile, mock)

    mock._refresh_from_api.side_effect = refresh
    mock._build_ingest_exception.return_value = NominalIngestError(f"{file_id} failed")
    return cast(DatasetFile, mock)

def _make_batch_file(file_id: str, dataset_rid: str, status: IngestStatus, clients: MagicMock) -> DatasetFile:
    """Create a mock DatasetFile for _batch_refresh_files tests with a shared API clients mock."""
    mock = MagicMock()
    mock.id = file_id
    mock.dataset_rid = dataset_rid
    mock.ingest_status = status
    mock._clients = clients
    return cast(DatasetFile, mock)


def test_filename_from_uri_decodes_path_and_replaces_colons():
    """URI with encoded spaces and a colon in the filename decodes and sanitizes correctly."""
    assert filename_from_uri("s3://bucket/a%20b/video:01.mp4") == "video_01.mp4"


def test_poll_until_ingestion_completed_sleeps_until_success():
    """Polls at the given interval until the file transitions from IN_PROGRESS to SUCCESS."""
    file = _make_file("file-1", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        result = DatasetFile.poll_until_ingestion_completed(file, interval=timedelta(seconds=2))

    assert result.id == file.id
    assert result.ingest_status is IngestStatus.SUCCESS
    mock_sleep.assert_called_once_with(2.0)


def test_poll_until_ingestion_completed_raises_ingest_error_on_failure():
    """Raises NominalIngestError immediately when the file status is FAILED."""
    file = _make_file("file-1", [IngestStatus.FAILED])

    with (
        patch("nominal.core.dataset_file.time.sleep") as mock_sleep,
        pytest.raises(NominalIngestError, match="file-1 failed"),
    ):
        DatasetFile.poll_until_ingestion_completed(file, interval=timedelta(seconds=2))

    mock_sleep.assert_not_called()


def test_wait_for_files_to_ingest_sleeps_between_polls_without_timeout():
    """Polls once and sleeps once when a single file transitions from IN_PROGRESS to SUCCESS."""
    file = _make_file("file-1", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest([file], poll_interval=timedelta(seconds=3))

    assert done == [file]
    assert not not_done
    mock_sleep.assert_called_once_with(3.0)


def test_wait_for_files_to_ingest_treats_deleted_statuses_as_done():
    """Files with DELETED status are immediately placed in the done list without sleeping."""
    file = _make_file("file-1", [IngestStatus.DELETED])

    done, not_done = wait_for_files_to_ingest([file])

    assert done == [file]
    assert not not_done


def test_wait_for_files_to_ingest_puts_failed_file_in_done_with_all_completed():
    """In default ALL_COMPLETED mode, a failed file is moved to done rather than raising."""
    file = _make_file("file-1", [IngestStatus.FAILED])

    done, not_done = wait_for_files_to_ingest([file])

    assert done == [file]
    assert not not_done


def test_wait_for_files_to_ingest_returns_after_first_completed():
    """With FIRST_COMPLETED mode, returns as soon as any file finishes without waiting for others."""
    done_file = _make_file("done-file", [IngestStatus.SUCCESS])
    pending_file = _make_file("pending-file", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest(
            [done_file, pending_file],
            return_when=IngestWaitType.FIRST_COMPLETED,
        )

    assert done == [done_file]
    assert not_done == [pending_file]
    mock_sleep.assert_not_called()


def test_wait_for_files_to_ingest_returns_after_first_exception():
    """With FIRST_EXCEPTION mode, returns as soon as one file fails without waiting for others."""
    failed = _make_file("failed-file", [IngestStatus.FAILED])
    pending = _make_file("pending-file", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest(
            [failed, pending],
            poll_interval=timedelta(seconds=5),
            return_when=IngestWaitType.FIRST_EXCEPTION,
        )

    assert done == [failed]
    assert not_done == [pending]
    mock_sleep.assert_not_called()


def test_wait_for_files_to_ingest_returns_not_done_when_timeout_expires():
    """Files still IN_PROGRESS when the timeout expires are returned as not_done without sleeping."""
    file = _make_file("file-1", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest([file], timeout=timedelta(0))

    assert not done
    assert not_done == [file]
    mock_sleep.assert_not_called()



def test_batch_refresh_reports_files_missing_from_response():
    """Files omitted from the batch API response are reported as absent by their IDs."""
    clients = MagicMock()
    clients.catalog.batch_get_dataset_files.return_value = {"present": MagicMock()}

    present = _make_batch_file("present", "ds-1", IngestStatus.IN_PROGRESS, clients)
    absent = _make_batch_file("absent", "ds-1", IngestStatus.IN_PROGRESS, clients)

    result = _batch_refresh_files([present, absent])

    assert result == {"absent"}


def test_batch_refresh_reports_no_missing_files_when_all_present():
    """When every requested file appears in the batch response, no absent IDs are reported."""
    clients = MagicMock()
    clients.catalog.batch_get_dataset_files.return_value = {
        "file-1": MagicMock(),
        "file-2": MagicMock(),
    }

    file1 = _make_batch_file("file-1", "ds-1", IngestStatus.IN_PROGRESS, clients)
    file2 = _make_batch_file("file-2", "ds-1", IngestStatus.IN_PROGRESS, clients)

    result = _batch_refresh_files([file1, file2])

    assert result == set()


def test_wait_for_files_to_ingest_treats_absent_file_as_failed():
    """A file absent from the batch response is moved to done without blocking, like a failed ingest."""
    file = _make_file("absent-1", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file._batch_refresh_files", return_value={"absent-1"}):
        done, not_done = wait_for_files_to_ingest([file])

    assert done == [file]
    assert not not_done


def test_wait_for_files_to_ingest_returns_after_absent_file_with_first_exception():
    """With FIRST_EXCEPTION, an absent file triggers early return with remaining files as not done."""
    absent = _make_file("absent-1", [IngestStatus.IN_PROGRESS])
    pending = _make_file("pending-1", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file._batch_refresh_files", return_value={"absent-1"}):
        done, not_done = wait_for_files_to_ingest(
            [absent, pending],
            return_when=IngestWaitType.FIRST_EXCEPTION,
        )

    assert done == [absent]
    assert not_done == [pending]


def test_as_files_ingested_does_not_sleep_when_all_files_complete_in_first_poll():
    """Does not sleep when all files complete ingestion on the first poll."""
    first = _make_file("first-file", [IngestStatus.SUCCESS])
    second = _make_file("second-file", [IngestStatus.SUCCESS])

    with (
        patch(
            "nominal.core.dataset_file.wait_for_files_to_ingest",
            return_value=([first, second], []),
        ),
        patch("nominal.core.dataset_file.time.sleep") as mock_sleep,
    ):
        yielded = list(as_files_ingested([first, second], poll_interval=timedelta(seconds=1)))

    assert yielded == [first, second]
    mock_sleep.assert_not_called()


def test_as_files_ingested_yields_files_in_completion_batches():
    """Yields files as they complete ingestion, sleeping between polls when files remain."""
    first = _make_file("first-file", [IngestStatus.SUCCESS])
    second = _make_file("second-file", [IngestStatus.SUCCESS])

    with (
        patch(
            "nominal.core.dataset_file.wait_for_files_to_ingest",
            side_effect=[
                ([first], [second]),
                ([second], []),
            ],
        ) as mock_wait,
        patch("nominal.core.dataset_file.time.sleep") as mock_sleep,
    ):
        yielded = list(as_files_ingested([first, second], poll_interval=timedelta(seconds=1)))

    assert yielded == [first, second]
    assert mock_wait.call_count == 2
    mock_sleep.assert_called_once_with(1.0)
