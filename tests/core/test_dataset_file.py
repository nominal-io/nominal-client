from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import pytest

from nominal.core.dataset_file import (
    DatasetFile,
    IngestStatus,
    IngestWaitType,
    as_files_ingested,
    filename_from_uri,
    wait_for_files_to_ingest,
)
from nominal.core.exceptions import NominalIngestError


def _make_ingest_api_status(
    status: IngestStatus, *, message: str = "ingest failed", error_type: str = "MockError"
) -> SimpleNamespace:
    error = None
    if status is IngestStatus.FAILED:
        error = SimpleNamespace(message=message, error_type=error_type)

    return SimpleNamespace(error=error)


@dataclass
class _FakeLatestApi:
    next_status: IngestStatus
    ingest_status: SimpleNamespace


class _FakeDatasetFile:
    _build_ingest_exception = DatasetFile._build_ingest_exception

    def __init__(self, file_id: str, statuses: list[IngestStatus]):
        self.id = file_id
        self.dataset_rid = "dataset-rid"
        self.name = f"{file_id}.csv"
        self.ingest_status = statuses[0]
        self._statuses = statuses
        self._index = 0

    def _get_latest_api(self) -> _FakeLatestApi:
        status = self._statuses[min(self._index, len(self._statuses) - 1)]
        self._index += 1
        return _FakeLatestApi(
            next_status=status,
            ingest_status=_make_ingest_api_status(status, message=f"{self.id} failed"),
        )

    def _refresh_from_api(self, api_file: _FakeLatestApi) -> _FakeDatasetFile:
        self.ingest_status = api_file.next_status
        return self


def _as_dataset_file(file: _FakeDatasetFile) -> DatasetFile:
    return cast(DatasetFile, file)


def test_filename_from_uri_decodes_path_and_replaces_colons():
    assert filename_from_uri("s3://bucket/a%20b/video:01.mp4") == "video_01.mp4"


def test_poll_until_ingestion_completed_sleeps_until_success():
    file = _FakeDatasetFile("file-1", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        result = DatasetFile.poll_until_ingestion_completed(_as_dataset_file(file), interval=timedelta(seconds=2))

    assert result.id == file.id
    assert result.ingest_status is IngestStatus.SUCCESS
    mock_sleep.assert_called_once_with(2.0)


def test_poll_until_ingestion_completed_raises_ingest_error_on_failure():
    file = _FakeDatasetFile("file-1", [IngestStatus.FAILED])

    with (
        patch("nominal.core.dataset_file.time.sleep") as mock_sleep,
        pytest.raises(NominalIngestError, match="file-1 failed"),
    ):
        DatasetFile.poll_until_ingestion_completed(_as_dataset_file(file), interval=timedelta(seconds=2))

    mock_sleep.assert_not_called()


def test_wait_for_files_to_ingest_sleeps_between_polls_without_timeout():
    file = _FakeDatasetFile("file-1", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest([_as_dataset_file(file)], poll_interval=timedelta(seconds=3))

    assert done == [_as_dataset_file(file)]
    assert not not_done
    mock_sleep.assert_called_once_with(3.0)


def test_wait_for_files_to_ingest_treats_deleted_statuses_as_done():
    file = _FakeDatasetFile("file-1", [IngestStatus.DELETED])

    done, not_done = wait_for_files_to_ingest([_as_dataset_file(file)])

    assert done == [_as_dataset_file(file)]
    assert not not_done


def test_wait_for_files_to_ingest_returns_after_first_exception():
    failed = _FakeDatasetFile("failed-file", [IngestStatus.FAILED])
    pending = _FakeDatasetFile("pending-file", [IngestStatus.IN_PROGRESS])

    with patch("nominal.core.dataset_file.time.sleep") as mock_sleep:
        done, not_done = wait_for_files_to_ingest(
            [_as_dataset_file(failed), _as_dataset_file(pending)],
            poll_interval=timedelta(seconds=5),
            return_when=IngestWaitType.FIRST_EXCEPTION,
        )

    assert done == [_as_dataset_file(failed)]
    assert not_done == [_as_dataset_file(pending)]
    mock_sleep.assert_not_called()


def test_as_files_ingested_yields_files_in_completion_batches():
    first = _FakeDatasetFile("first-file", [IngestStatus.SUCCESS])
    second = _FakeDatasetFile("second-file", [IngestStatus.SUCCESS])

    with (
        patch(
            "nominal.core.dataset_file.wait_for_files_to_ingest",
            side_effect=[
                ([_as_dataset_file(first)], [_as_dataset_file(second)]),
                ([_as_dataset_file(second)], []),
            ],
        ) as mock_wait,
        patch("nominal.core.dataset_file.time.sleep") as mock_sleep,
    ):
        yielded = list(
            as_files_ingested([_as_dataset_file(first), _as_dataset_file(second)], poll_interval=timedelta(seconds=1))
        )

    assert yielded == [_as_dataset_file(first), _as_dataset_file(second)]
    assert mock_wait.call_count == 2
    mock_sleep.assert_called_once_with(1.0)
