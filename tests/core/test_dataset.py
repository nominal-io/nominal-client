from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.exceptions import NominalIngestError
from nominal.core.log import LogPoint
from nominal.core.unit import Unit
from nominal.core.video_dataset_file import VideoDatasetFile

UNITS = [
    Unit(name="coulomb", symbol="C"),
    Unit(name="kilograms", symbol="kg"),
    Unit(name="mole", symbol="mol"),
]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.logical_series = MagicMock()
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    ds = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=mock_clients,
    )

    spy: MagicMock = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    spy.return_value = ds

    return ds


def test_write_logs_more_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=2)

    assert len(endpoint.call_args_list) == 2

    _auth, _rid, first_req = endpoint.call_args_list[0][0]
    assert len(first_req.logs) == 2

    _auth, _rid, second_req = endpoint.call_args_list[1][0]
    assert len(second_req.logs) == 1


def test_write_logs_less_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=1000)

    assert len(endpoint.call_args_list) == 1
    _auth, _rid, req = endpoint.call_args_list[0][0]
    assert len(req.logs) == 3


def test_list_files_specializes_video_rows():
    ds = MagicMock()
    video_row = MagicMock(name="video_row")
    plain_row = MagicMock(name="plain_row")
    # `ds` is a bare MagicMock (not a real Dataset instance), so patch the return value directly
    # on its auto-vivified `_list_files` attribute rather than on the class -- patching
    # `Dataset._list_files` has no effect here since `ds._list_files` never resolves to it.
    ds._list_files.return_value = [video_row, plain_row]
    with patch(
        "nominal.core.dataset._dataset_file_from_conjure",
        side_effect=lambda clients, row: "VIDEO" if row is video_row else "PLAIN",
    ):
        result = list(Dataset.list_files(ds, successful_only=False))
    assert result == ["VIDEO", "PLAIN"]


def _video_response(dataset_file_id, ingest_job_rid="job-1"):
    response = MagicMock()
    response.details.dataset.dataset_rid = "ds-1"
    response.details.dataset.dataset_file_id = dataset_file_id
    response.ingest_job_rid = ingest_job_rid
    return response


def test_handle_video_response_prefers_direct_dataset_file_id():
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    with patch("nominal.core.dataset._dataset_file_from_conjure", return_value=video_file):
        result = Dataset._handle_video_ingest_response(ds, _video_response("file-1"))
    assert result is video_file
    ds._clients.catalog.get_dataset_file.assert_called_once()


def test_handle_video_response_direct_id_non_video_raises():
    ds = MagicMock()
    with (
        patch("nominal.core.dataset._dataset_file_from_conjure", return_value=MagicMock()),  # not a VideoDatasetFile
        pytest.raises(NominalIngestError, match="not a video dataset file"),
    ):
        Dataset._handle_video_ingest_response(ds, _video_response("file-1"))


def test_handle_video_response_falls_back_to_ingest_job_single_video_file():
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    job = MagicMock()
    # A non-video file (bare MagicMock) and the one video file; the handler must filter to the video.
    job.dataset_files.return_value = [MagicMock(), video_file]
    with patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job):
        result = Dataset._handle_video_ingest_response(ds, _video_response(None))
    assert result is video_file


def test_handle_video_response_fallback_zero_or_multiple_raises():
    ds = MagicMock()
    job = MagicMock()
    job.dataset_files.return_value = []
    with (
        patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job),
        pytest.raises(NominalIngestError, match="exactly one video file"),
    ):
        Dataset._handle_video_ingest_response(ds, _video_response(None))


def test_handle_video_response_no_id_and_no_job_raises():
    ds = MagicMock()
    with pytest.raises(NominalIngestError, match="neither a dataset-file id nor an ingest job"):
        Dataset._handle_video_ingest_response(ds, _video_response(None, ingest_job_rid=None))
