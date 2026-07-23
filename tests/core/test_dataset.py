from __future__ import annotations

import io
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


def test_add_video_from_io_requires_a_timestamp_mode():
    ds = MagicMock()
    with pytest.raises(ValueError, match="Either 'start' or 'frame_timestamps'"):
        Dataset.add_video_from_io(ds, io.BytesIO(b""), "v.mp4", channel="c")


def test_add_video_from_io_rejects_both_timestamp_modes():
    ds = MagicMock()
    with pytest.raises(ValueError, match="Only one of 'start' or 'frame_timestamps'"):
        Dataset.add_video_from_io(ds, io.BytesIO(b""), "v.mp4", channel="c", start=0, frame_timestamps=[1])


def test_add_video_from_io_rejects_text_stream():
    ds = MagicMock()
    with pytest.raises(TypeError, match="binary mode"):
        Dataset.add_video_from_io(ds, io.StringIO("x"), "v.mp4", channel="c", start=0)


def test_add_video_from_io_submits_video_v2_and_returns_handler_result():
    ds = MagicMock()
    ds.rid = "ds-rid"
    with (
        patch("nominal.core.dataset.build_video_timestamp_manifest", return_value="MANIFEST") as build_manifest,
        patch("nominal.core.dataset.build_video_ingest_options", return_value="OPTIONS") as build_opts,
        patch("nominal.core.dataset.upload_multipart_io", return_value="s3://p"),
    ):
        result = Dataset.add_video_from_io(
            ds, io.BytesIO(b"data"), "front.mp4", channel="camera/front", start=123, tags={"v": "a"}
        )

    build_manifest.assert_called_once()
    build_opts.assert_called_once_with("ds-rid", "camera/front", {"v": "a"}, "s3://p", "MANIFEST", False)
    ds._clients.ingest.ingest.assert_called_once()
    ds._handle_video_ingest_response.assert_called_once_with(ds._clients.ingest.ingest.return_value)
    assert result is ds._handle_video_ingest_response.return_value


def test_add_mcap_video_from_io_rejects_text_stream():
    ds = MagicMock()
    with pytest.raises(TypeError, match="binary mode"):
        Dataset.add_mcap_video_from_io(ds, io.StringIO("x"), "v.mcap", channel="c", topic="/t")


def test_add_mcap_video_from_io_builds_mcap_manifest_and_submits():
    ds = MagicMock()
    ds.rid = "ds-rid"
    with (
        patch("nominal.core.dataset.build_video_timestamp_manifest", return_value="MANIFEST") as build_manifest,
        patch("nominal.core.dataset.build_video_ingest_options", return_value="OPTIONS") as build_opts,
        patch("nominal.core.dataset.upload_multipart_io", return_value="s3://p"),
    ):
        result = Dataset.add_mcap_video_from_io(
            ds, io.BytesIO(b"data"), "rec.mcap", channel="camera/front", topic="/camera/front/h264"
        )

    _, kwargs = build_manifest.call_args
    assert kwargs["mcap_topic"] == "/camera/front/h264"
    build_opts.assert_called_once_with("ds-rid", "camera/front", None, "s3://p", "MANIFEST", False)
    ds._clients.ingest.ingest.assert_called_once()
    assert result is ds._handle_video_ingest_response.return_value
