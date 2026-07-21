from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import api, ingest_api, scout_catalog

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.dataset_file import DatasetFile


def _dataset(clients: MagicMock) -> Dataset:
    return Dataset(
        rid="ri.dataset.1",
        name="d",
        description=None,
        properties={},
        labels=(),
        bounds=DatasetBounds(start=0, end=1),
        _clients=clients,
    )


def _dataset_file(dataset_rid: str, file_id: str) -> scout_catalog.DatasetFile:
    return scout_catalog.DatasetFile(
        id=file_id,
        dataset_rid=dataset_rid,
        name="video.mp4",
        handle=scout_catalog.Handle(s3=scout_catalog.S3Handle(bucket="b", key="k")),
        uploaded_at="2026-01-01T00:00:00Z",
        ingest_status=api.IngestStatusV2(success=api.SuccessResult()),
    )


def test_add_video_from_io_maps_request_and_returns_dataset_file() -> None:
    """The video is uploaded, the request targets this dataset/channel, and the response maps to a DatasetFile."""
    clients = MagicMock()
    dataset = _dataset(clients)
    clients.ingest.ingest.return_value = ingest_api.IngestResponse(
        details=ingest_api.IngestDetails(
            dataset=ingest_api.IngestDatasetFileDetails(dataset_rid="ri.dataset.1", dataset_file_id="ri.file.1")
        )
    )
    clients.catalog.get_dataset_file.return_value = _dataset_file("ri.dataset.1", "ri.file.1")

    with patch("nominal.core.dataset.upload_multipart_io", return_value="s3://bucket/video.mp4") as mock_upload:
        result = dataset.add_video_from_io(
            BytesIO(b"not-real-video-bytes"),
            "front_camera",
            start=0,
            tags={"site": "hq"},
            overwrite_overlapping=True,
        )

    assert isinstance(result, DatasetFile)
    assert result.id == "ri.file.1"
    mock_upload.assert_called_once()

    request = clients.ingest.ingest.call_args[0][1]
    options = request.options.video_v2
    assert options.channel == "front_camera"
    assert options.tags == {"site": "hq"}
    assert options.over_write_segments is True
    assert options.target.existing.dataset_rid == "ri.dataset.1"
    assert options.source.s3.path == "s3://bucket/video.mp4"
    assert options.timestamp_manifest.no_manifest is not None


def test_add_video_from_io_requires_start_or_frame_timestamps() -> None:
    clients = MagicMock()
    dataset = _dataset(clients)

    with pytest.raises(ValueError, match="Either 'start' or 'frame_timestamps' must be provided"):
        dataset.add_video_from_io(BytesIO(b"data"), "front_camera")

    clients.ingest.ingest.assert_not_called()


def test_add_video_from_io_rejects_both_start_and_frame_timestamps() -> None:
    clients = MagicMock()
    dataset = _dataset(clients)

    with pytest.raises(ValueError, match="Only one of 'start' or 'frame_timestamps' may be provided"):
        dataset.add_video_from_io(BytesIO(b"data"), "front_camera", start=0, frame_timestamps=[0, 1])

    clients.ingest.ingest.assert_not_called()
