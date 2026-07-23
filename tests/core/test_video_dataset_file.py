from __future__ import annotations

from unittest.mock import MagicMock, patch

from nominal.core.dataset_file import DatasetFile, IngestStatus
from nominal.core.video_dataset_file import VideoDatasetFile


def _common_kwargs(clients: MagicMock) -> dict:
    return dict(
        id="file-1",
        dataset_rid="ds-1",
        name="front.mp4",
        bounds=None,
        uploaded_at=0,
        ingested_at=None,
        deleted_at=None,
        ingest_status=IngestStatus.INGESTING,
        timestamp_channel=None,
        timestamp_type=None,
        file_tags=None,
        tag_columns=None,
        _clients=clients,
        _ingest_error_message=None,
    )


def _video_row(segment: object | None) -> MagicMock:
    row = MagicMock()
    row.metadata.video.timestamp_manifest = MagicMock(name="manifest")
    row.metadata.video.segment_metadata = segment
    return row


def test_from_conjure_populates_aggregates_from_segment_metadata():
    clients = MagicMock()
    segment = MagicMock(
        num_frames=100, num_segments=3, scale_factor=2.0, media_duration_seconds=10.0, media_frame_rate=30.0
    )
    row = _video_row(segment)
    with patch("nominal.core.video_dataset_file._parse_common_file_fields", return_value=_common_kwargs(clients)):
        file = VideoDatasetFile._from_conjure(clients, row)

    assert isinstance(file, DatasetFile)
    assert (file.num_frames, file.num_segments, file.scale_factor) == (100, 3, 2.0)
    assert (file.media_duration_seconds, file.media_frame_rate) == (10.0, 30.0)
    assert file._timestamp_manifest is row.metadata.video.timestamp_manifest


def test_from_conjure_leaves_aggregates_none_without_segment_metadata():
    clients = MagicMock()
    row = _video_row(segment=None)
    with patch("nominal.core.video_dataset_file._parse_common_file_fields", return_value=_common_kwargs(clients)):
        file = VideoDatasetFile._from_conjure(clients, row)

    assert file.num_frames is None
    assert file.num_segments is None
    assert file.media_duration_seconds is None
    assert file.media_frame_rate is None
    assert file.scale_factor is None


def test_timestamp_manifest_excluded_from_repr_and_equality():
    clients = MagicMock()
    shared = dict(
        **_common_kwargs(clients),
        num_frames=1,
        num_segments=1,
        media_duration_seconds=1.0,
        media_frame_rate=1.0,
        scale_factor=1.0,
    )
    a = VideoDatasetFile(**shared, _timestamp_manifest=MagicMock(name="m1"))
    b = VideoDatasetFile(**shared, _timestamp_manifest=MagicMock(name="m2"))

    assert a == b  # differ only by the excluded manifest
    assert "timestamp_manifest" not in repr(a)
