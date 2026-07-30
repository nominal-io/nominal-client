from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from nominal_api import api, datasource, scout_catalog, scout_video_api

from nominal.core.dataset_file import DatasetFile, IngestStatus, _dataset_file_from_conjure
from nominal.core.video_dataset_file import VideoDatasetFile

SEGMENTS = datasource.VideoSegmentsMetadata(
    media_duration_seconds=10.0, media_frame_rate=30.0, num_frames=100, num_segments=3, scale_factor=2.0
)


def _catalog_file_bean(
    *, video: bool = True, video_segments: datasource.VideoSegmentsMetadata | None = None
) -> scout_catalog.DatasetFile:
    """A minimal real catalog row: a video row when `video`, a plain row otherwise."""
    manifest = scout_video_api.VideoFileTimestampManifest(
        no_manifest=scout_video_api.NoTimestampManifest(starting_timestamp=api.Timestamp(seconds=1, nanos=0))
    )
    metadata = (
        scout_catalog.DatasetFileMetadata(
            video=datasource.VideoFileMetadata(timestamp_manifest=manifest, segment_metadata=video_segments)
        )
        if video
        else None
    )
    return scout_catalog.DatasetFile(
        dataset_rid="ds-1",
        handle=scout_catalog.Handle(s3=scout_catalog.S3Handle(bucket="bucket", key="key")),
        id="file-1",
        ingest_status=api.IngestStatusV2(success=api.SuccessResult()),
        name="front.mp4",
        uploaded_at="2026-07-30T00:00:00Z",
        metadata=metadata,
    )


@pytest.mark.parametrize(
    ("bean", "expected_type"),
    [
        (_catalog_file_bean(), VideoDatasetFile),
        (_catalog_file_bean(video=False), DatasetFile),
    ],
    ids=["video-arm", "no-metadata"],
)
def test_dispatch_specializes_only_video_rows(bean: scout_catalog.DatasetFile, expected_type: type):
    """Rows with a video metadata arm come back as VideoDatasetFile; everything else stays the base type.

    The third dispatch state — metadata present with no video arm — is forward-compat for future
    metadata kinds and cannot be built with today's bindings (video is the union's only arm, and
    unknown arms are rejected at construction and decode), so only the two real states are exercised.
    """
    result = _dataset_file_from_conjure(MagicMock(), bean)
    assert type(result) is expected_type


@pytest.mark.parametrize("segments", [None, SEGMENTS], ids=["absent", "present"])
def test_from_conjure_maps_segment_aggregates(segments: datasource.VideoSegmentsMetadata | None):
    """Aggregates mirror segment metadata when present and stay None before segmentation completes."""
    file = VideoDatasetFile._from_conjure(MagicMock(), _catalog_file_bean(video_segments=segments))
    expected = (None, None, None, None, None) if segments is None else (100, 3, 10.0, 30.0, 2.0)
    actual = (file.num_frames, file.num_segments, file.media_duration_seconds, file.media_frame_rate, file.scale_factor)
    assert actual == expected


def _video_file(clients: MagicMock, *, bounds: object = None) -> VideoDatasetFile:
    return VideoDatasetFile(
        id="file-1",
        dataset_rid="ds-1",
        name="front.mp4",
        bounds=bounds,
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
        _timestamp_manifest=MagicMock(name="manifest"),
    )


def _update_request(clients: MagicMock) -> object:
    """The single request passed to the batch-update endpoint."""
    args, _ = clients.video.batch_update_video_channel_dataset_files.call_args
    return args[1]


def test_update_addresses_the_files_dataset():
    """An update is addressed to the file's own dataset; no channel identity is sent."""
    clients = MagicMock()
    file = _video_file(clients)
    with patch.object(VideoDatasetFile, "refresh", return_value=file) as refresh:
        file.update(starting_timestamp=1_700_000_000_000_000_000)

    request = _update_request(clients)
    assert request.dataset_rid == "ds-1"
    assert [u.dataset_file_id for u in request.updates] == ["file-1"]
    assert request.updates[0].start is not None
    refresh.assert_called_once()


def test_update_rejects_multiple_scale_inputs():
    """Only one of the three scale inputs may be supplied, matching legacy VideoFile.update."""
    clients = MagicMock()
    file = _video_file(clients)
    with pytest.raises(ValueError, match="at most one of"):
        file.update(true_frame_rate=30.0, scale_factor=2.0)
    clients.video.batch_update_video_channel_dataset_files.assert_not_called()


def test_update_requires_at_least_one_field():
    """An update with nothing set is rejected rather than sent as a no-op."""
    clients = MagicMock()
    file = _video_file(clients)
    with pytest.raises(ValueError, match="At least one of"):
        file.update()
    clients.video.batch_update_video_channel_dataset_files.assert_not_called()
