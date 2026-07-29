from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.dataset_file import DatasetFile, IngestStatus, _dataset_file_from_conjure
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
    base = DatasetFile(**_common_kwargs(clients))
    with patch.object(DatasetFile, "_from_conjure", return_value=base):
        file = VideoDatasetFile._from_conjure(clients, row)

    assert isinstance(file, DatasetFile)
    assert (file.num_frames, file.num_segments, file.scale_factor) == (100, 3, 2.0)
    assert (file.media_duration_seconds, file.media_frame_rate) == (10.0, 30.0)
    assert file._timestamp_manifest is row.metadata.video.timestamp_manifest


def test_from_conjure_leaves_aggregates_none_without_segment_metadata():
    clients = MagicMock()
    row = _video_row(segment=None)
    base = DatasetFile(**_common_kwargs(clients))
    with patch.object(DatasetFile, "_from_conjure", return_value=base):
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


def test_dispatch_returns_video_subtype_for_video_metadata():
    clients = MagicMock()
    row = MagicMock()
    row.metadata.video = MagicMock()  # video arm present
    with patch.object(VideoDatasetFile, "_from_conjure", return_value="video-file") as video_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "video-file"
    video_factory.assert_called_once_with(clients, row)


def test_dispatch_returns_base_type_when_no_video_metadata():
    clients = MagicMock()
    row = MagicMock()
    row.metadata = None  # no metadata at all
    with patch.object(DatasetFile, "_from_conjure", return_value="base-file") as base_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "base-file"
    base_factory.assert_called_once_with(clients, row)


def test_dispatch_returns_base_type_when_metadata_present_without_video_arm():
    clients = MagicMock()
    row = MagicMock()
    row.metadata.video = None  # metadata present, but not a video row
    with patch.object(DatasetFile, "_from_conjure", return_value="base-file") as base_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "base-file"
    base_factory.assert_called_once_with(clients, row)


def _video_file(clients: MagicMock, *, channel: str | None = None, bounds: object = None) -> VideoDatasetFile:
    kwargs = _common_kwargs(clients)
    kwargs["bounds"] = bounds
    return VideoDatasetFile(
        **kwargs,
        _timestamp_manifest=MagicMock(name="manifest"),
        channel=channel,
    )


def _update_request(clients: MagicMock) -> object:
    """The single request passed to the batch-update endpoint."""
    args, _ = clients.video.batch_update_video_channel_dataset_files.call_args
    return args[1]


def test_update_addresses_the_known_channel():
    """An update is addressed to the file's known channel on its dataset."""
    clients = MagicMock()
    file = _video_file(clients, channel="camera/front")
    with patch.object(VideoDatasetFile, "refresh", return_value=file) as refresh:
        file.update(starting_timestamp=1_700_000_000_000_000_000)

    request = _update_request(clients)
    assert request.channel_series.data_source.channel == "camera/front"
    assert request.channel_series.data_source.data_source_rid == "ds-1"
    assert [u.dataset_file_id for u in request.updates] == ["file-1"]
    assert request.updates[0].start is not None
    refresh.assert_called_once()


def test_update_without_known_channel_raises():
    """A file read back without a channel cannot be updated until the backend records the channel."""
    clients = MagicMock()
    file = _video_file(clients, channel=None)
    with pytest.raises(ValueError, match="channel is not known"):
        file.update(name="renamed.mp4")
    clients.video.batch_update_video_channel_dataset_files.assert_not_called()


def test_update_maps_each_scale_input_to_its_union_arm():
    """ending_timestamp, true_frame_rate, and scale_factor each set their own ScaleParameter arm."""
    for kwargs, arm in (
        ({"ending_timestamp": 1_700_000_000_000_000_000}, "ending_timestamp"),
        ({"true_frame_rate": 29.97}, "true_frame_rate"),
        ({"scale_factor": 2.0}, "scale_factor"),
    ):
        clients = MagicMock()
        file = _video_file(clients, channel="cam")
        with patch.object(VideoDatasetFile, "refresh", return_value=file):
            file.update(**kwargs)
        scale = _update_request(clients).updates[0].scale_parameter
        assert getattr(scale, arm) is not None, arm


def test_update_rejects_multiple_scale_inputs():
    """Only one of the three scale inputs may be supplied, matching legacy VideoFile.update."""
    clients = MagicMock()
    file = _video_file(clients, channel="cam")
    with pytest.raises(ValueError, match="at most one of"):
        file.update(true_frame_rate=30.0, scale_factor=2.0)
    clients.video.batch_update_video_channel_dataset_files.assert_not_called()


def test_update_requires_at_least_one_field():
    """An update with nothing set is rejected rather than sent as a no-op."""
    clients = MagicMock()
    file = _video_file(clients, channel="cam")
    with pytest.raises(ValueError, match="At least one of"):
        file.update()
    clients.video.batch_update_video_channel_dataset_files.assert_not_called()
