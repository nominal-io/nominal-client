from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from nominal_api import api

from nominal.core._video_ingest import (
    build_video_ingest_options,
    iter_video_channel_names,
    resolve_video_channel_for_file,
)
from nominal.core.bounds import Bounds
from nominal.core.video import _build_video_file_timestamp_manifest


def test_manifest_from_start_uses_no_manifest_arm():
    """A bare starting timestamp produces the no-manifest arm."""
    manifest = _build_video_file_timestamp_manifest("auth", None, MagicMock(), start=1_000_000_000)
    assert manifest.no_manifest is not None
    assert manifest.s3path is None
    assert manifest.mcap is None


def test_manifest_from_mcap_topic_uses_mcap_arm():
    """An mcap topic produces the mcap arm with the topic locator."""
    manifest = _build_video_file_timestamp_manifest("auth", None, MagicMock(), mcap_topic="/camera/front")
    assert manifest.mcap is not None
    assert manifest.mcap.mcap_channel_locator.topic == "/camera/front"


def test_manifest_from_frame_timestamps_uploads_and_uses_s3path():
    """Per-frame timestamps are uploaded and referenced via the s3path arm."""
    with patch("nominal.core.video._upload_frame_timestamps", return_value="s3://path") as upload:
        manifest = _build_video_file_timestamp_manifest("auth", "ws", MagicMock(), frame_timestamps=[1, 2, 3])
    assert manifest.s3path == "s3://path"
    upload.assert_called_once()


def test_manifest_requires_exactly_one_mode():
    """Zero or multiple timestamp modes raise a ValueError."""
    with pytest.raises(ValueError, match="exactly one of"):
        _build_video_file_timestamp_manifest("auth", None, MagicMock())
    with pytest.raises(ValueError, match="exactly one of"):
        _build_video_file_timestamp_manifest("auth", None, MagicMock(), start=1, mcap_topic="/t")


def test_ingest_options_builds_video_v2():
    """The options builder maps every argument onto the VideoOptsV2 arm."""
    manifest = MagicMock(name="manifest")
    opts = build_video_ingest_options(
        "ds-rid",
        channel="camera/front",
        tags={"vehicle": "alpha"},
        s3_path="s3://p",
        timestamp_manifest=manifest,
        overwrite_overlapping=True,
    )
    assert opts.video_v2 is not None
    assert opts.video_v2.channel == "camera/front"
    assert opts.video_v2.tags == {"vehicle": "alpha"}
    assert opts.video_v2.over_write_segments is True
    assert opts.video_v2.target.existing.dataset_rid == "ds-rid"
    assert opts.video_v2.timestamp_manifest is manifest


def test_ingest_options_none_tags_becomes_empty_and_no_overwrite_is_none():
    """Absent tags map to an empty dict and overwrite=False maps to an absent field."""
    opts = build_video_ingest_options(
        "ds-rid",
        channel="c",
        tags=None,
        s3_path="s3://p",
        timestamp_manifest=MagicMock(),
        overwrite_overlapping=False,
    )
    assert opts.video_v2.tags == {}
    assert opts.video_v2.over_write_segments is None


def _channel_metadata(name: str, data_type: object) -> MagicMock:
    meta = MagicMock()
    meta.name = name
    meta.data_type = data_type
    return meta


def _search_page(results: list, next_page_token: str | None = None) -> MagicMock:
    page = MagicMock()
    page.results = results
    page.next_page_token = next_page_token
    return page


def _files_page(file_ids: list[str], next_page_token: str | None = None) -> MagicMock:
    page = MagicMock()
    page.dataset_files = [MagicMock(dataset_file_id=fid) for fid in file_ids]
    page.next_page_token = next_page_token
    return page


def test_iter_video_channel_names_yields_only_video_typed_channels():
    """Non-video channels on the dataset are skipped."""
    clients = MagicMock()
    clients.datasource.search_channels.return_value = _search_page(
        [
            _channel_metadata("temperature", api.SeriesDataType.DOUBLE),
            _channel_metadata("camera/front", api.SeriesDataType.VIDEO),
        ]
    )
    assert list(iter_video_channel_names(clients, "ds-1")) == ["camera/front"]


def test_resolve_video_channel_finds_the_channel_containing_the_file():
    """Discovery probes each video channel and returns the one holding the file."""
    clients = MagicMock()
    clients.datasource.search_channels.return_value = _search_page(
        [
            _channel_metadata("camera/front", api.SeriesDataType.VIDEO),
            _channel_metadata("camera/rear", api.SeriesDataType.VIDEO),
        ]
    )
    clients.video.list_video_channel_dataset_files.side_effect = [
        _files_page(["other-file"]),
        _files_page(["file-1"]),
    ]
    assert resolve_video_channel_for_file(clients, "ds-1", "file-1", None) == "camera/rear"


def test_resolve_video_channel_scopes_the_probe_to_the_file_bounds():
    """The file's own bounds narrow each probe instead of paging a channel's whole history."""
    clients = MagicMock()
    clients.datasource.search_channels.return_value = _search_page(
        [_channel_metadata("camera/front", api.SeriesDataType.VIDEO)]
    )
    clients.video.list_video_channel_dataset_files.return_value = _files_page(["file-1"])
    resolve_video_channel_for_file(clients, "ds-1", "file-1", Bounds(start=100, end=200))

    _, request = clients.video.list_video_channel_dataset_files.call_args[0]
    assert request.bounds is not None


def test_resolve_video_channel_raises_when_no_channel_contains_the_file():
    """A file that no video channel claims is an error, not a silent None."""
    clients = MagicMock()
    clients.datasource.search_channels.return_value = _search_page(
        [_channel_metadata("camera/front", api.SeriesDataType.VIDEO)]
    )
    clients.video.list_video_channel_dataset_files.return_value = _files_page(["other-file"])
    with pytest.raises(ValueError, match="could not determine the video channel"):
        resolve_video_channel_for_file(clients, "ds-1", "file-1", None)
