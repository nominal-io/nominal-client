from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core._video_ingest import build_video_ingest_options, build_video_timestamp_manifest


def test_manifest_from_start_uses_no_manifest_arm():
    manifest = build_video_timestamp_manifest("auth", None, MagicMock(), start=1_000_000_000)
    assert manifest.no_manifest is not None
    assert manifest.s3path is None
    assert manifest.mcap is None


def test_manifest_from_mcap_topic_uses_mcap_arm():
    manifest = build_video_timestamp_manifest("auth", None, MagicMock(), mcap_topic="/camera/front")
    assert manifest.mcap is not None
    assert manifest.mcap.mcap_channel_locator.topic == "/camera/front"


def test_manifest_from_frame_timestamps_uploads_and_uses_s3path():
    with patch("nominal.core._video_ingest._upload_frame_timestamps", return_value="s3://path") as upload:
        manifest = build_video_timestamp_manifest("auth", "ws", MagicMock(), frame_timestamps=[1, 2, 3])
    assert manifest.s3path == "s3://path"
    upload.assert_called_once()


def test_manifest_requires_exactly_one_mode():
    with pytest.raises(ValueError):
        build_video_timestamp_manifest("auth", None, MagicMock())
    with pytest.raises(ValueError):
        build_video_timestamp_manifest("auth", None, MagicMock(), start=1, mcap_topic="/t")


def test_ingest_options_builds_video_v2():
    manifest = MagicMock(name="manifest")
    opts = build_video_ingest_options("ds-rid", "camera/front", {"vehicle": "alpha"}, "s3://p", manifest, True)
    assert opts.video_v2 is not None
    assert opts.video_v2.channel == "camera/front"
    assert opts.video_v2.tags == {"vehicle": "alpha"}
    assert opts.video_v2.over_write_segments is True
    assert opts.video_v2.target.existing.dataset_rid == "ds-rid"
    assert opts.video_v2.timestamp_manifest is manifest


def test_ingest_options_none_tags_becomes_empty_and_no_overwrite_is_none():
    opts = build_video_ingest_options("ds-rid", "c", None, "s3://p", MagicMock(), False)
    assert opts.video_v2.tags == {}
    assert opts.video_v2.over_write_segments is None
