from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core._video_ingest import build_video_ingest_options
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
