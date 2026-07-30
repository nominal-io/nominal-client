from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from conjure_python_client import ConjureEncoder

from nominal.core.exceptions import NominalVideoTimestampModeError
from nominal.core.video import Video, _build_video_file_timestamp_manifest


@pytest.mark.parametrize(
    ("kwargs", "expected_arm"),
    [
        pytest.param({"start": 1_000_000_000}, "noManifest", id="start"),
        pytest.param({"mcap_topic": "/camera/front"}, "mcap", id="mcap-topic"),
        pytest.param({"frame_timestamps": [1, 2, 3]}, "s3path", id="frame-timestamps"),
    ],
)
def test_manifest_selects_the_arm_for_its_mode(kwargs: dict[str, Any], expected_arm: str) -> None:
    """Each timestamp mode selects its own union arm.

    A conjure union serializes exactly one arm, so the encoded discriminator is also the assertion
    that the other two are unset.
    """
    with patch("nominal.core.video._upload_frame_timestamps", return_value="s3://path"):
        manifest = _build_video_file_timestamp_manifest("auth", "ws", MagicMock(), **kwargs)

    assert ConjureEncoder.do_encode(manifest)["type"] == expected_arm


def test_manifest_from_mcap_topic_carries_the_topic_locator() -> None:
    """The mcap arm keeps the topic it was given, which is what locates the video channel."""
    manifest = _build_video_file_timestamp_manifest("auth", None, MagicMock(), mcap_topic="/camera/front")

    assert manifest.mcap is not None
    assert manifest.mcap.mcap_channel_locator.topic == "/camera/front"


def test_manifest_from_frame_timestamps_uploads_the_sidecar() -> None:
    """Per-frame timestamps are uploaded, and the returned path is what the arm references."""
    with patch("nominal.core.video._upload_frame_timestamps", return_value="s3://path") as upload:
        manifest = _build_video_file_timestamp_manifest("auth", "ws", MagicMock(), frame_timestamps=[1, 2, 3])

    assert manifest.s3path == "s3://path"
    upload.assert_called_once()


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({}, id="no-mode"),
        pytest.param({"start": 1, "mcap_topic": "/t"}, id="start-and-mcap-topic"),
        pytest.param({"start": 1, "frame_timestamps": [1]}, id="start-and-frame-timestamps"),
        pytest.param({"frame_timestamps": [1], "mcap_topic": "/t"}, id="frame-timestamps-and-mcap-topic"),
    ],
)
def test_manifest_requires_exactly_one_mode(kwargs: dict[str, Any]) -> None:
    """Zero or multiple timestamp modes raise a ValueError."""
    with pytest.raises(NominalVideoTimestampModeError, match="exactly one of"):
        _build_video_file_timestamp_manifest("auth", None, MagicMock(), **kwargs)


def _video() -> Video:
    return Video(
        rid="ri.scout.video.v.1",
        name="cam",
        description=None,
        properties={},
        labels=(),
        created_at=1_753_000_000_000_000_000,
        _clients=MagicMock(),
    )


@pytest.mark.parametrize(
    "timestamp_modes",
    [
        pytest.param({}, id="neither"),
        pytest.param({"start": 1_753_000_000_000_000_000, "frame_timestamps": [1, 2]}, id="both"),
    ],
)
def test_add_file_requires_exactly_one_timestamp_mode(timestamp_modes):
    """Video.add_file rejects zero or both modes before touching the path.

    Passing both used to take the 'start' branch and silently drop the per-frame timestamps, since
    add_file forwards only one mode to add_from_io and the conflict never reached validation.
    """
    with pytest.raises(NominalVideoTimestampModeError, match="exactly one of 'start' or 'frame_timestamps'"):
        _video().add_file("no/such/video.mp4", **timestamp_modes)


def test_timestamp_mode_error_is_also_a_value_error() -> None:
    """These call sites raised ValueError before the type existed, so `except ValueError` still works."""
    with pytest.raises(ValueError, match="exactly one of"):
        _build_video_file_timestamp_manifest("auth", None, MagicMock())
