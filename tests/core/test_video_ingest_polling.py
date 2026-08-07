"""Tests that video ingest polling always terminates.

A server-side worker that dies mid-ingest leaves a video in `inProgress` forever. Without a
deadline these loops poll once a second indefinitely — which wedged a real tenant migration for
hours with no output.
"""

from __future__ import annotations

import warnings
from datetime import timedelta
from typing import Iterator
from unittest.mock import MagicMock

import pytest

from nominal.core.exceptions import LegacyVideoDeprecationWarning, NominalIngestFailed, NominalIngestTimeout
from nominal.core.video import Video
from nominal.core.video_file import VideoFile


@pytest.fixture(autouse=True)
def _allow_legacy_video_api() -> Iterator[None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=LegacyVideoDeprecationWarning)
        yield


def _status(status_type: str) -> MagicMock:
    status = MagicMock()
    status.ingest_status.type = status_type
    return status


def _video_file(status_type: str) -> VideoFile:
    clients = MagicMock()
    clients.video_file.get_ingest_status.return_value = _status(status_type)
    return VideoFile(
        rid="ri.video.cerulean-staging.video-file.00000001-0000-0000-0000-000000000000",
        name="video.mp4",
        description=None,
        created_at=0,
        _clients=clients,
    )


def test_poll_raises_timeout_when_ingest_never_finishes() -> None:
    """The deadline fires rather than polling forever."""
    video_file = _video_file("inProgress")

    with pytest.raises(NominalIngestTimeout, match="still ingesting"):
        video_file.poll_until_ingestion_completed(
            interval=timedelta(seconds=0),
            timeout=timedelta(seconds=0),
        )


def test_poll_returns_immediately_on_success() -> None:
    """A completed ingest costs exactly one status check."""
    video_file = _video_file("success")

    video_file.poll_until_ingestion_completed(interval=timedelta(seconds=0), timeout=timedelta(seconds=0))

    video_file._clients.video_file.get_ingest_status.assert_called_once()


def test_poll_still_raises_ingest_failure_rather_than_timeout() -> None:
    """A real ingest failure must keep surfacing as a failure, not get masked by the new deadline."""
    video_file = _video_file("error")
    error = MagicMock()
    error.message = "Video failed to segment"
    error.error_type = "VideoSegmenter:Internal"
    video_file._clients.video_file.get_ingest_status.return_value.ingest_status.error = error

    with pytest.raises(NominalIngestFailed, match="Video failed to segment"):
        video_file.poll_until_ingestion_completed(interval=timedelta(seconds=0), timeout=timedelta(seconds=0))


def test_poll_eventually_succeeds_within_timeout() -> None:
    """An ingest that finishes after a few polls is not cut short."""
    video_file = _video_file("inProgress")
    video_file._clients.video_file.get_ingest_status.side_effect = [
        _status("inProgress"),
        _status("inProgress"),
        _status("success"),
    ]

    video_file.poll_until_ingestion_completed(interval=timedelta(seconds=0), timeout=timedelta(seconds=30))

    assert video_file._clients.video_file.get_ingest_status.call_count == 3


def _video(status_type: str) -> Video:
    clients = MagicMock()
    clients.video.get_ingest_status.return_value.type = status_type
    return Video(
        rid="ri.video.cerulean-staging.video.00000001-0000-0000-0000-000000000000",
        name="video",
        description=None,
        properties={},
        labels=(),
        created_at=0,
        _clients=clients,
    )


def test_standalone_video_poll_raises_timeout_when_ingest_never_finishes() -> None:
    """`Video` has its own loop with a different branch shape, so its deadline needs its own test."""
    video = _video("inProgress")

    with pytest.raises(NominalIngestTimeout, match="still ingesting"):
        video.poll_until_ingestion_completed(interval=timedelta(seconds=0), timeout=timedelta(seconds=0))


def test_standalone_video_poll_still_raises_ingest_failure_rather_than_timeout() -> None:
    """A real failure must not be masked by the new deadline."""
    video = _video("error")
    error = MagicMock()
    error.errors = [MagicMock(message="Video failed to segment", error_type="VideoSegmenter:Internal")]
    video._clients.video.get_ingest_status.return_value.error = error

    with pytest.raises(NominalIngestFailed, match="Video failed to segment"):
        video.poll_until_ingestion_completed(interval=timedelta(seconds=0), timeout=timedelta(seconds=0))
