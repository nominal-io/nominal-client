"""Tests for video-file migration resilience.

Every case here is a failure mode that stopped a real tenant migration: an ingest that never
finished (hanging the run indefinitely), a source video with no segment metadata, and a source
filename carrying percent-encoded characters the destination refuses.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.exceptions import NominalIngestTimeout, NominalVideoFileMetadataError
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.video_file_migrator import VideoFileMigrator
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.video_file_utils import (
    _resolve_destination_file_stem,
    copy_video_file_to_video_dataset,
)

_STACK = "cerulean-staging"


def _video_file_rid(n: int) -> str:
    return f"ri.video.{_STACK}.video-file.{n:08x}-0000-0000-0000-000000000000"


def _make_context() -> MigrationContext:
    destination_client = MagicMock()
    destination_client._clients.workspace_rid = "ws-rid"
    return MigrationContext(destination_client=destination_client, migration_state=MigrationState())


def _make_source_video_file(rid: str, name: str = "video.mp4") -> MagicMock:
    source_file = MagicMock()
    source_file.rid = rid
    source_file.name = name
    source_file.description = None
    source_file._clients.catalog.get_video_file_uri.return_value.uri = "https://example.invalid/video.mp4"
    return source_file


def _make_destination_video(new_file: MagicMock) -> MagicMock:
    destination_video = MagicMock()
    destination_video._clients.workspace_rid = "ws-rid"
    destination_video.add_from_io.return_value = new_file
    return destination_video


def _timestamp_options() -> MagicMock:
    options = MagicMock()
    options.starting_timestamp = 0
    options.ending_timestamp = 1
    return options


def test_source_video_without_segment_metadata_is_skipped_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    """A video that never finished segmenting at the source is unusable — skip it, don't abort the asset."""
    source_file = _make_source_video_file(_video_file_rid(1))
    source_file._get_file_ingest_options.side_effect = NominalVideoFileMetadataError("no segment metadata")
    destination_video = MagicMock()
    destination_video._clients.workspace_rid = "ws-rid"

    outcome = copy_video_file_to_video_dataset(source_file, destination_video)

    assert outcome.file is None
    assert outcome.skip_reason is not None
    assert "unusable at source" in outcome.skip_reason
    # Nothing was fetched or created on the destination.
    source_file._clients.catalog.get_video_file_uri.assert_not_called()
    destination_video.add_from_io.assert_not_called()


def test_skipped_source_video_is_recorded_as_skip_and_not_mapped() -> None:
    """A skipped video leaves no mapping — so it is never reported as migrated."""
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(2))
    source_file._get_file_ingest_options.side_effect = NominalVideoFileMetadataError("no segment metadata")

    VideoFileMigrator(ctx).copy_from(source_file, MagicMock(_clients=MagicMock(workspace_rid="ws-rid")))

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) is None
    assert [(skip.resource_type, skip.source_rid) for skip in ctx.skipped()] == [
        (ResourceType.VIDEO_FILE, source_file.rid)
    ]


def test_ingest_timeout_records_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timed-out ingest is mapped (so a rerun does not re-upload) *and* reported as incomplete."""
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(3))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(30)
    new_file.poll_until_ingestion_completed.side_effect = NominalIngestTimeout("still ingesting")
    destination_video = _make_destination_video(new_file)

    monkeypatch.setattr(
        "nominal.experimental.migration.utils.video_file_utils.requests.get",
        lambda *args, **kwargs: MagicMock(raw=MagicMock()),
    )

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.skipped()) == 1
    assert "ingest did not complete" in ctx.skipped()[0].reason
    # Timing metadata is not applied on top of a file the server never finished ingesting.
    new_file.update.assert_not_called()


def test_successful_copy_records_mapping_and_no_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(4))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(40)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)

    monkeypatch.setattr(
        "nominal.experimental.migration.utils.video_file_utils.requests.get",
        lambda *args, **kwargs: MagicMock(raw=MagicMock()),
    )

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.skipped() == ()
    new_file.update.assert_called_once()


def test_percent_encoded_source_filename_is_sanitized_before_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Percent characters break Azure SAS signing, so the uploader rejects them outright.

    Source names can arrive double-encoded (a literal space -> '%20' -> '%2520'), which must not
    block the copy.
    """
    ctx = _make_context()
    source_file = _make_source_video_file(
        _video_file_rid(5),
        name="2026-01-16T17_52_57.976179697Z_flight_2%2520cam%2520front.mp4",
    )
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(50)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)

    monkeypatch.setattr(
        "nominal.experimental.migration.utils.video_file_utils.requests.get",
        lambda *args, **kwargs: MagicMock(raw=MagicMock()),
    )

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    uploaded_name = destination_video.add_from_io.call_args.kwargs["name"]
    assert "%" not in uploaded_name
    assert uploaded_name == "flight_2_2520cam_2520front"


def test_resolve_destination_file_stem_strips_ingest_timestamp_prefix() -> None:
    assert _resolve_destination_file_stem("2026-01-16T17_52_57.976179697Z_my_video.mp4") == "my_video"
    # A name without the ingest prefix is left alone.
    assert _resolve_destination_file_stem("my_video.mp4") == "my_video"


def test_video_ingest_timeout_is_threaded_through_from_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The configured timeout reaches the poll, so a migration can tune it in one place."""
    ctx = _make_context()
    ctx.video_ingest_timeout = timedelta(seconds=5)
    source_file = _make_source_video_file(_video_file_rid(6))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(60)
    destination_video = _make_destination_video(new_file)

    monkeypatch.setattr(
        "nominal.experimental.migration.utils.video_file_utils.requests.get",
        lambda *args, **kwargs: MagicMock(raw=MagicMock()),
    )

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    new_file.poll_until_ingestion_completed.assert_called_once_with(timeout=timedelta(seconds=5))
