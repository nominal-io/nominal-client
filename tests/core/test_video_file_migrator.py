"""Video-file migration resilience: each case is a failure mode that stopped a real tenant migration."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

import requests
import urllib3.exceptions

from nominal.core.exceptions import (
    NominalIngestError,
    NominalIngestFailed,
    NominalIngestTimeout,
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalVideoFileMetadataError,
)
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.video_file_migrator import VideoFileMigrator
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.retry_utils import DEFAULT_MAX_ATTEMPTS
from nominal.experimental.migration.utils.video_file_utils import copy_video_file_to_video_dataset

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
    source_file._clients.video_file.get_ingest_status.return_value.ingest_status.type = "success"
    return source_file


def _mark_source_ingest_failed(source_file: MagicMock, message: str, error_type: str) -> None:
    status = source_file._clients.video_file.get_ingest_status.return_value.ingest_status
    status.type = "error"
    status.error.message = message
    status.error.error_type = error_type


def _make_destination_video(new_file: MagicMock) -> MagicMock:
    destination_video = MagicMock()
    destination_video._clients.workspace_rid = "ws-rid"
    destination_video.add_from_io.return_value = new_file
    return destination_video


def _timestamp_options() -> MagicMock:
    options = MagicMock()
    options.starting_timestamp = 0
    options.scaling_factor = 2.0
    return options


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("nominal.experimental.migration.utils.retry_utils.time.sleep", lambda _seconds: None)


def _mock_stream_response(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Stub the streamed source download as the context manager the code enters; returns the
    context-manager mock so tests can assert per-attempt open/close.
    """
    stream = MagicMock()
    stream.__enter__.return_value = MagicMock(raw=MagicMock())
    monkeypatch.setattr(
        "nominal.experimental.migration.utils.video_file_utils.requests.get",
        lambda *args, **kwargs: stream,
    )
    return stream


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    return requests.exceptions.HTTPError(f"{status_code} error", response=MagicMock(status_code=status_code))


def test_source_video_without_segment_metadata_is_skipped_not_raised() -> None:
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
    assert [(skip.resource_type, skip.source_rid) for skip in ctx.migration_state.skipped_resources] == [
        (ResourceType.VIDEO_FILE.value, source_file.rid)
    ]


def test_source_with_failed_ingest_is_skipped_before_transfer() -> None:
    """A file whose ingest failed at the source is bytes the platform already refused once —
    re-ingesting them elsewhere fails the same way, so skip before moving anything.
    """
    source_file = _make_source_video_file(_video_file_rid(19))
    _mark_source_ingest_failed(
        source_file, "Video failed to segment. Please contact support for assistance.", "VideoSegmenter:Internal"
    )
    destination_video = MagicMock()
    destination_video._clients.workspace_rid = "ws-rid"

    outcome = copy_video_file_to_video_dataset(source_file, destination_video)

    assert outcome.file is None
    assert outcome.skip_reason is not None
    assert "its own ingest failed there" in outcome.skip_reason
    assert "VideoSegmenter:Internal" in outcome.skip_reason
    # Nothing was read beyond the status, and nothing was transferred or created.
    source_file._get_file_ingest_options.assert_not_called()
    source_file._clients.catalog.get_video_file_uri.assert_not_called()
    destination_video.add_from_io.assert_not_called()


def test_source_with_failed_ingest_records_skip_and_no_mapping() -> None:
    """The failed-at-source file lands in the end-of-run summary and a rerun re-checks it cheaply."""
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(20))
    _mark_source_ingest_failed(source_file, "Video failed to segment.", "VideoSegmenter:Internal")

    VideoFileMigrator(ctx).copy_from(source_file, MagicMock(_clients=MagicMock(workspace_rid="ws-rid")))

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) is None
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "unusable at source" in ctx.migration_state.skipped_resources[0].reason


def test_unavailable_source_ingest_status_does_not_block_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    """The status gate avoids wasted transfer; its own lookup failing (e.g. a legacy file with
    no ingest-status record) must degrade to copying, not block a healthy file.
    """
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(21))
    source_file._clients.video_file.get_ingest_status.side_effect = _http_error(404)
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(210)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)

    _mock_stream_response(monkeypatch)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []


def test_ingest_timeout_records_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timed-out ingest is mapped (so a rerun does not re-upload) *and* reported as incomplete."""
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(3))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(30)
    new_file.poll_until_ingestion_completed.side_effect = NominalIngestTimeout("still ingesting")
    destination_video = _make_destination_video(new_file)

    _mock_stream_response(monkeypatch)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "ingest did not complete" in ctx.migration_state.skipped_resources[0].reason
    # Timing metadata is applied even on timeout — no rerun will come back to set it.
    new_file.update.assert_called_once()


def test_successful_copy_records_mapping_and_no_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean copy records the mapping and sets timing from the source's defining pair —
    start + scale, never a segment-derived absolute ending, which goes stale if the source's
    declared start was edited after segmentation (the inverted-bounds production failure).
    """
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(4))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(40)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)

    _mock_stream_response(monkeypatch)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []
    new_file.update.assert_called_once_with(starting_timestamp=0, scale_factor=2.0)


def test_percent_encoded_source_filename_is_sanitized_before_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Source names can arrive double-encoded (' ' -> '%20' -> '%2520'), and the uploader rejects '%'."""
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

    _mock_stream_response(monkeypatch)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    uploaded_name = destination_video.add_from_io.call_args.kwargs["name"]
    assert "%" not in uploaded_name
    assert uploaded_name == "flight_2_2520cam_2520front"


def test_video_ingest_timeout_is_threaded_through_from_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The configured timeout reaches the poll, so a migration can tune it in one place."""
    ctx = _make_context()
    ctx.video_ingest_timeout = timedelta(seconds=5)
    source_file = _make_source_video_file(_video_file_rid(6))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(60)
    destination_video = _make_destination_video(new_file)

    _mock_stream_response(monkeypatch)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    # The poll timeout is measured against a shared deadline, so assert the bound rather than
    # an exact value.
    new_file.poll_until_ingestion_completed.assert_called_once()
    passed_timeout = new_file.poll_until_ingestion_completed.call_args.kwargs["timeout"]
    assert timedelta(0) < passed_timeout <= timedelta(seconds=5)


def test_connection_broken_mid_transfer_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection reset while streaming source bytes into the upload restarts the transfer."""
    _no_sleep(monkeypatch)
    stream = _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(7))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(70)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)
    destination_video.add_from_io.side_effect = [
        urllib3.exceptions.ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')"),
        new_file,
    ]

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert destination_video.add_from_io.call_count == 2
    # The download connection is closed once per attempt — retries must not stack open sockets.
    assert stream.__exit__.call_count == 2
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []


def test_multipart_upload_failure_wrapping_transient_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """put_multipart_upload raises an ExceptionGroup of per-attempt wrappers (no __cause__ on the
    group itself) — a transient root cause inside it must still be retried.
    """
    _no_sleep(monkeypatch)
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(15))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    attempt_error = NominalMultipartUploadError("part 1, attempt 3: 502 Server Error")
    attempt_error.__cause__ = _http_error(502)
    new_file = MagicMock()
    new_file.rid = _video_file_rid(150)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)
    destination_video.add_from_io.side_effect = [
        NominalMultipartUploadFailed("upload failed after retries", [attempt_error]),
        new_file,
    ]

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert destination_video.add_from_io.call_count == 2
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []


def test_transient_error_during_ingest_poll_does_not_reupload(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 502 on a single status check must not abandon a finished upload — polling resumes instead."""
    _no_sleep(monkeypatch)
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(8))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(80)
    new_file.poll_until_ingestion_completed.side_effect = [_http_error(502), None]
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert destination_video.add_from_io.call_count == 1
    assert new_file.poll_until_ingestion_completed.call_count == 2
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []


def test_destination_ingest_failure_records_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A terminal destination-side ingest error (e.g. segmentation failure) is a skip, not an abort."""
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(9))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(90)
    new_file.poll_until_ingestion_completed.side_effect = NominalIngestFailed(
        "ingest failed for video: Video failed to segment. (VideoSegmenter:Internal)"
    )
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "ingest failed at destination" in ctx.migration_state.skipped_resources[0].reason
    # The file needs hand-checking anyway; don't push timing metadata onto a failed ingest.
    new_file.update.assert_not_called()


def test_unknown_ingest_status_records_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any post-upload failure — even an unexpected one — must map the copy, or a rerun duplicates it."""
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(13))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(130)
    new_file.poll_until_ingestion_completed.side_effect = NominalIngestError("unhandled ingest status 'mystery'")
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "could not be confirmed" in ctx.migration_state.skipped_resources[0].reason


def test_exhausted_poll_retries_record_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 502 that outlasts the retry budget after a successful upload maps the copy — no rerun re-upload."""
    _no_sleep(monkeypatch)
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(14))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(140)
    new_file.poll_until_ingestion_completed.side_effect = _http_error(502)
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert destination_video.add_from_io.call_count == 1
    # The full budget was spent before giving up — otherwise this test passes with retries off.
    assert new_file.poll_until_ingestion_completed.call_count == DEFAULT_MAX_ATTEMPTS
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "could not be confirmed" in ctx.migration_state.skipped_resources[0].reason


def test_rejected_timestamp_update_records_mapping_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 400 on the post-ingest timestamp update must not abort the asset or orphan the ingested copy."""
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(10))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    new_file = MagicMock()
    new_file.rid = _video_file_rid(100)
    new_file.poll_until_ingestion_completed.return_value = None
    new_file.update.side_effect = _http_error(400)
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    # 400 is not transient: exactly one attempt.
    new_file.update.assert_called_once()
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert len(ctx.migration_state.skipped_resources) == 1
    reason = ctx.migration_state.skipped_resources[0].reason
    assert "timestamp update was rejected" in reason
    # The sent values are recorded — often the only way to diagnose a destination-side 400.
    assert "starting_timestamp=0" in reason
    assert "scale_factor=2.0" in reason


def test_unexpected_copy_failure_is_recorded_and_does_not_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any copy failure logs-and-continues: the asset task survives, and a rerun re-attempts the file."""
    _no_sleep(monkeypatch)
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(11))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())

    destination_video = MagicMock()
    destination_video._clients.workspace_rid = "ws-rid"
    destination_video.add_from_io.side_effect = requests.exceptions.ReadTimeout(
        "HTTPSConnectionPool(host='example.invalid', port=443): Read timed out."
    )

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) is None
    assert len(ctx.migration_state.skipped_resources) == 1
    assert "copy failed" in ctx.migration_state.skipped_resources[0].reason


def test_rerun_success_clears_stale_skip_from_earlier_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resource that failed transiently last run and succeeds this run is not still reported as skipped."""
    _mock_stream_response(monkeypatch)
    ctx = _make_context()
    source_file = _make_source_video_file(_video_file_rid(12))
    source_file._get_file_ingest_options.return_value = (None, _timestamp_options())
    ctx.migration_state.record_skip(ResourceType.VIDEO_FILE, source_file.rid, "copy failed: transient")

    new_file = MagicMock()
    new_file.rid = _video_file_rid(120)
    new_file.poll_until_ingestion_completed.return_value = None
    destination_video = _make_destination_video(new_file)

    VideoFileMigrator(ctx).copy_from(source_file, destination_video)

    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) == new_file.rid
    assert ctx.migration_state.skipped_resources == []


def test_parallel_runner_passes_video_ingest_timeout_to_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The parallel runner builds its own MigrationContext; dropping the timeout there would
    silently ignore `--video-ingest-timeout-seconds` for every parallel run.
    """
    from nominal.experimental.migration import parallel_migration_runner
    from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
    from nominal.experimental.migration.config.migration_resources import MigrationResources
    from nominal.experimental.migration.migration_runner import MigrationRunner

    runner = MigrationRunner(
        migration_resources=MigrationResources(source_assets={}, source_standalone_templates=[]),
        dataset_config=MigrationDatasetConfig(include_dataset_files=False, preserve_dataset_uuid=True),
        destination_client=MagicMock(),
        migration_state_path=tmp_path / "state.json",
        video_ingest_timeout=timedelta(seconds=7),
    )

    captured: list[MigrationContext] = []

    def capture_ctx(ctx: MigrationContext) -> MagicMock:
        captured.append(ctx)
        return MagicMock()

    monkeypatch.setattr(parallel_migration_runner, "AssetMigrator", capture_ctx)
    monkeypatch.setattr(parallel_migration_runner, "WorkbookTemplateMigrator", capture_ctx)
    monkeypatch.setattr(parallel_migration_runner, "ChecklistMigrator", capture_ctx)

    parallel_migration_runner.run_parallel_migration(runner, max_workers=1)

    assert [ctx.video_ingest_timeout for ctx in captured] == [timedelta(seconds=7)] * 3
