"""Live-wire check that transient network failures are retried and contained.

Unlike test_migration.py (which goes through MigrationRunner against two healthy
environments), this exercises the failure path with a real socket: a destination client
pointed at an unreachable address makes every upload attempt raise a genuine requests
ConnectionError, verifying end to end that the error is classified transient, retried,
and — once the retry budget is exhausted — recorded as a skip instead of aborting.

Run with:
    uv run pytest tests/e2e/migration/test_retry_e2e.py --source-profile=<prod> -v
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.video_file_migrator import VideoFileMigrator
from nominal.experimental.migration.resource_type import ResourceType
from tests.e2e import POLL_INTERVAL

# Connection refused is near-instant (nothing listens on the discard port), so each retry
# attempt fails fast instead of waiting out a timeout.
_UNREACHABLE_BASE_URL = "http://127.0.0.1:9/api"


def test_unreachable_destination_is_retried_then_recorded_as_skip(
    source_client: NominalClient,
    mp4_data: bytes,
    register_cleanup,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A real source video file, so the copy exercises the genuine metadata + download legs
    # before the upload leg hits the unreachable destination.
    video = source_client.create_video(f"migration-e2e-retry-{uuid4().hex[:8]}")
    register_cleanup(video.archive)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    video.add_from_io(BytesIO(mp4_data), "retry-test.mp4", start=start).poll_until_ingestion_completed(
        interval=POLL_INTERVAL
    )
    (source_file,) = video.list_files()

    bogus_client = NominalClient.create(
        base_url=_UNREACHABLE_BASE_URL,
        token="bogus-token",
        # Pinned so nothing tries to resolve a workspace over the dead socket before the upload.
        workspace_rid="ri.security.main.workspace.00000000-0000-0000-0000-000000000000",
    )
    # A real Video handle rebound to the unreachable client: only the upload leg fails.
    dest_video = dataclasses.replace(source_client.get_video(video.rid), _clients=bogus_client._clients)

    ctx = MigrationContext(destination_client=bogus_client, migration_state=MigrationState())
    with caplog.at_level(logging.WARNING, logger="nominal.experimental.migration"):
        # Must not raise: exhausted retries log-and-continue.
        VideoFileMigrator(ctx).copy_from(source_file, dest_video)

    retry_lines = [
        record for record in caplog.records if "Transient failure in copy of video file" in record.getMessage()
    ]
    assert retry_lines, "expected the unreachable upload to be classified transient and retried"

    # No mapping (a rerun re-attempts the file), and the failure is surfaced as a skip.
    assert ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid) is None
    assert [skipped.source_rid for skipped in ctx.migration_state.skipped_resources] == [source_file.rid]
    assert "copy failed" in ctx.migration_state.skipped_resources[0].reason
