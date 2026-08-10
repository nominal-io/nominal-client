"""Tests that skipped resources survive persistence and are reported at the end of a run.

Skips are recorded on the persisted migration state rather than in memory, so that a resumed run
still knows what earlier attempts left behind — and so both runners can report them.
"""

from __future__ import annotations

import logging
import sys

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_runner import log_skipped_resources
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.parallel_migration_state import ThreadSafeMigrationState
from nominal.experimental.migration.resource_type import ResourceType

_VIDEO_FILE_RID = "ri.video.cerulean-staging.video-file.00000001-0000-0000-0000-000000000000"


def test_skips_round_trip_through_serialized_state() -> None:
    """A skip recorded mid-run is still there after the state is written and reloaded."""
    state = MigrationState()
    state.record_skip(ResourceType.VIDEO_FILE, _VIDEO_FILE_RID, "ingest did not complete")

    reloaded = MigrationState.from_json(state.to_json())

    assert [(skip.resource_type, skip.source_rid, skip.reason) for skip in reloaded.skipped_resources] == [
        (ResourceType.VIDEO_FILE.value, _VIDEO_FILE_RID, "ingest did not complete")
    ]


def test_parallel_state_adopts_every_field_when_resuming() -> None:
    """Resuming must not drop skips or deferred workbooks — only `rid_mapping` used to carry over."""
    state = MigrationState()
    state.record_skip(ResourceType.VIDEO_FILE, _VIDEO_FILE_RID, "ingest did not complete")
    state.record_mapping(ResourceType.ASSET, "old-asset", "new-asset")
    state.record_pending_multi_asset_workbook("wb-rid", ["asset-a", "asset-b"])

    resumed = ThreadSafeMigrationState.from_state(state)

    assert resumed.skipped_resources == state.skipped_resources
    assert resumed.pending_multi_asset_workbooks == {"wb-rid": ["asset-a", "asset-b"]}
    assert resumed.get_mapped_rid(ResourceType.ASSET, "old-asset") == "new-asset"


def test_skips_are_reported_at_end_of_run(caplog: pytest.LogCaptureFixture) -> None:
    """The report is what stops a partial migration from reading as a complete one."""
    state = MigrationState()
    state.record_skip(ResourceType.VIDEO_FILE, _VIDEO_FILE_RID, "ingest did not complete")

    with caplog.at_level(logging.WARNING):
        log_skipped_resources(state)

    assert "1 resource(s) were skipped" in caplog.text
    assert _VIDEO_FILE_RID in caplog.text
    assert "ingest did not complete" in caplog.text


def test_clean_run_states_so_explicitly_and_warns_about_nothing(caplog: pytest.LogCaptureFixture) -> None:
    """A clean run says so positively — absence of warnings is not the same as confirmation."""
    with caplog.at_level(logging.INFO):
        log_skipped_resources(MigrationState())

    assert "Nothing was skipped" in caplog.text
    assert [record for record in caplog.records if record.levelno >= logging.WARNING] == []
