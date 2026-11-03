"""Tests for multi-asset/run workbook migration: copy methods, deferred routing, and migration state."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migration_state import MigrationState, SkippedResource
from nominal.experimental.migration.migrator.asset_migrator import AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_migrator import WorkbookMigrator
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration
from nominal.experimental.migration.resource_type import ResourceType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STACK = "cerulean-staging"


def _asset_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.asset.{n:08x}-0000-0000-0000-000000000000"


def _run_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.run.{n:08x}-0000-0000-0000-000000000000"


def _wb_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.notebook.{n:08x}-0000-0000-0000-000000000000"


def _make_context(source_asset_rids: frozenset[str] = frozenset()) -> MigrationContext:
    mock_client = MagicMock()
    return MigrationContext(
        destination_client=mock_client,
        migration_state=MigrationState(),
        source_asset_rids=source_asset_rids,
    )


def _stub_source_workbook(
    rid: str, asset_rids: list[str] | None = None, run_rids: list[str] | None = None
) -> MagicMock:
    wb = MagicMock()
    wb.rid = rid
    wb.title = "Test Workbook"
    wb.asset_rids = asset_rids
    wb.run_rids = run_rids
    wb.is_draft.return_value = False
    wb._clients.auth_header = "Bearer src"
    return wb


def _stub_raw_notebook(
    title: str = "WB", labels: list[str] | None = None, properties: dict[str, str] | None = None
) -> MagicMock:
    nb = MagicMock()
    nb.content_v2 = None  # forces use of nb.content, avoiding isinstance check on MagicMock
    nb.metadata.title = title
    nb.metadata.description = ""
    nb.metadata.labels = labels or ["tag"]
    nb.metadata.properties = properties or {"env": "staging"}
    nb.metadata.preview_image = None
    return nb


# ---------------------------------------------------------------------------
# MigrationState: pending workbooks and skip log
# ---------------------------------------------------------------------------


class TestMigrationStatePendingAndSkips:
    def test_pending_workbooks_are_idempotent_and_clearable(self) -> None:
        """record_pending overwrites on repeat calls; clear removes the entry without error;
        clearing a missing key is a no-op.
        """
        state = MigrationState()
        wb = _wb_rid(1)
        assets = [_asset_rid(1), _asset_rid(2)]

        state.record_pending_multi_asset_workbook(wb, assets)
        assert state.pending_multi_asset_workbooks[wb] == assets

        # Second call with different list overwrites (idempotent re-run behaviour)
        updated = [_asset_rid(1)]
        state.record_pending_multi_asset_workbook(wb, updated)
        assert state.pending_multi_asset_workbooks[wb] == updated

        state.clear_pending_multi_asset_workbook(wb)
        assert wb not in state.pending_multi_asset_workbooks

        # Clearing again is a no-op
        state.clear_pending_multi_asset_workbook(wb)

    def test_record_skip_accumulates_entries(self) -> None:
        """record_skip appends without overwriting; multiple skips for different resources are all kept."""
        state = MigrationState()

        state.record_skip(ResourceType.WORKBOOK, _wb_rid(1), "asset out of scope")
        state.record_skip(ResourceType.WORKBOOK, _wb_rid(2), "run not in state")

        assert len(state.skipped_resources) == 2
        reasons = {s.source_rid: s.reason for s in state.skipped_resources}
        assert reasons[_wb_rid(1)] == "asset out of scope"
        assert reasons[_wb_rid(2)] == "run not in state"

    def test_workbook_skip_is_terminal_for_pending_queues(self) -> None:
        """A workbook skip clears stale pending entries and prevents later queue attempts."""
        state = MigrationState()
        wb = _wb_rid(1)
        assets = [_asset_rid(1), _asset_rid(2)]
        runs = [_run_rid(1), _run_rid(2)]

        assert state.record_pending_multi_asset_workbook_unless_skipped(wb, assets) is True
        assert state.record_pending_multi_run_workbook_unless_skipped(wb, runs) is True

        assert state.record_workbook_skip_and_clear_pending(wb, "assets not in migration scope") is True
        assert wb not in state.pending_multi_asset_workbooks
        assert wb not in state.pending_multi_run_workbooks
        assert state.workbook_was_skipped(wb) is True

        assert state.record_pending_multi_asset_workbook_unless_skipped(wb, assets) is False
        assert state.record_pending_multi_run_workbook_unless_skipped(wb, runs) is False
        assert wb not in state.pending_multi_asset_workbooks
        assert wb not in state.pending_multi_run_workbooks

        assert state.record_workbook_skip_and_clear_pending(wb, "duplicate skip") is False
        assert len(state.skipped_resources) == 1
        assert state.skipped_resources[0].reason == "assets not in migration scope"

    def test_workbook_skip_replaces_retryable_reason_of_pending_workbook(self) -> None:
        """A terminal skip of a still-pending workbook replaces its waiting reason instead of keeping it."""
        state = MigrationState()
        wb = _wb_rid(1)
        state.record_pending_multi_run_workbook(wb, [_run_rid(1), _run_rid(2)])
        state.set_skip(ResourceType.WORKBOOK, wb, "waiting for runs not yet migrated")

        assert state.record_workbook_skip_and_clear_pending(wb, "assets not in migration scope") is True

        assert state.skipped_resources == [
            SkippedResource(ResourceType.WORKBOOK.value, wb, "assets not in migration scope")
        ]
        assert wb not in state.pending_multi_run_workbooks


# ---------------------------------------------------------------------------
# WorkbookMigrator._copy_from_impl (via copy_from)
# ---------------------------------------------------------------------------


class TestCopyFromImpl:
    def _make_migrator(self, **ctx_kwargs: Any) -> tuple[WorkbookMigrator, MigrationContext]:
        ctx = _make_context(**ctx_kwargs)
        return WorkbookMigrator(ctx), ctx

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    @patch.object(WorkbookMigrator, "_migrate_content_attachments")
    def test_asset_workbook_looks_up_rids_from_state(
        self, mock_migrate_attachments: MagicMock, mock_from_conjure: MagicMock, mock_clone: MagicMock
    ) -> None:
        """Multi-asset workbook: all mapped RIDs are read from migration state, clone called with
        full asset map as overrides, data_scope contains the new asset RIDs.
        """
        mock_migrate_attachments.side_effect = lambda source, content: content
        old_a1, old_a2 = _asset_rid(1), _asset_rid(2)
        new_a1, new_a2 = _asset_rid(101), _asset_rid(102)
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, new_a1)
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a2, new_a2)

        source = _stub_source_workbook(wb_src, asset_rids=[old_a1, old_a2])
        raw_nb = _stub_raw_notebook(labels=["lbl"], properties={"k": "v"})
        raw_nb.metadata.data_scope.asset_rids = [old_a1, old_a2]
        raw_nb.metadata.data_scope.run_rids = []
        source._clients.notebook.get.return_value = raw_nb

        new_wb = MagicMock()
        new_wb.rid = wb_dst
        mock_clone.return_value = (MagicMock(), MagicMock())
        mock_from_conjure.return_value = new_wb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        result = migrator.copy_from(
            source, WorkbookCopyOptions(source_to_destination_asset_rid_mapping={old_a1: new_a1, old_a2: new_a2})
        )

        _, kwargs = mock_clone.call_args
        assert kwargs["rid_overrides"] == {old_a1: new_a1, old_a2: new_a2}

        create_req = ctx.destination_client._clients.notebook.create.call_args[0][1]  # type: ignore[attr-defined]
        assert set(create_req.data_scope.asset_rids) == {new_a1, new_a2}
        assert create_req.data_scope.run_rids is None
        assert create_req.labels == ["lbl"]
        assert create_req.properties == {"k": "v"}

        assert ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, wb_src) == wb_dst
        assert result is new_wb

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    @patch.object(WorkbookMigrator, "_migrate_content_attachments")
    def test_run_workbook_looks_up_rids_from_state(
        self, mock_migrate_attachments: MagicMock, mock_from_conjure: MagicMock, mock_clone: MagicMock
    ) -> None:
        """Multi-run workbook: run RIDs looked up from state, asset map also included in overrides,
        data_scope contains the new run RIDs.
        """
        mock_migrate_attachments.side_effect = lambda source, content: content
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        new_r1, new_r2 = _run_rid(101), _run_rid(102)
        old_a1, new_a1 = _asset_rid(1), _asset_rid(101)
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, new_r1)
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r2, new_r2)
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, new_a1)

        source = _stub_source_workbook(wb_src, run_rids=[old_r1, old_r2])
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.run_rids = [old_r1, old_r2]
        raw_nb.metadata.data_scope.asset_rids = []
        source._clients.notebook.get.return_value = raw_nb

        new_wb = MagicMock()
        new_wb.rid = wb_dst
        mock_clone.return_value = (MagicMock(), MagicMock())
        mock_from_conjure.return_value = new_wb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        result = migrator.copy_from(
            source,
            WorkbookCopyOptions(
                source_to_destination_run_rid_mapping={old_r1: new_r1, old_r2: new_r2},
                source_to_destination_asset_rid_mapping={old_a1: new_a1},
            ),
        )

        _, kwargs = mock_clone.call_args
        assert kwargs["rid_overrides"] == {old_r1: new_r1, old_r2: new_r2, old_a1: new_a1}

        create_req = ctx.destination_client._clients.notebook.create.call_args[0][1]  # type: ignore[attr-defined]
        assert set(create_req.data_scope.run_rids) == {new_r1, new_r2}
        assert create_req.data_scope.asset_rids is None

        assert ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, wb_src) == wb_dst
        assert result is new_wb

    def test_missing_asset_rid_raises(self) -> None:
        """If a source asset RID is absent from migration state, ValueError is raised."""
        old_a1, old_a2 = _asset_rid(1), _asset_rid(2)
        wb_src = _wb_rid(1)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, _asset_rid(101))
        # old_a2 intentionally not mapped

        source = _stub_source_workbook(wb_src, asset_rids=[old_a1, old_a2])
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.asset_rids = [old_a1, old_a2]
        raw_nb.metadata.data_scope.run_rids = []
        source._clients.notebook.get.return_value = raw_nb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        with pytest.raises(ValueError, match=old_a2):
            migrator.copy_from(
                source, WorkbookCopyOptions(source_to_destination_asset_rid_mapping={old_a1: _asset_rid(101)})
            )

        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]

    def test_missing_run_rid_raises(self) -> None:
        """If a source run RID is absent from migration state, ValueError is raised."""
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        wb_src = _wb_rid(1)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, _run_rid(101))
        # old_r2 intentionally not mapped

        source = _stub_source_workbook(wb_src, run_rids=[old_r1, old_r2])
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.run_rids = [old_r1, old_r2]
        raw_nb.metadata.data_scope.asset_rids = []
        source._clients.notebook.get.return_value = raw_nb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        with pytest.raises(ValueError, match=old_r2):
            migrator.copy_from(
                source, WorkbookCopyOptions(source_to_destination_run_rid_mapping={old_r1: _run_rid(101)})
            )

        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    @patch.object(WorkbookMigrator, "_migrate_content_attachments")
    def test_archived_run_rids_are_dropped_from_scope(
        self,
        mock_migrate_attachments: MagicMock,
        mock_from_conjure: MagicMock,
        mock_clone: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A workbook referencing an archived run is migrated with only its unarchived runs,
        with a warning, instead of failing on the missing run mapping.
        """
        mock_migrate_attachments.side_effect = lambda source, content: content
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        new_r1 = _run_rid(101)
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, new_r1)
        # old_r2 has no mapping: it was skipped as archived

        source = _stub_source_workbook(wb_src, run_rids=[old_r1, old_r2])
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.run_rids = [old_r1, old_r2]
        raw_nb.metadata.data_scope.asset_rids = []
        source._clients.notebook.get.return_value = raw_nb

        new_wb = MagicMock()
        new_wb.rid = wb_dst
        mock_clone.return_value = (MagicMock(), MagicMock())
        mock_from_conjure.return_value = new_wb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        with caplog.at_level(logging.WARNING):
            result = migrator.copy_from(
                source,
                WorkbookCopyOptions(
                    source_to_destination_run_rid_mapping={old_r1: new_r1},
                    archived_run_rids=frozenset({old_r2}),
                ),
            )

        assert "archived run(s)" in caplog.text
        assert old_r2 in caplog.text

        create_req = ctx.destination_client._clients.notebook.create.call_args[0][1]  # type: ignore[attr-defined]
        assert create_req.data_scope.run_rids == [new_r1]
        assert create_req.data_scope.asset_rids is None
        assert result is new_wb

    def test_workbook_with_only_archived_runs_raises(self) -> None:
        """If every run in the workbook's scope is archived, copy_from raises instead of
        creating a workbook with an empty data scope.
        """
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        wb_src = _wb_rid(1)

        migrator, ctx = self._make_migrator()

        source = _stub_source_workbook(wb_src, run_rids=[old_r1, old_r2])
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.run_rids = [old_r1, old_r2]
        raw_nb.metadata.data_scope.asset_rids = []
        source._clients.notebook.get.return_value = raw_nb

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        with pytest.raises(ValueError, match="only archived runs"):
            migrator.copy_from(source, WorkbookCopyOptions(archived_run_rids=frozenset({old_r1, old_r2})))

        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]

    def test_idempotent_returns_existing_workbook_without_creating(self) -> None:
        """If the workbook is already in the migration state, the existing destination workbook
        is returned and no new notebook is created.
        """
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.WORKBOOK, wb_src, wb_dst)

        existing_wb = MagicMock()
        existing_wb.rid = wb_dst
        ctx.destination_client.get_workbook.return_value = existing_wb  # type: ignore[attr-defined]

        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        source = _stub_source_workbook(wb_src, asset_rids=[_asset_rid(1)])
        result = migrator.copy_from(
            source, WorkbookCopyOptions(source_to_destination_asset_rid_mapping={_asset_rid(1): _asset_rid(101)})
        )

        assert result is existing_wb
        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# WorkbookMigrator.migrate_deferred_workbooks
# ---------------------------------------------------------------------------


class TestMigrateDeferredWorkbooks:
    def test_empty_pending_is_noop(self) -> None:
        """When no workbooks are pending, no source clients are fetched and no copy methods are called."""
        ctx = _make_context()
        migrator = WorkbookMigrator(ctx)
        source_clients: dict[str, Any] = {}

        migrator.migrate_deferred_workbooks(source_clients)
        # Nothing to assert beyond no exceptions raised

    @patch.object(WorkbookMigrator, "copy_from")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_routes_pending_asset_and_run_workbooks(
        self,
        mock_from_conjure: MagicMock,
        mock_copy_from: MagicMock,
    ) -> None:
        """Pending asset and run workbooks are fetched from source clients and copy_from is called
        for each. Multi-run workbook falls back to first available client when no asset RID matches.
        """
        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        asset_rid_1 = _asset_rid(1)
        wb_asset = _wb_rid(1)
        wb_run = _wb_rid(2)
        run_rids = [_run_rid(1), _run_rid(2)]

        ctx = _make_context()
        ctx.migration_state.record_pending_multi_asset_workbook(wb_asset, [asset_rid_1])
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, run_rids)

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer src"
        raw_nb_asset = _stub_raw_notebook()
        raw_nb_run = _stub_raw_notebook()
        source_clients.notebook.get.side_effect = [raw_nb_asset, raw_nb_run]

        source_wb_asset = MagicMock()
        source_wb_run = MagicMock()
        mock_from_conjure.side_effect = [source_wb_asset, source_wb_run]

        migrator = WorkbookMigrator(ctx)
        migrator.migrate_deferred_workbooks({asset_rid_1: source_clients})

        # Both workbooks were fetched
        assert source_clients.notebook.get.call_count == 2
        source_clients.notebook.get.assert_any_call(source_clients.auth_header, wb_asset)
        source_clients.notebook.get.assert_any_call(source_clients.auth_header, wb_run)

        # copy_from called for each with explicit RID mappings built from migration state
        assert mock_copy_from.call_count == 2
        mock_copy_from.assert_any_call(source_wb_asset, WorkbookCopyOptions(source_to_destination_asset_rid_mapping={}))
        mock_copy_from.assert_any_call(
            source_wb_run,
            WorkbookCopyOptions(source_to_destination_asset_rid_mapping={}, source_to_destination_run_rid_mapping={}),
        )

    @patch.object(WorkbookMigrator, "copy_from")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_partially_archived_multi_run_workbook_migrates_with_archived_set(
        self, mock_from_conjure: MagicMock, mock_copy_from: MagicMock
    ) -> None:
        """A pending multi-run workbook with some archived runs is still migrated, passing the
        archived run set through so the copy drops those runs from the scope.
        """
        from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions

        wb_run = _wb_rid(1)
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        new_r1 = _run_rid(101)

        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [old_r1, old_r2])
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, new_r1)
        ctx.migration_state.record_archived_run(old_r2)

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer src"
        source_clients.notebook.get.return_value = _stub_raw_notebook()
        source_wb = MagicMock()
        mock_from_conjure.return_value = source_wb

        migrator = WorkbookMigrator(ctx)
        migrator.migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_from.assert_called_once_with(
            source_wb,
            WorkbookCopyOptions(
                source_to_destination_asset_rid_mapping={},
                source_to_destination_run_rid_mapping={old_r1: new_r1},
                archived_run_rids=frozenset({old_r2}),
            ),
        )
        assert wb_run not in ctx.migration_state.pending_multi_run_workbooks

    @patch.object(WorkbookMigrator, "copy_from")
    def test_fully_archived_multi_run_workbook_is_skipped(
        self, mock_copy_from: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A pending multi-run workbook whose runs are all archived is skipped with a warning
        and a recorded skip, not migrated and not fetched.
        """
        wb_run = _wb_rid(1)
        old_r1, old_r2 = _run_rid(1), _run_rid(2)

        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [old_r1, old_r2])
        ctx.migration_state.record_archived_run(old_r1)
        ctx.migration_state.record_archived_run(old_r2)

        source_clients = MagicMock()

        migrator = WorkbookMigrator(ctx)
        with caplog.at_level(logging.WARNING):
            migrator.migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_from.assert_not_called()
        source_clients.notebook.get.assert_not_called()
        assert "all of its runs are archived" in caplog.text
        assert ctx.migration_state.workbook_was_skipped(wb_run)
        assert wb_run not in ctx.migration_state.pending_multi_run_workbooks

    @staticmethod
    def _source_clients_serving(wb_rid: str, run_rids: list[str], mock_from_conjure: MagicMock) -> MagicMock:
        raw_nb = _stub_raw_notebook()
        raw_nb.metadata.data_scope.run_rids = run_rids
        raw_nb.metadata.data_scope.asset_rids = []
        source_clients = MagicMock()
        source_clients.notebook.get.return_value = raw_nb
        source = _stub_source_workbook(wb_rid, run_rids=run_rids)
        source._clients = source_clients
        mock_from_conjure.return_value = source
        return source_clients

    @pytest.mark.parametrize("earlier_skip", [None, "copy failed: Run RIDs not provided"], ids=["fresh", "resumed"])
    @patch.object(WorkbookMigrator, "_copy_workbook")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_multi_run_workbook_waits_for_unmigrated_run(
        self, mock_from_conjure: MagicMock, mock_copy_workbook: MagicMock, earlier_skip: str | None
    ) -> None:
        """A multi-run workbook with an unmigrated run waits, then copies once a later migration maps that run."""
        wb_run = _wb_rid(1)
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        mock_copy_workbook.return_value = MagicMock(rid=_wb_rid(101))

        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [old_r1, old_r2])
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, _run_rid(101))
        ctx.migration_state.set_skip(ResourceType.WORKBOOK, wb_run, earlier_skip)
        source_clients = self._source_clients_serving(wb_run, [old_r1, old_r2], mock_from_conjure)
        migrator = WorkbookMigrator(ctx)

        migrator.migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_workbook.assert_not_called()
        assert ctx.migration_state.skipped_resources == [
            SkippedResource(ResourceType.WORKBOOK.value, wb_run, f"waiting for runs not yet migrated: {[old_r2]}")
        ]
        assert wb_run in ctx.migration_state.pending_multi_run_workbooks

        ctx.migration_state.record_mapping(ResourceType.RUN, old_r2, _run_rid(102))
        migrator.migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_workbook.assert_called_once()
        assert ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, wb_run) == _wb_rid(101)
        assert ctx.migration_state.skipped_resources == []
        assert ctx.migration_state.pending_multi_run_workbooks == {}

    @patch.object(WorkbookMigrator, "_copy_workbook")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_multi_run_workbook_is_checked_against_its_current_runs(
        self, mock_from_conjure: MagicMock, mock_copy_workbook: MagicMock
    ) -> None:
        """A run removed from a workbook after it was queued does not keep the workbook waiting."""
        wb_run = _wb_rid(1)
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        mock_copy_workbook.return_value = MagicMock(rid=_wb_rid(101))

        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [old_r1, old_r2])
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, _run_rid(101))
        source_clients = self._source_clients_serving(wb_run, [old_r1], mock_from_conjure)

        WorkbookMigrator(ctx).migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_workbook.assert_called_once()
        assert ctx.migration_state.skipped_resources == []
        assert ctx.migration_state.pending_multi_run_workbooks == {}

    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_failed_workbook_is_skipped_and_later_workbooks_still_copy(self, _mock_from_conjure: MagicMock) -> None:
        """A permanent copy failure records a skip and keeps that workbook pending without blocking the next one."""
        wb_bad, wb_good = _wb_rid(1), _wb_rid(2)
        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_bad, [_run_rid(1), _run_rid(2)])
        ctx.migration_state.record_pending_multi_run_workbook(wb_good, [_run_rid(3), _run_rid(4)])
        source_clients = MagicMock()

        migrator = WorkbookMigrator(ctx)
        with patch.object(WorkbookMigrator, "copy_from", side_effect=[ValueError("Run RIDs not provided"), None]):
            migrator.migrate_deferred_workbooks({_asset_rid(1): source_clients})

        assert list(ctx.migration_state.pending_multi_run_workbooks) == [wb_bad]
        assert ctx.migration_state.workbook_was_skipped(wb_bad)
        assert not ctx.migration_state.workbook_was_skipped(wb_good)

    @patch.object(WorkbookMigrator, "copy_from")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_successful_copy_clears_earlier_copy_failure(
        self, _mock_from_conjure: MagicMock, _mock_copy_from: MagicMock
    ) -> None:
        """A workbook that failed on an earlier run drops out of the skip summary once it copies."""
        wb_run = _wb_rid(1)
        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [_run_rid(1), _run_rid(2)])
        ctx.migration_state.set_skip(ResourceType.WORKBOOK, wb_run, "copy failed: earlier run")

        WorkbookMigrator(ctx).migrate_deferred_workbooks({_asset_rid(1): MagicMock()})

        assert not ctx.migration_state.workbook_was_skipped(wb_run)
        assert ctx.migration_state.pending_multi_run_workbooks == {}

    @patch.object(WorkbookMigrator, "copy_from")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_transient_read_failure_is_retried(
        self, mock_from_conjure: MagicMock, mock_copy_from: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transient failure reading a deferred workbook is retried rather than skipping the workbook."""
        monkeypatch.setattr("time.sleep", lambda _seconds: None)
        wb_run = _wb_rid(1)
        ctx = _make_context()
        ctx.migration_state.record_pending_multi_run_workbook(wb_run, [_run_rid(1), _run_rid(2)])
        source_clients = MagicMock()
        source_clients.notebook.get.side_effect = [requests.exceptions.ConnectionError("reset"), _stub_raw_notebook()]

        WorkbookMigrator(ctx).migrate_deferred_workbooks({_asset_rid(1): source_clients})

        mock_copy_from.assert_called_once()
        assert ctx.migration_state.pending_multi_run_workbooks == {}
        assert not ctx.migration_state.workbook_was_skipped(wb_run)

    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_missing_source_client_for_asset_workbook_skips_gracefully(self, mock_from_conjure: MagicMock) -> None:
        """If none of a multi-asset workbook's asset RIDs are in source_clients_by_asset_rid,
        the workbook is skipped with a warning and no exception is raised.
        """
        wb_asset = _wb_rid(1)
        ctx = _make_context()
        ctx.migration_state.record_pending_multi_asset_workbook(wb_asset, [_asset_rid(99)])

        migrator = WorkbookMigrator(ctx)
        # Empty client map — asset_rid(99) not present
        migrator.migrate_deferred_workbooks({})

        mock_from_conjure.assert_not_called()


# ---------------------------------------------------------------------------
# Runners flush deferred workbooks after every asset has migrated
# ---------------------------------------------------------------------------


def _run_sequential(runner: MigrationRunner) -> None:
    runner.run_migration()


def _run_parallel(runner: MigrationRunner) -> None:
    run_parallel_migration(runner, max_workers=2)


@pytest.mark.parametrize("run", [_run_sequential, _run_parallel], ids=["sequential", "parallel"])
@patch.object(WorkbookMigrator, "copy_from")
@patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
def test_runner_migrates_deferred_workbooks(
    mock_from_conjure: MagicMock,
    mock_copy_from: MagicMock,
    run: Callable[[MigrationRunner], None],
    tmp_path: Path,
) -> None:
    """Workbooks deferred during asset migration are copied before the runner returns, on both runners."""
    asset_rid = _asset_rid(1)
    wb_asset, wb_run = _wb_rid(1), _wb_rid(2)
    source_asset = MagicMock(rid=asset_rid)
    source_asset._clients.notebook.get.return_value = _stub_raw_notebook()

    def queue_deferred_workbooks(self: AssetMigrator, source: Any, options: Any) -> None:
        self.ctx.migration_state.record_pending_multi_asset_workbook_unless_skipped(wb_asset, [asset_rid])
        self.ctx.migration_state.record_pending_multi_run_workbook_unless_skipped(wb_run, [_run_rid(1), _run_rid(2)])

    state_path = tmp_path / "state.json"
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={asset_rid: AssetResources(asset=source_asset, source_workbook_templates=[])},
            source_standalone_templates=[],
            source_standalone_checklists=[],
        ),
        dataset_config=MigrationDatasetConfig(include_dataset_files=False, preserve_dataset_uuid=True),
        destination_client=MagicMock(),
        migration_state_path=state_path,
    )

    with patch.object(AssetMigrator, "copy_from", autospec=True, side_effect=queue_deferred_workbooks):
        run(runner)

    fetched_rids = {call.args[1] for call in source_asset._clients.notebook.get.call_args_list}
    assert fetched_rids == {wb_asset, wb_run}
    assert mock_copy_from.call_count == 2
    saved_state = json.loads(state_path.read_text())
    assert saved_state["pending_multi_asset_workbooks"] == {}
    assert saved_state["pending_multi_run_workbooks"] == {}
