"""Tests for multi-asset/run workbook migration: copy methods, deferred routing, and migration state."""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_migrator import WorkbookMigrator
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


# ---------------------------------------------------------------------------
# WorkbookMigrator.copy_multi_asset_workbook
# ---------------------------------------------------------------------------


class TestCopyMultiAssetWorkbook:
    def _make_migrator(self, **ctx_kwargs: Any) -> tuple[WorkbookMigrator, MigrationContext]:
        ctx = _make_context(**ctx_kwargs)
        return WorkbookMigrator(ctx), ctx

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_happy_path_remaps_rids_records_state_and_copies_metadata(
        self, mock_from_conjure: MagicMock, mock_clone: MagicMock
    ) -> None:
        """All assets present: RID map is built correctly, clone called with overrides,
        notebook created with remapped data_scope, mapping recorded, pending cleared,
        labels/properties copied from source.
        """
        old_a1, old_a2 = _asset_rid(1), _asset_rid(2)
        new_a1, new_a2 = _asset_rid(101), _asset_rid(102)
        wb_src = _wb_rid(1)
        wb_dst = _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, new_a1)
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a2, new_a2)
        ctx.migration_state.record_pending_multi_asset_workbook(wb_src, [old_a1, old_a2])

        source = _stub_source_workbook(wb_src, asset_rids=[old_a1, old_a2])
        raw_nb = _stub_raw_notebook(labels=["lbl"], properties={"k": "v"})
        source._clients.notebook.get.return_value = raw_nb

        new_layout, new_content = MagicMock(), MagicMock()
        mock_clone.return_value = (new_layout, new_content)
        new_wb = MagicMock()
        new_wb.rid = wb_dst
        mock_from_conjure.return_value = new_wb

        result = migrator.copy_multi_asset_workbook(source, [old_a1, old_a2])

        # clone called with the correct RID overrides
        mock_clone.assert_called_once()
        _, kwargs = mock_clone.call_args
        assert kwargs["rid_overrides"] == {old_a1: new_a1, old_a2: new_a2}

        # notebook created with new asset RIDs in data_scope
        create_req = ctx.destination_client._clients.notebook.create.call_args[0][1]  # type: ignore[attr-defined]
        assert set(create_req.data_scope.asset_rids) == {new_a1, new_a2}
        assert create_req.data_scope.run_rids is None

        # state: mapping recorded, pending cleared
        assert ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, wb_src) == wb_dst
        assert wb_src not in ctx.migration_state.pending_multi_asset_workbooks

        # metadata copied
        new_wb.update.assert_called_once_with(labels=["lbl"], properties={"k": "v"})
        assert result is new_wb

    def test_missing_assets_returns_none_and_records_skip(self) -> None:
        """When one or more asset RIDs are absent from the migration state, returns None
        and records a skip entry; no notebook is created.
        """
        old_a1, old_a2 = _asset_rid(1), _asset_rid(2)
        new_a1 = _asset_rid(101)
        # old_a2 intentionally not mapped

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, new_a1)

        source = _stub_source_workbook(_wb_rid(1), asset_rids=[old_a1, old_a2])
        source._clients.notebook.get.return_value = _stub_raw_notebook()

        result = migrator.copy_multi_asset_workbook(source, [old_a1, old_a2])

        assert result is None
        assert len(ctx.migration_state.skipped_resources) == 1
        assert old_a2 in ctx.migration_state.skipped_resources[0].reason
        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_idempotent_returns_existing_workbook_without_creating(
        self, mock_from_conjure: MagicMock, mock_clone: MagicMock
    ) -> None:
        """If the workbook is already in the migration state, the existing destination workbook
        is returned and no new notebook is created.
        """
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.WORKBOOK, wb_src, wb_dst)

        existing_wb = MagicMock()
        existing_wb.rid = wb_dst
        ctx.destination_client.get_workbook.return_value = existing_wb  # type: ignore[attr-defined]

        source = _stub_source_workbook(wb_src, asset_rids=[_asset_rid(1)])
        result = migrator.copy_multi_asset_workbook(source, [_asset_rid(1)])

        assert result is existing_wb
        mock_clone.assert_not_called()
        ctx.destination_client._clients.notebook.create.assert_not_called()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# WorkbookMigrator.copy_multi_run_workbook
# ---------------------------------------------------------------------------


class TestCopyMultiRunWorkbook:
    def _make_migrator(self) -> tuple[WorkbookMigrator, MigrationContext]:
        ctx = _make_context()
        return WorkbookMigrator(ctx), ctx

    @patch("nominal.experimental.migration.migrator.workbook_migrator.clone_conjure_objects_with_rid_overrides")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_happy_path_remaps_run_rids(self, mock_from_conjure: MagicMock, mock_clone: MagicMock) -> None:
        """All runs present: RID map built, data_scope has new run RIDs, mapping recorded, pending cleared."""
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        new_r1, new_r2 = _run_rid(101), _run_rid(102)
        wb_src, wb_dst = _wb_rid(1), _wb_rid(100)

        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, new_r1)
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r2, new_r2)
        ctx.migration_state.record_pending_multi_run_workbook(wb_src, [old_r1, old_r2])

        source = _stub_source_workbook(wb_src, run_rids=[old_r1, old_r2])
        source._clients.notebook.get.return_value = _stub_raw_notebook()
        mock_clone.return_value = (MagicMock(), MagicMock())
        new_wb = MagicMock()
        new_wb.rid = wb_dst
        mock_from_conjure.return_value = new_wb

        # Also record a migrated asset to verify it's included in rid_overrides
        old_a1 = _asset_rid(1)
        new_a1 = _asset_rid(101)
        ctx.migration_state.record_mapping(ResourceType.ASSET, old_a1, new_a1)

        result = migrator.copy_multi_run_workbook(source, [old_r1, old_r2])

        _, kwargs = mock_clone.call_args
        assert kwargs["rid_overrides"] == {old_r1: new_r1, old_r2: new_r2, old_a1: new_a1}

        create_req = ctx.destination_client._clients.notebook.create.call_args[0][1]  # type: ignore[attr-defined]
        assert set(create_req.data_scope.run_rids) == {new_r1, new_r2}
        assert create_req.data_scope.asset_rids is None

        assert ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, wb_src) == wb_dst
        assert wb_src not in ctx.migration_state.pending_multi_run_workbooks
        assert result is new_wb

    def test_missing_run_returns_none_and_records_skip(self) -> None:
        """A run RID absent from the migration state causes the workbook to be skipped."""
        old_r1, old_r2 = _run_rid(1), _run_rid(2)
        migrator, ctx = self._make_migrator()
        ctx.migration_state.record_mapping(ResourceType.RUN, old_r1, _run_rid(101))
        # old_r2 not mapped

        source = _stub_source_workbook(_wb_rid(1), run_rids=[old_r1, old_r2])
        source._clients.notebook.get.return_value = _stub_raw_notebook()

        result = migrator.copy_multi_run_workbook(source, [old_r1, old_r2])

        assert result is None
        assert len(ctx.migration_state.skipped_resources) == 1
        assert old_r2 in ctx.migration_state.skipped_resources[0].reason
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

    @patch.object(WorkbookMigrator, "copy_multi_run_workbook")
    @patch.object(WorkbookMigrator, "copy_multi_asset_workbook")
    @patch("nominal.experimental.migration.migrator.workbook_migrator.Workbook._from_conjure")
    def test_routes_pending_asset_and_run_workbooks(
        self,
        mock_from_conjure: MagicMock,
        mock_copy_asset: MagicMock,
        mock_copy_run: MagicMock,
    ) -> None:
        """Pending asset and run workbooks are fetched from source clients and routed to the
        correct copy method. The correct source_asset_rids / source_run_rids are forwarded.
        Multi-run workbook falls back to first available client when no asset RID matches.
        """
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

        # Each routed to the correct copy method with the right RID lists
        mock_copy_asset.assert_called_once_with(source_wb_asset, [asset_rid_1])
        mock_copy_run.assert_called_once_with(source_wb_run, run_rids)

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
