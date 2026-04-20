"""Tests for AssetMigrator attachment migration."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, call, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STACK = "cerulean-staging"


def _att_rid(n: int) -> str:
    hex8 = f"{n:08x}"
    return f"ri.attachments.{_STACK}.attachment.{hex8}-0000-0000-0000-000000000000"


def _asset_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.asset.{n:08x}-0000-0000-0000-000000000000"


def _run_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.run.{n:08x}-0000-0000-0000-000000000000"


def _wb_rid(n: int) -> str:
    return f"ri.scout.{_STACK}.notebook.{n:08x}-0000-0000-0000-000000000000"


def _make_context(source_asset_rids: frozenset[str] = frozenset()) -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(
        destination_client=mock_client,
        migration_state=MigrationState(),
        source_asset_rids=source_asset_rids,
    )


def _make_source_asset(
    rid: str = "source-asset-rid",
    name: str = "Source Asset",
    attachments: list[MagicMock] | None = None,
) -> MagicMock:
    asset = MagicMock()
    asset.rid = rid
    asset.name = name
    asset.description = "A description"
    asset.properties = {}
    asset.labels = []
    asset.list_attachments.return_value = attachments or []
    asset.search_workbooks.return_value = []
    return asset


def _make_dest_asset(rid: str = "dest-asset-rid") -> MagicMock:
    asset = MagicMock()
    asset.rid = rid
    return asset


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAssetMigratorAttachments:
    def _make_migrator(self) -> tuple[AssetMigrator, MigrationContext]:
        ctx = _make_context()
        return AssetMigrator(ctx), ctx

    def test_default_copy_options_includes_attachments(self) -> None:
        """default_copy_options enables attachment migration."""
        migrator, _ = self._make_migrator()
        assert migrator.default_copy_options().include_attachments is True

    @patch("nominal.experimental.migration.migrator.asset_migrator.AttachmentMigrator")
    def test_include_attachments_false_skips_migration(self, mock_att_cls: MagicMock) -> None:
        """When include_attachments=False, AttachmentMigrator is never instantiated."""
        migrator, ctx = self._make_migrator()

        source_att = MagicMock()
        source_att.rid = _att_rid(1)
        source_asset = _make_source_asset(attachments=[source_att])

        dest_asset = _make_dest_asset()
        ctx.destination_client.create_asset.return_value = dest_asset

        migrator.copy_from(source_asset, AssetCopyOptions(include_attachments=False))

        mock_att_cls.assert_not_called()
        dest_asset.add_attachments.assert_not_called()

    @patch("nominal.experimental.migration.migrator.asset_migrator.AttachmentMigrator")
    def test_include_attachments_true_migrates_each_attachment(self, mock_att_cls: MagicMock) -> None:
        """When include_attachments=True, each source attachment is migrated via AttachmentMigrator."""
        migrator, ctx = self._make_migrator()

        old_rid_1 = _att_rid(1)
        old_rid_2 = _att_rid(2)
        new_rid_1 = _att_rid(101)
        new_rid_2 = _att_rid(102)

        source_att_1 = MagicMock()
        source_att_1.rid = old_rid_1
        source_att_2 = MagicMock()
        source_att_2.rid = old_rid_2

        source_asset = _make_source_asset(attachments=[source_att_1, source_att_2])

        dest_asset = _make_dest_asset()
        ctx.destination_client.create_asset.return_value = dest_asset

        new_att_1 = MagicMock()
        new_att_1.rid = new_rid_1
        new_att_2 = MagicMock()
        new_att_2.rid = new_rid_2

        mock_att_migrator = mock_att_cls.return_value
        mock_att_migrator.copy_from.side_effect = [new_att_1, new_att_2]

        migrator.copy_from(source_asset, AssetCopyOptions(include_attachments=True))

        mock_att_migrator.copy_from.assert_has_calls([call(source_att_1), call(source_att_2)])
        dest_asset.add_attachments.assert_called_once_with([new_att_1, new_att_2])

    @patch("nominal.experimental.migration.migrator.asset_migrator.AttachmentMigrator")
    def test_include_attachments_true_no_source_attachments_skips_add(self, mock_att_cls: MagicMock) -> None:
        """When include_attachments=True but source has no attachments, add_attachments is not called."""
        migrator, ctx = self._make_migrator()

        source_asset = _make_source_asset(attachments=[])
        dest_asset = _make_dest_asset()
        ctx.destination_client.create_asset.return_value = dest_asset

        migrator.copy_from(source_asset, AssetCopyOptions(include_attachments=True))

        mock_att_cls.return_value.copy_from.assert_not_called()
        dest_asset.add_attachments.assert_not_called()

    @patch("nominal.experimental.migration.migrator.asset_migrator.AttachmentMigrator")
    def test_include_attachments_records_mapping_in_shared_state(self, mock_att_cls: MagicMock) -> None:
        """AttachmentMigrator is constructed with the same migration state so mappings are shared."""
        migrator, ctx = self._make_migrator()

        source_att = MagicMock()
        source_att.rid = _att_rid(1)
        source_asset = _make_source_asset(attachments=[source_att])

        dest_asset = _make_dest_asset()
        ctx.destination_client.create_asset.return_value = dest_asset

        new_att = MagicMock()
        new_att.rid = _att_rid(101)
        mock_att_cls.return_value.copy_from.return_value = new_att

        migrator.copy_from(source_asset, AssetCopyOptions(include_attachments=True))

        # AttachmentMigrator was constructed with a context that shares the same migration_state
        init_ctx = mock_att_cls.call_args[0][0]
        assert init_ctx.migration_state is ctx.migration_state


# ---------------------------------------------------------------------------
# Workbook routing: _copy_asset_and_run_workbooks
# ---------------------------------------------------------------------------


def _stub_workbook(rid: str, asset_rids: list[str] | None = None, run_rids: list[str] | None = None) -> MagicMock:
    wb = MagicMock()
    wb.rid = rid
    wb.asset_rids = asset_rids
    wb.run_rids = run_rids
    return wb


class TestAssetMigratorWorkbookRouting:
    """Tests for _copy_asset_and_run_workbooks: single vs. multi routing and scope checks."""

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_routing_single_asset_multi_asset_and_out_of_scope(self, mock_wm_cls: MagicMock) -> None:
        """Verifies all routing branches in one pass:
        - single-asset workbook → copy_from called
        - multi-asset workbook with all assets in source_asset_rids → pending
        - multi-asset workbook with all assets already in migration state (prior run) → pending
        - multi-asset workbook with one asset completely out of scope → skip recorded
        - workbook with no asset_rids → ignored
        """
        a1, a2, a3, a_out = _asset_rid(1), _asset_rid(2), _asset_rid(3), _asset_rid(99)
        new_a3 = _asset_rid(103)
        wb_single = _wb_rid(1)
        wb_multi_in_scope = _wb_rid(2)
        wb_multi_prior_run = _wb_rid(3)
        wb_multi_missing = _wb_rid(4)
        wb_no_rids = _wb_rid(5)

        # a1, a2 are in the current migration config; a3 was migrated in a prior run
        ctx = _make_context(source_asset_rids=frozenset([a1, a2]))
        ctx.migration_state.record_mapping(ResourceType.ASSET, a3, new_a3)

        source_asset = _make_source_asset()
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = [
            _stub_workbook(wb_single, asset_rids=[a1]),  # single → copy_from
            _stub_workbook(wb_multi_in_scope, asset_rids=[a1, a2]),  # all in config → pending
            _stub_workbook(wb_multi_prior_run, asset_rids=[a1, a3]),  # a3 in state → pending
            _stub_workbook(wb_multi_missing, asset_rids=[a1, a_out]),  # a_out missing → skip
            _stub_workbook(wb_no_rids, asset_rids=None),  # no rids → ignored
        ]
        source_asset.list_runs.return_value = []

        migrator = AssetMigrator(ctx)
        migrator._copy_asset_and_run_workbooks(source_asset, new_asset, include_runs=False)

        mock_wm = mock_wm_cls.return_value
        mock_wm.copy_from.assert_called_once()

        state = ctx.migration_state
        assert wb_multi_in_scope in state.pending_multi_asset_workbooks
        assert state.pending_multi_asset_workbooks[wb_multi_in_scope] == [a1, a2]

        assert wb_multi_prior_run in state.pending_multi_asset_workbooks
        assert state.pending_multi_asset_workbooks[wb_multi_prior_run] == [a1, a3]

        assert wb_multi_missing not in state.pending_multi_asset_workbooks
        assert len(state.skipped_resources) == 1
        assert a_out in state.skipped_resources[0].reason

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_multi_run_workbook_always_enqueued_single_run_uses_copy_from(self, mock_wm_cls: MagicMock) -> None:
        """Single-run workbooks go to copy_from; multi-run workbooks are always enqueued
        for deferred migration without an upfront scope check. Also verifies that finding
        the same multi-run workbook via two different runs overwrites the pending entry
        idempotently.
        """
        r1, r2 = _run_rid(1), _run_rid(2)
        new_r1, new_r2 = _run_rid(101), _run_rid(102)
        wb_single_run = _wb_rid(1)
        wb_multi_run = _wb_rid(2)

        ctx = _make_context()
        ctx.migration_state.record_mapping(ResourceType.RUN, r1, new_r1)
        ctx.migration_state.record_mapping(ResourceType.RUN, r2, new_r2)

        source_asset = _make_source_asset()
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = []

        run1 = MagicMock()
        run1.rid = r1
        run1.search_workbooks.return_value = [
            _stub_workbook(wb_single_run, run_rids=[r1]),
            _stub_workbook(wb_multi_run, run_rids=[r1, r2]),
        ]
        run2 = MagicMock()
        run2.rid = r2
        # wb_multi_run found again via run2 — should overwrite pending, not duplicate
        run2.search_workbooks.return_value = [
            _stub_workbook(wb_multi_run, run_rids=[r1, r2]),
        ]
        source_asset.list_runs.return_value = [run1, run2]

        dest_run1 = MagicMock()
        ctx.destination_client.get_run.return_value = dest_run1

        migrator = AssetMigrator(ctx)
        migrator._copy_asset_and_run_workbooks(source_asset, new_asset, include_runs=True)

        mock_wm = mock_wm_cls.return_value
        assert mock_wm.copy_from.call_count == 1  # only wb_single_run

        state = ctx.migration_state
        assert wb_multi_run in state.pending_multi_run_workbooks
        assert state.pending_multi_run_workbooks[wb_multi_run] == [r1, r2]
        assert len(state.pending_multi_run_workbooks) == 1  # not duplicated
