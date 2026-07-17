"""Tests for AssetMigrator attachment migration."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, call, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.dry_run import would_create_message
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
        ctx.destination_client.create_asset.return_value = dest_asset  # type: ignore[attr-defined]

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
        ctx.destination_client.create_asset.return_value = dest_asset  # type: ignore[attr-defined]

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
        ctx.destination_client.create_asset.return_value = dest_asset  # type: ignore[attr-defined]

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
        ctx.destination_client.create_asset.return_value = dest_asset  # type: ignore[attr-defined]

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

    # ---------------------------------------------------------------------------
    # Workbook allowlist
    # ---------------------------------------------------------------------------

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_allowlist_none_copies_all_asset_workbooks(self, mock_wm_cls: MagicMock) -> None:
        """When allowlist is None, all single-asset workbooks are copied (default behaviour preserved)."""
        a1 = _asset_rid(1)
        wb1, wb2 = _wb_rid(1), _wb_rid(2)

        ctx = _make_context(source_asset_rids=frozenset([a1]))
        source_asset = _make_source_asset(rid=a1)
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = [
            _stub_workbook(wb1, asset_rids=[a1]),
            _stub_workbook(wb2, asset_rids=[a1]),
        ]
        source_asset.list_runs.return_value = []

        AssetMigrator(ctx)._copy_asset_and_run_workbooks(
            source_asset, new_asset, include_runs=False, workbook_rids_allowlist=None
        )

        assert mock_wm_cls.return_value.copy_from.call_count == 2

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_allowlist_skips_non_allowlisted_asset_workbooks(self, mock_wm_cls: MagicMock) -> None:
        """Only workbooks whose RID is in the allowlist are copied; others are silently skipped."""
        a1 = _asset_rid(1)
        wb1, wb2 = _wb_rid(1), _wb_rid(2)

        ctx = _make_context(source_asset_rids=frozenset([a1]))
        source_asset = _make_source_asset(rid=a1)
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = [
            _stub_workbook(wb1, asset_rids=[a1]),
            _stub_workbook(wb2, asset_rids=[a1]),
        ]
        source_asset.list_runs.return_value = []

        AssetMigrator(ctx)._copy_asset_and_run_workbooks(
            source_asset, new_asset, include_runs=False, workbook_rids_allowlist=frozenset([wb1])
        )

        mock_wm = mock_wm_cls.return_value
        assert mock_wm.copy_from.call_count == 1
        assert mock_wm.copy_from.call_args[0][0].rid == wb1

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_allowlist_empty_skips_all_workbooks(self, mock_wm_cls: MagicMock) -> None:
        """An empty frozenset allowlist causes all workbooks to be skipped."""
        a1 = _asset_rid(1)

        ctx = _make_context(source_asset_rids=frozenset([a1]))
        source_asset = _make_source_asset(rid=a1)
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = [
            _stub_workbook(_wb_rid(1), asset_rids=[a1]),
            _stub_workbook(_wb_rid(2), asset_rids=[a1]),
        ]
        source_asset.list_runs.return_value = []

        AssetMigrator(ctx)._copy_asset_and_run_workbooks(
            source_asset, new_asset, include_runs=False, workbook_rids_allowlist=frozenset()
        )

        mock_wm_cls.return_value.copy_from.assert_not_called()

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_allowlist_applies_to_run_workbooks(self, mock_wm_cls: MagicMock) -> None:
        """Allowlist filters run-level workbooks with the same logic as asset-level workbooks."""
        a1 = _asset_rid(1)
        r1 = _run_rid(1)
        new_r1 = _run_rid(101)
        wb_asset = _wb_rid(1)  # allowlisted
        wb_run_allowed = _wb_rid(2)  # allowlisted
        wb_run_blocked = _wb_rid(3)  # not allowlisted

        ctx = _make_context(source_asset_rids=frozenset([a1]))
        ctx.migration_state.record_mapping(ResourceType.RUN, r1, new_r1)

        source_asset = _make_source_asset(rid=a1)
        new_asset = _make_dest_asset()
        source_asset.search_workbooks.return_value = [_stub_workbook(wb_asset, asset_rids=[a1])]

        run1 = MagicMock()
        run1.rid = r1
        run1.search_workbooks.return_value = [
            _stub_workbook(wb_run_allowed, run_rids=[r1]),
            _stub_workbook(wb_run_blocked, run_rids=[r1]),
        ]
        source_asset.list_runs.return_value = [run1]

        AssetMigrator(ctx)._copy_asset_and_run_workbooks(
            source_asset,
            new_asset,
            include_runs=True,
            workbook_rids_allowlist=frozenset([wb_asset, wb_run_allowed]),
        )

        mock_wm = mock_wm_cls.return_value
        assert mock_wm.copy_from.call_count == 2
        copied_rids = {c[0][0].rid for c in mock_wm.copy_from.call_args_list}
        assert copied_rids == {wb_asset, wb_run_allowed}

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_multi_run_workbook_always_enqueued_single_run_uses_copy_from(self, mock_wm_cls: MagicMock) -> None:
        """Single-run workbooks (run owned by exactly one asset) go to copy_from; multi-run
        workbooks are always enqueued for deferred migration without an upfront scope check.
        Also verifies that finding the same multi-run workbook via two different runs overwrites
        the pending entry idempotently.
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
        run1.assets = [source_asset.rid]  # single owning asset
        run1.search_workbooks.return_value = [
            _stub_workbook(wb_single_run, run_rids=[r1]),
            _stub_workbook(wb_multi_run, run_rids=[r1, r2]),
        ]
        run2 = MagicMock()
        run2.rid = r2
        run2.assets = [source_asset.rid, _asset_rid(2)]
        # wb_multi_run found again via run2 — should overwrite pending, not duplicate
        run2.search_workbooks.return_value = [
            _stub_workbook(wb_multi_run, run_rids=[r1, r2]),
        ]
        source_asset.list_runs.return_value = [run1, run2]

        dest_run1 = MagicMock()
        ctx.destination_client.get_run.return_value = dest_run1  # type: ignore[attr-defined]

        migrator = AssetMigrator(ctx)
        migrator._copy_asset_and_run_workbooks(source_asset, new_asset, include_runs=True)

        mock_wm = mock_wm_cls.return_value
        assert mock_wm.copy_from.call_count == 1  # only wb_single_run

        state = ctx.migration_state
        assert wb_multi_run in state.pending_multi_run_workbooks
        assert state.pending_multi_run_workbooks[wb_multi_run] == [r1, r2]
        assert len(state.pending_multi_run_workbooks) == 1  # not duplicated

    @patch("nominal.experimental.migration.migrator.asset_migrator.WorkbookMigrator")
    def test_single_run_workbook_owned_by_multiple_assets_is_deferred(self, mock_wm_cls: MagicMock) -> None:
        """A workbook scoped to a *single* run (NotebookDataScope.run_rids == [X]) must still be
        deferred if run X itself is owned by more than one asset. Copying it immediately — using
        only the one asset mapping known at this point in the migration — would build an
        incomplete RID override map: everywhere the workbook's content/layout/state references
        the *other* owning asset(s), the RID-clone step has no override for them and silently
        regenerates a fresh, unmapped UUID (same stack prefix) instead of leaving them for a
        later, complete pass. Deferring (like the already-handled multi-asset and multi-run
        cases) ensures the workbook is only copied once every asset it depends on is mapped.
        """
        a1, a2 = _asset_rid(1), _asset_rid(2)
        new_a1 = _asset_rid(101)
        r1 = _run_rid(1)
        new_r1 = _run_rid(101)
        wb_run_scoped = _wb_rid(1)

        ctx = _make_context()
        ctx.migration_state.record_mapping(ResourceType.RUN, r1, new_r1)
        # Only a1 has been mapped so far; a2 (the run's other owning asset) has not.
        ctx.migration_state.record_mapping(ResourceType.ASSET, a1, new_a1)

        source_asset = _make_source_asset(rid=a1)
        new_asset = _make_dest_asset(rid=new_a1)
        source_asset.search_workbooks.return_value = []

        run1 = MagicMock()
        run1.rid = r1
        run1.assets = [a1, a2]  # run is owned by two assets
        run1.search_workbooks.return_value = [
            _stub_workbook(wb_run_scoped, run_rids=[r1]),  # single-run scope on the workbook itself
        ]
        source_asset.list_runs.return_value = [run1]

        migrator = AssetMigrator(ctx)
        migrator._copy_asset_and_run_workbooks(source_asset, new_asset, include_runs=True)

        mock_wm = mock_wm_cls.return_value
        mock_wm.copy_from.assert_not_called()

        state = ctx.migration_state
        assert wb_run_scoped in state.pending_multi_run_workbooks


# ---------------------------------------------------------------------------
# Dry-run behavior
# ---------------------------------------------------------------------------


def _make_dry_run_context(source_asset_rids: frozenset[str] = frozenset()) -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(
        destination_client=mock_client,
        migration_state=MigrationState(),
        source_asset_rids=source_asset_rids,
        dry_run=True,
    )


_NO_CHILD_OPTIONS = AssetCopyOptions(
    dataset_config=None,
    include_attachments=False,
    include_events=False,
    include_runs=False,
    include_video=False,
    include_checklists=False,
    include_workbooks=False,
)


class TestAssetMigratorDryRun:
    def test_dry_run_preserves_real_prior_mapping(self) -> None:
        """_copy_from_impl must not overwrite a real prior asset mapping with a placeholder in dry_run."""
        src_rid = _asset_rid(1)
        dest_rid = _asset_rid(100)
        ctx = _make_dry_run_context()
        ctx.migration_state.record_mapping(ResourceType.ASSET, src_rid, dest_rid)

        migrator = AssetMigrator(ctx)
        migrator.copy_from(_make_source_asset(rid=src_rid), _NO_CHILD_OPTIONS)

        assert ctx.migration_state.get_mapped_rid(ResourceType.ASSET, src_rid) == dest_rid

    def test_dry_run_does_not_call_create_asset(self) -> None:
        """In dry_run, destination_client.create_asset must never be called."""
        ctx = _make_dry_run_context()
        migrator = AssetMigrator(ctx)
        migrator.copy_from(_make_source_asset(), _NO_CHILD_OPTIONS)
        ctx.destination_client.create_asset.assert_not_called()  # type: ignore[attr-defined]

    def test_dry_run_logs_would_create_for_new_asset(self, caplog: pytest.LogCaptureFixture) -> None:
        """New assets in dry_run should produce a '[DRY RUN] Would create asset' log line."""
        import logging

        ctx = _make_dry_run_context()
        migrator = AssetMigrator(ctx)
        source_asset = _make_source_asset(name="MyNewAsset")
        with caplog.at_level(logging.INFO):
            migrator.copy_from(source_asset, _NO_CHILD_OPTIONS)
        expected = would_create_message(ResourceType.ASSET) % (source_asset.name, source_asset.rid)
        assert any(expected in r.getMessage() for r in caplog.records)
