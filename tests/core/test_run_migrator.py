"""Tests for RunMigrator, focusing on multi-asset run migration behavior."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.run_migrator import RunCopyOptions, RunMigrator
from nominal.experimental.migration.resource_type import ResourceType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STACK = "cerulean-staging"


def _run_rid(n: int) -> str:
    hex8 = f"{n:08x}"
    return f"ri.scout.{_STACK}.run.{hex8}-0000-0000-0000-000000000000"


def _asset_rid(n: int) -> str:
    hex8 = f"{n:08x}"
    return f"ri.scout.{_STACK}.asset.{hex8}-0000-0000-0000-000000000000"


def _att_rid(n: int) -> str:
    hex8 = f"{n:08x}"
    return f"ri.attachments.{_STACK}.attachment.{hex8}-0000-0000-0000-000000000000"


def _make_run(rid: str, name: str = "Run", asset_rids: list[str] | None = None) -> MagicMock:
    run = MagicMock()
    run.rid = rid
    run.name = name
    run.assets = asset_rids or []
    run.start = 0
    run.end = None
    run.description = ""
    run.properties = {}
    run.labels = []
    run.links = []
    run.list_attachments.return_value = []
    # update() returns a new mock with updated assets by default - tests can override
    run.update.return_value = run
    return run


def _make_context() -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(destination_client=mock_client, migration_state=MigrationState())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRunMigratorNewRun:
    def test_creates_run_with_new_asset(self) -> None:
        """When the run hasn't been migrated yet, create_run is called with the given asset."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_run = _make_run(_run_rid(1))
        dest_asset_rid = _asset_rid(10)

        dest_run = _make_run(_run_rid(100), asset_rids=[dest_asset_rid])
        ctx.destination_client.create_run.return_value = dest_run

        result = migrator.copy_from(source_run, RunCopyOptions(new_assets=[dest_asset_rid]))

        ctx.destination_client.create_run.assert_called_once()
        assert result.rid == _run_rid(100)
        assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, _run_rid(1)) == _run_rid(100)

    def test_mapping_recorded_after_creation(self) -> None:
        """The old→new RID mapping is recorded in migration state after creation."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_run = _make_run(_run_rid(1))
        dest_run = _make_run(_run_rid(100))
        ctx.destination_client.create_run.return_value = dest_run

        migrator.copy_from(source_run, RunCopyOptions())

        assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, _run_rid(1)) == _run_rid(100)


class TestRunMigratorExistingRun:
    def test_adds_missing_asset_to_existing_run(self) -> None:
        """When a run was already migrated, a new asset that isn't on it yet is added via update()."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid = _run_rid(1)
        dest_rid = _run_rid(100)
        asset_a_rid = _asset_rid(10)  # already on the run
        asset_b_rid = _asset_rid(20)  # new asset being migrated now

        # Run already migrated
        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)

        existing_dest_run = _make_run(dest_rid, asset_rids=[asset_a_rid])
        ctx.destination_client.get_run.return_value = existing_dest_run

        updated_run = _make_run(dest_rid, asset_rids=[asset_a_rid, asset_b_rid])
        existing_dest_run.update.return_value = updated_run

        source_run = _make_run(source_rid)
        result = migrator.copy_from(source_run, RunCopyOptions(new_assets=[asset_b_rid]))

        existing_dest_run.update.assert_called_once()
        call_kwargs = existing_dest_run.update.call_args
        assert set(call_kwargs.kwargs["assets"]) == {asset_a_rid, asset_b_rid}
        assert result is updated_run

    def test_no_update_when_asset_already_present(self) -> None:
        """When the new asset is already on the existing run, update() is not called."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid = _run_rid(1)
        dest_rid = _run_rid(100)
        asset_rid = _asset_rid(10)

        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)

        existing_dest_run = _make_run(dest_rid, asset_rids=[asset_rid])
        ctx.destination_client.get_run.return_value = existing_dest_run

        source_run = _make_run(source_rid)
        result = migrator.copy_from(source_run, RunCopyOptions(new_assets=[asset_rid]))

        existing_dest_run.update.assert_not_called()
        assert result is existing_dest_run

    def test_no_update_when_no_new_assets_in_options(self) -> None:
        """When new_assets is None (default options), _ensure_assets_added is skipped, the existing run is returned."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid = _run_rid(1)
        dest_rid = _run_rid(100)

        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)

        existing_dest_run = _make_run(dest_rid, asset_rids=[_asset_rid(10)])
        ctx.destination_client.get_run.return_value = existing_dest_run

        source_run = _make_run(source_rid)
        result = migrator.copy_from(source_run, RunCopyOptions())

        existing_dest_run.update.assert_not_called()
        assert result is existing_dest_run

    def test_create_run_not_called_for_existing_run(self) -> None:
        """When the run is already in migration state, create_run is never called."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid = _run_rid(1)
        dest_rid = _run_rid(100)

        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)
        ctx.destination_client.get_run.return_value = _make_run(dest_rid)

        source_run = _make_run(source_rid)
        migrator.copy_from(source_run, RunCopyOptions())

        ctx.destination_client.create_run.assert_not_called()

    def test_adds_multiple_missing_assets(self) -> None:
        """Multiple missing assets are all added in a single update() call."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid = _run_rid(1)
        dest_rid = _run_rid(100)
        existing_asset = _asset_rid(10)
        new_asset_b = _asset_rid(20)
        new_asset_c = _asset_rid(30)

        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)

        existing_dest_run = _make_run(dest_rid, asset_rids=[existing_asset])
        ctx.destination_client.get_run.return_value = existing_dest_run

        source_run = _make_run(source_rid)
        migrator.copy_from(source_run, RunCopyOptions(new_assets=[new_asset_b, new_asset_c]))

        existing_dest_run.update.assert_called_once()
        call_kwargs = existing_dest_run.update.call_args
        assert set(call_kwargs.kwargs["assets"]) == {existing_asset, new_asset_b, new_asset_c}


class TestRunMigratorEnsureAssetsAdded:
    """Unit tests for _ensure_assets_added in isolation."""

    def _make_migrator(self) -> RunMigrator:
        return RunMigrator(_make_context())

    def test_accepts_asset_object(self) -> None:
        """Asset objects (with .rid) are resolved to RIDs correctly."""
        migrator = self._make_migrator()

        asset_rid = _asset_rid(99)
        asset_obj = MagicMock()
        asset_obj.rid = asset_rid

        run = _make_run(_run_rid(1), asset_rids=[])
        migrator._ensure_assets_added(run, [asset_obj])

        run.update.assert_called_once()
        assert asset_rid in run.update.call_args.kwargs["assets"]

    def test_accepts_string_rid(self) -> None:
        """Plain string RIDs are accepted as new_assets."""
        migrator = self._make_migrator()

        asset_rid = _asset_rid(99)
        run = _make_run(_run_rid(1), asset_rids=[])
        migrator._ensure_assets_added(run, [asset_rid])

        run.update.assert_called_once()
        assert asset_rid in run.update.call_args.kwargs["assets"]

    def test_empty_new_assets_list_raises(self) -> None:
        """An empty new_assets list raises ValueError since runs must have at least one asset."""
        migrator = self._make_migrator()
        run = _make_run(_run_rid(1), asset_rids=[_asset_rid(10)])
        with pytest.raises(ValueError, match="non-empty"):
            migrator._ensure_assets_added(run, [])


# ---------------------------------------------------------------------------
# Attachment migration
# ---------------------------------------------------------------------------


class TestRunMigratorAttachments:
    @patch("nominal.experimental.migration.migrator.run_migrator.AttachmentMigrator")
    def test_new_run_attachments_are_migrated(self, mock_att_cls: MagicMock) -> None:
        """When creating a new run, each source attachment is migrated and passed to create_run."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        src_att_1, src_att_2 = MagicMock(rid=_att_rid(1)), MagicMock(rid=_att_rid(2))
        new_att_1, new_att_2 = MagicMock(rid=_att_rid(101)), MagicMock(rid=_att_rid(102))

        mock_att_migrator = mock_att_cls.return_value
        mock_att_migrator.copy_from.side_effect = [new_att_1, new_att_2]

        source_run = _make_run(_run_rid(1))
        source_run.list_attachments.return_value = [src_att_1, src_att_2]

        dest_run = _make_run(_run_rid(100))
        ctx.destination_client.create_run.return_value = dest_run

        migrator.copy_from(source_run, RunCopyOptions())

        # AttachmentMigrator was instantiated with the shared context
        mock_att_cls.assert_called_once_with(ctx)

        # Each attachment was migrated in order
        mock_att_migrator.copy_from.assert_any_call(src_att_1)
        mock_att_migrator.copy_from.assert_any_call(src_att_2)
        assert mock_att_migrator.copy_from.call_count == 2

        # create_run received the migrated attachments
        call_kwargs = ctx.destination_client.create_run.call_args.kwargs
        assert set(a.rid for a in call_kwargs["attachments"]) == {_att_rid(101), _att_rid(102)}

    @patch("nominal.experimental.migration.migrator.run_migrator.AttachmentMigrator")
    def test_existing_run_does_not_remigrate_attachments(self, mock_att_cls: MagicMock) -> None:
        """When the run already exists in state, AttachmentMigrator is never instantiated."""
        ctx = _make_context()
        migrator = RunMigrator(ctx)

        source_rid, dest_rid = _run_rid(1), _run_rid(100)
        ctx.migration_state.record_mapping(ResourceType.RUN, source_rid, dest_rid)

        existing_run = _make_run(dest_rid)
        ctx.destination_client.get_run.return_value = existing_run

        source_run = _make_run(source_rid)
        source_run.list_attachments.return_value = [MagicMock(rid=_att_rid(1))]

        migrator.copy_from(source_run, RunCopyOptions())

        mock_att_cls.assert_not_called()
        ctx.destination_client.create_run.assert_not_called()
