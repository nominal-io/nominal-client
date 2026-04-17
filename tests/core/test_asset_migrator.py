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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STACK = "cerulean-staging"


def _att_rid(n: int) -> str:
    hex8 = f"{n:08x}"
    return f"ri.attachments.{_STACK}.attachment.{hex8}-0000-0000-0000-000000000000"


def _make_context() -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(destination_client=mock_client, migration_state=MigrationState())


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
