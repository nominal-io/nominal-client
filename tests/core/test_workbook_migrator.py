"""Tests for workbook migration (preview image and metadata)."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal_api import api as nominal_api

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_migrator import (
    ATTACHMENT_RID_PATTERN,
    WorkbookMigrator,
)

# ---------------------------------------------------------------------------
# ATTACHMENT_RID_PATTERN tests
# ---------------------------------------------------------------------------


class TestAttachmentRidPattern:
    def test_matches_standard_rid(self) -> None:
        rid = "ri.attachments.cerulean-staging.attachment.12345678-abcd-1234-abcd-123456789abc"
        assert ATTACHMENT_RID_PATTERN.fullmatch(rid)

    def test_matches_different_stacks(self) -> None:
        for stack in ("prod", "gov-staging", "cerulean-staging"):
            rid = f"ri.attachments.{stack}.attachment.aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            assert ATTACHMENT_RID_PATTERN.fullmatch(rid), f"Failed for stack={stack}"

    def test_rejects_non_attachment_rid(self) -> None:
        assert not ATTACHMENT_RID_PATTERN.fullmatch(
            "ri.assets.cerulean-staging.asset.12345678-abcd-1234-abcd-123456789abc"
        )

    def test_rejects_malformed_uuid(self) -> None:
        assert not ATTACHMENT_RID_PATTERN.fullmatch("ri.attachments.prod.attachment.not-a-uuid")

    def test_finds_multiple_in_string(self) -> None:
        text = (
            "ri.attachments.prod.attachment.aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee "
            "some text "
            "ri.attachments.prod.attachment.11111111-2222-3333-4444-555555555555"
        )
        assert len(ATTACHMENT_RID_PATTERN.findall(text)) == 2

    def test_finds_rid_in_json_string(self) -> None:
        text = json.dumps({"image": "ri.attachments.prod.attachment.aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"})
        assert len(ATTACHMENT_RID_PATTERN.findall(text)) == 1

    def test_does_not_match_uppercase_only_hex(self) -> None:
        # UUID_RE allows a-fA-F, but real attachment RIDs use lowercase; pattern still matches mixed-case
        rid = "ri.attachments.prod.attachment.AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
        assert ATTACHMENT_RID_PATTERN.fullmatch(rid)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_STACK = "cerulean-staging"


def _rid(n: int) -> str:
    """Build a deterministic attachment RID."""
    hex8 = f"{n:08x}"
    return f"ri.attachments.{_STACK}.attachment.{hex8}-0000-0000-0000-000000000000"


def _make_context() -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(destination_client=mock_client, migration_state=MigrationState())


def _stub_notebook(
    preview_image: object | None = None,
) -> MagicMock:
    nb = MagicMock()
    nb.metadata.preview_image = preview_image
    return nb


def _stub_source_workbook(rid: str = "source-wb-rid") -> MagicMock:
    wb = MagicMock()
    wb.rid = rid
    wb.title = "Source WB"
    # _clients is a ClientsBunch at runtime
    wb._clients.auth_header = "Bearer source"
    return wb


def _stub_dest_workbook(rid: str = "dest-wb-rid") -> MagicMock:
    wb = MagicMock()
    wb.rid = rid
    wb.title = "Dest WB"
    return wb


# ---------------------------------------------------------------------------
# _migrate_preview_image tests
# ---------------------------------------------------------------------------


class TestMigratePreviewImage:
    """Tests for WorkbookMigrator._migrate_preview_image."""

    def _make_migrator(self) -> tuple[WorkbookMigrator, MigrationContext]:
        ctx = _make_context()
        migrator = WorkbookMigrator(ctx)
        return migrator, ctx

    def test_no_preview_image(self) -> None:
        """When preview_image is None, nothing should be called."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_workbook()
        dest = _stub_dest_workbook()

        source_nb = _stub_notebook(preview_image=None)
        source._clients.notebook.get.return_value = source_nb

        migrator._migrate_preview_image(source, dest)

        ctx.destination_client._clients.notebook.update_metadata.assert_not_called()

    @patch("nominal.experimental.migration.migrator.workbook_migrator.AttachmentMigrator")
    def test_preview_image_light_and_dark(self, mock_att_migrator_cls: MagicMock) -> None:
        """Preview image with both light and dark are migrated and metadata is updated."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_workbook()
        dest = _stub_dest_workbook()

        light_rid = _rid(1)
        dark_rid = _rid(2)
        new_light_rid = _rid(101)
        new_dark_rid = _rid(102)

        preview = nominal_api.ThemeAwareImage(light=light_rid, dark=dark_rid)
        source_nb = _stub_notebook(preview_image=preview)
        source._clients.notebook.get.return_value = source_nb

        rid_mapping = {light_rid: new_light_rid, dark_rid: new_dark_rid}
        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.side_effect = lambda clients, rid: MagicMock(rid=rid_mapping[rid])

        migrator._migrate_preview_image(source, dest)

        assert mock_att.migrate_by_rid.call_count == 2

        ctx.destination_client._clients.notebook.update_metadata.assert_called_once()
        meta_call = ctx.destination_client._clients.notebook.update_metadata.call_args
        preview_arg = meta_call[0][1].preview_image
        assert preview_arg.light == new_light_rid
        assert preview_arg.dark == new_dark_rid

    @patch("nominal.experimental.migration.migrator.workbook_migrator.AttachmentMigrator")
    def test_preview_image_with_none_field(self, mock_att_migrator_cls: MagicMock) -> None:
        """Preview image with None light or dark field does not crash."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_workbook()
        dest = _stub_dest_workbook()

        light_rid = _rid(1)
        new_light_rid = _rid(101)

        # dark is None
        preview = MagicMock()
        preview.light = light_rid
        preview.dark = None
        source_nb = _stub_notebook(preview_image=preview)
        source._clients.notebook.get.return_value = source_nb

        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.return_value = MagicMock(rid=new_light_rid)

        migrator._migrate_preview_image(source, dest)

        mock_att.migrate_by_rid.assert_called_once_with(source._clients, light_rid)

        ctx.destination_client._clients.notebook.update_metadata.assert_called_once()
        meta_call = ctx.destination_client._clients.notebook.update_metadata.call_args
        preview_arg = meta_call[0][1].preview_image
        assert preview_arg.light == new_light_rid
        assert preview_arg.dark is None

    def test_preview_image_no_attachment_rids(self) -> None:
        """Preview image with non-attachment-RID strings does not trigger migration."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_workbook()
        dest = _stub_dest_workbook()

        preview = MagicMock()
        preview.light = "https://example.com/light.png"
        preview.dark = "https://example.com/dark.png"
        source_nb = _stub_notebook(preview_image=preview)
        source._clients.notebook.get.return_value = source_nb

        migrator._migrate_preview_image(source, dest)

        ctx.destination_client._clients.notebook.update_metadata.assert_not_called()

    @patch("nominal.experimental.migration.migrator.workbook_migrator.AttachmentMigrator")
    def test_shared_rid_in_light_and_dark(self, mock_att_migrator_cls: MagicMock) -> None:
        """Same RID in both light and dark is migrated only once."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_workbook()
        dest = _stub_dest_workbook()

        shared_rid = _rid(1)
        new_rid = _rid(100)

        preview = nominal_api.ThemeAwareImage(light=shared_rid, dark=shared_rid)
        source_nb = _stub_notebook(preview_image=preview)
        source._clients.notebook.get.return_value = source_nb

        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.return_value = MagicMock(rid=new_rid)

        migrator._migrate_preview_image(source, dest)

        # Only one migration despite RID appearing in both light and dark
        mock_att.migrate_by_rid.assert_called_once_with(source._clients, shared_rid)

        meta_call = ctx.destination_client._clients.notebook.update_metadata.call_args
        preview_arg = meta_call[0][1].preview_image
        assert preview_arg.light == new_rid
        assert preview_arg.dark == new_rid
