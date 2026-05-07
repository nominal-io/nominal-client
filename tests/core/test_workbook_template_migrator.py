"""Tests for workbook template migration (content attachment migration)."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_template_migrator import (
    WorkbookTemplateMigrator,
)

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


def _stub_source_template(rid: str = "source-tmpl-rid") -> MagicMock:
    tmpl = MagicMock()
    tmpl.rid = rid
    tmpl.title = "Source Template"
    tmpl._clients.auth_header = "Bearer source"
    return tmpl


# ---------------------------------------------------------------------------
# _migrate_content_attachments tests
# ---------------------------------------------------------------------------


class TestMigrateContentAttachments:
    """Tests for WorkbookTemplateMigrator._migrate_content_attachments."""

    def _make_migrator(self) -> tuple[WorkbookTemplateMigrator, MigrationContext]:
        ctx = _make_context()
        migrator = WorkbookTemplateMigrator(ctx)
        return migrator, ctx

    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureEncoder")
    def test_no_attachment_rids_in_content(self, mock_encoder_cls: MagicMock) -> None:
        """When content has no attachment RIDs, content is returned unchanged."""
        migrator, _ctx = self._make_migrator()
        source = _stub_source_template()

        layout = MagicMock()
        content = MagicMock()
        mock_encoder_cls.do_encode.return_value = {"charts": {"chart1": "some data"}}

        result_layout, result_content = migrator._migrate_content_attachments(source, layout, content)

        # Content and layout returned unchanged
        assert result_layout is layout
        assert result_content is content

    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.AttachmentMigrator")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureEncoder")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureDecoder")
    def test_content_attachments_migrated(
        self, mock_decoder_cls: MagicMock, mock_encoder_cls: MagicMock, mock_att_migrator_cls: MagicMock
    ) -> None:
        """Attachments in content are migrated and replaced."""
        migrator, ctx = self._make_migrator()
        source = _stub_source_template()

        old_rid = _rid(1)
        new_rid = _rid(100)

        layout = MagicMock()
        content = MagicMock()
        mock_encoder_cls.do_encode.return_value = {"charts": {"chart1": f"![img]({old_rid})"}}

        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.return_value = MagicMock(rid=new_rid)

        mock_decoder_cls.return_value.do_decode.return_value = MagicMock()

        result_layout, result_content = migrator._migrate_content_attachments(source, layout, content)

        mock_att_migrator_cls.assert_called_once_with(ctx)
        mock_att.migrate_by_rid.assert_called_once_with(source._clients, old_rid)

        # Layout unchanged, content was replaced
        assert result_layout is layout
        assert result_content is mock_decoder_cls.return_value.do_decode.return_value

    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.AttachmentMigrator")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureEncoder")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureDecoder")
    def test_duplicate_rid_migrated_once(
        self, mock_decoder_cls: MagicMock, mock_encoder_cls: MagicMock, mock_att_migrator_cls: MagicMock
    ) -> None:
        """An attachment RID appearing multiple times in content is migrated once and all occurrences replaced."""
        migrator, _ctx = self._make_migrator()
        source = _stub_source_template()

        old_rid = _rid(1)
        new_rid = _rid(100)

        layout = MagicMock()
        content = MagicMock()
        mock_encoder_cls.do_encode.return_value = {
            "chart1": f"![img]({old_rid})",
            "chart2": f"![img]({old_rid})",
            "chart3": f"![img]({old_rid})",
        }

        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.return_value = MagicMock(rid=new_rid)

        mock_decoder_cls.return_value.do_decode.return_value = MagicMock()

        migrator._migrate_content_attachments(source, layout, content)

        # Migrated exactly once despite three occurrences
        mock_att.migrate_by_rid.assert_called_once_with(source._clients, old_rid)

        # All three occurrences replaced in the content passed to do_decode
        decode_call = mock_decoder_cls.return_value.do_decode.call_args[0][0]
        decoded_str = json.dumps(decode_call)
        assert decoded_str.count(new_rid) == 3
        assert old_rid not in decoded_str

    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.AttachmentMigrator")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureEncoder")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureDecoder")
    def test_content_rid_replacement_does_not_chain(
        self, mock_decoder_cls: MagicMock, mock_encoder_cls: MagicMock, mock_att_migrator_cls: MagicMock
    ) -> None:
        """Replacing A->B and B->C must not cause A->C."""
        migrator, _ctx = self._make_migrator()
        source = _stub_source_template()

        rid_a = _rid(1)
        rid_b = _rid(2)
        rid_c = _rid(3)

        layout = MagicMock()
        content = MagicMock()
        mock_encoder_cls.do_encode.return_value = {"charts": f"{rid_a} and {rid_b}"}

        # rid_a -> rid_b, rid_b -> rid_c
        rid_mapping = {rid_a: rid_b, rid_b: rid_c}
        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.side_effect = lambda clients, rid: MagicMock(rid=rid_mapping[rid])

        mock_decoder_cls.return_value.do_decode.return_value = MagicMock()

        migrator._migrate_content_attachments(source, layout, content)

        # Verify the JSON string passed to do_decode has correct replacements
        decode_call = mock_decoder_cls.return_value.do_decode.call_args[0][0]
        decoded_str = json.dumps(decode_call)
        # rid_a should have become rid_b, and original rid_b should have become rid_c
        # Chaining would incorrectly make rid_a -> rid_c
        assert rid_b in decoded_str
        assert rid_c in decoded_str
        assert rid_a not in decoded_str

    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.AttachmentMigrator")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureEncoder")
    @patch("nominal.experimental.migration.migrator.workbook_template_migrator.ConjureDecoder")
    def test_multiple_distinct_rids(
        self, mock_decoder_cls: MagicMock, mock_encoder_cls: MagicMock, mock_att_migrator_cls: MagicMock
    ) -> None:
        """Multiple distinct attachment RIDs are each migrated once."""
        migrator, _ctx = self._make_migrator()
        source = _stub_source_template()

        rid_1 = _rid(1)
        rid_2 = _rid(2)
        new_rid_1 = _rid(101)
        new_rid_2 = _rid(102)

        layout = MagicMock()
        content = MagicMock()
        mock_encoder_cls.do_encode.return_value = {
            "chart1": f"![img]({rid_1})",
            "chart2": f"![img]({rid_2})",
        }

        rid_mapping = {rid_1: new_rid_1, rid_2: new_rid_2}
        mock_att = mock_att_migrator_cls.return_value
        mock_att.migrate_by_rid.side_effect = lambda clients, rid: MagicMock(rid=rid_mapping[rid])

        mock_decoder_cls.return_value.do_decode.return_value = MagicMock()

        migrator._migrate_content_attachments(source, layout, content)

        assert mock_att.migrate_by_rid.call_count == 2

        decode_call = mock_decoder_cls.return_value.do_decode.call_args[0][0]
        decoded_str = json.dumps(decode_call)
        assert new_rid_1 in decoded_str
        assert new_rid_2 in decoded_str
        assert rid_1 not in decoded_str
        assert rid_2 not in decoded_str
