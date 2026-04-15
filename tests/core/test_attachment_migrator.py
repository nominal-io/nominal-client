"""Tests for AttachmentMigrator."""

from __future__ import annotations

import io
import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType

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


def _make_raw_attachment(
    rid: str,
    title: str = "image.png",
    file_type: str = "image/png",
    description: str = "",
    properties: dict[str, str] | None = None,
    labels: list[str] | None = None,
) -> MagicMock:
    raw = MagicMock()
    raw.rid = rid
    raw.title = title
    raw.file_type = file_type
    raw.description = description
    raw.properties = properties or {}
    raw.labels = labels or []
    raw.created_at = 1000000000  # 1 second in nanoseconds
    return raw


def _make_source_clients(raw_attachment: MagicMock, content: bytes = b"image-bytes") -> MagicMock:
    """Create mock source clients that return the given raw attachment and content."""
    clients = MagicMock()
    clients.auth_header = "Bearer source"
    clients.attachment.get.return_value = raw_attachment
    clients.attachment.get_content.return_value = io.BytesIO(content)
    return clients


# ---------------------------------------------------------------------------
# AttachmentMigrator tests
# ---------------------------------------------------------------------------


class TestAttachmentMigrator:
    def _make_migrator(self) -> tuple[AttachmentMigrator, MigrationContext]:
        ctx = _make_context()
        migrator = AttachmentMigrator(ctx)
        return migrator, ctx

    def test_resource_type(self) -> None:
        migrator, _ = self._make_migrator()
        assert migrator.resource_type == ResourceType.ATTACHMENT

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_migrate_by_rid(self, mock_attachment_cls: MagicMock) -> None:
        """migrate_by_rid fetches the raw attachment, constructs an Attachment, and delegates to copy_from."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        new_rid = _rid(100)
        raw = _make_raw_attachment(old_rid)
        source_clients = _make_source_clients(raw)

        # Stub Attachment._from_conjure to return a mock with the old RID
        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "image.png"
        mock_attachment_cls._from_conjure.return_value = mock_source_att

        # Stub the destination upload
        new_att = MagicMock()
        new_att.rid = new_rid
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        # Stub _get_latest_api and get_contents on the source attachment (used by _copy_from_impl)
        mock_source_att._clients = source_clients
        mock_source_att._get_latest_api.return_value = raw
        mock_source_att.get_contents.return_value = io.BytesIO(b"image-bytes")

        result = migrator.migrate_by_rid(source_clients, old_rid)

        # Raw attachment was fetched from source
        source_clients.attachment.get.assert_called_with("Bearer source", old_rid)

        # Attachment._from_conjure was called to construct the source Attachment
        mock_attachment_cls._from_conjure.assert_called_once_with(source_clients, raw)

        # Destination upload occurred
        ctx.destination_client.create_attachment_from_io.assert_called_once()

        # Result has the new RID
        assert result.rid == new_rid

        # Migration state was recorded
        assert ctx.migration_state.get_mapped_rid(ResourceType.ATTACHMENT, old_rid) == new_rid

    def test_migrate_by_rid_already_mapped(self) -> None:
        """migrate_by_rid returns early if the attachment is already in migration state."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        new_rid = _rid(100)
        raw = _make_raw_attachment(old_rid)

        # Pre-populate migration state
        ctx.migration_state.record_mapping(ResourceType.ATTACHMENT, old_rid, new_rid)

        # Stub get_attachment on destination client
        existing_att = MagicMock()
        existing_att.rid = new_rid
        ctx.destination_client.get_attachment.return_value = existing_att

        source_clients = _make_source_clients(raw)
        result = migrator.migrate_by_rid(source_clients, old_rid)

        source_clients.attachment.get.assert_called_once_with("Bearer source", old_rid)

        # Returned the existing destination attachment
        assert result.rid == new_rid
        ctx.destination_client.get_attachment.assert_called_once_with(new_rid)

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_migrate_by_rid_already_mapped_uses_resolved_destination_client(
        self, mock_attachment_cls: MagicMock
    ) -> None:
        """migrate_by_rid honors the resolver when an attachment was already migrated."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        new_rid = _rid(100)
        raw = _make_raw_attachment(old_rid)
        source_clients = _make_source_clients(raw)

        source_attachment = MagicMock()
        source_attachment.rid = old_rid
        source_attachment.name = "image.png"
        mock_attachment_cls._from_conjure.return_value = source_attachment

        resolved_client = MagicMock()
        resolved_client._clients.workspace_rid = "resolved-ws-rid"
        resolved_workspace = MagicMock()
        resolved_workspace.rid = "resolved-ws-rid"
        resolved_client.get_workspace.return_value = resolved_workspace
        resolved_attachment = MagicMock()
        resolved_attachment.rid = new_rid
        resolved_client.get_attachment.return_value = resolved_attachment

        ctx.destination_client_resolver = lambda resource: resolved_client if resource is source_attachment else None
        ctx.migration_state.record_mapping(ResourceType.ATTACHMENT, old_rid, new_rid)

        result = migrator.migrate_by_rid(source_clients, old_rid)

        source_clients.attachment.get.assert_called_once_with("Bearer source", old_rid)
        mock_attachment_cls._from_conjure.assert_called_once_with(source_clients, raw)
        resolved_client.get_attachment.assert_called_once_with(new_rid)
        ctx.destination_client.get_attachment.assert_not_called()
        assert result is resolved_attachment

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_copy_from_records_mapping(self, mock_attachment_cls: MagicMock) -> None:
        """copy_from records the old->new RID mapping in migration state."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        new_rid = _rid(100)
        raw = _make_raw_attachment(old_rid)

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer source"
        source_clients.attachment.get.return_value = raw
        source_clients.attachment.get_content.return_value = io.BytesIO(b"bytes")

        # Create a mock source Attachment
        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "image.png"
        mock_source_att._clients = source_clients

        new_att = MagicMock()
        new_att.rid = new_rid
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        migrator.copy_from(mock_source_att)

        assert ctx.migration_state.get_mapped_rid(ResourceType.ATTACHMENT, old_rid) == new_rid

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_copy_from_skips_already_mapped(self, mock_attachment_cls: MagicMock) -> None:
        """copy_from returns the existing attachment if already in migration state."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        new_rid = _rid(100)

        # Pre-populate migration state
        ctx.migration_state.record_mapping(ResourceType.ATTACHMENT, old_rid, new_rid)

        existing_att = MagicMock()
        existing_att.rid = new_rid
        ctx.destination_client.get_attachment.return_value = existing_att

        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "image.png"

        result = migrator.copy_from(mock_source_att)

        assert result.rid == new_rid
        ctx.destination_client.create_attachment_from_io.assert_not_called()

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_file_type_fallback_to_binary(self, mock_attachment_cls: MagicMock) -> None:
        """When source attachment has no file_type, falls back to BINARY."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        raw = _make_raw_attachment(old_rid, title="data.bin", file_type="")

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer source"
        source_clients.attachment.get.return_value = raw
        source_clients.attachment.get_content.return_value = io.BytesIO(b"bytes")

        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "data.bin"
        mock_source_att._clients = source_clients

        new_att = MagicMock(rid=_rid(100))
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        migrator.copy_from(mock_source_att)

        call_args = ctx.destination_client.create_attachment_from_io.call_args
        assert call_args[0][2].mimetype == "application/octet-stream"

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_file_type_preserved(self, mock_attachment_cls: MagicMock) -> None:
        """When source attachment has a file_type, it is preserved."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        raw = _make_raw_attachment(old_rid, title="photo.png", file_type="image/png")

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer source"
        source_clients.attachment.get.return_value = raw
        source_clients.attachment.get_content.return_value = io.BytesIO(b"bytes")

        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "photo.png"
        mock_source_att._clients = source_clients

        new_att = MagicMock(rid=_rid(100))
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        migrator.copy_from(mock_source_att)

        call_args = ctx.destination_client.create_attachment_from_io.call_args
        assert call_args[0][2].mimetype == "image/png"

    @patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
    def test_content_and_metadata_passed_to_upload(self, mock_attachment_cls: MagicMock) -> None:
        """Attachment title, description, properties, and labels are passed to the upload."""
        migrator, ctx = self._make_migrator()

        old_rid = _rid(1)
        raw = _make_raw_attachment(
            old_rid,
            title="report.pdf",
            file_type="application/pdf",
            description="Monthly report",
            properties={"team": "eng"},
            labels=["report", "monthly"],
        )

        source_clients = MagicMock()
        source_clients.auth_header = "Bearer source"
        source_clients.attachment.get.return_value = raw
        source_clients.attachment.get_content.return_value = io.BytesIO(b"pdf-bytes")

        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "report.pdf"
        mock_source_att._clients = source_clients

        new_att = MagicMock(rid=_rid(100))
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        migrator.copy_from(mock_source_att)

        call_args = ctx.destination_client.create_attachment_from_io.call_args
        assert call_args[0][1] == "report.pdf"  # title
        assert call_args[1]["description"] == "Monthly report"
        assert call_args[1]["properties"] == {"team": "eng"}
        assert call_args[1]["labels"] == ["report", "monthly"]
