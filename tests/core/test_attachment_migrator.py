"""Tests for AttachmentMigrator."""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest
import urllib3.exceptions

from nominal.experimental.migration.migration_state import MigrationState, SkippedResource
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType
from nominal.protos.attachments.v2 import attachments_pb2

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
) -> attachments_pb2.Attachment:
    return attachments_pb2.Attachment(
        rid=rid,
        title=title,
        file_type=file_type,
        description=description,
        properties=properties,
        labels=labels,
    )


def _make_source_clients(raw_attachment: attachments_pb2.Attachment) -> MagicMock:
    """Create mock source clients that return the given raw attachment."""
    clients = MagicMock()
    clients.auth_header = "Bearer source"
    clients.attachment.GetAttachment.return_value = attachments_pb2.GetAttachmentResponse(attachment=raw_attachment)
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

        # Stub Attachment._from_proto to return a mock with the old RID
        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "image.png"
        mock_attachment_cls._from_proto.return_value = mock_source_att

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
        source_clients.attachment.GetAttachment.assert_called_with(
            attachments_pb2.GetAttachmentRequest(attachment_rid=old_rid)
        )

        # Attachment._from_proto was called to construct the source Attachment
        mock_attachment_cls._from_proto.assert_called_once_with(source_clients, raw)

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

        source_clients.attachment.GetAttachment.assert_called_once_with(
            attachments_pb2.GetAttachmentRequest(attachment_rid=old_rid)
        )

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
        mock_attachment_cls._from_proto.return_value = source_attachment

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

        source_clients.attachment.GetAttachment.assert_called_once_with(
            attachments_pb2.GetAttachmentRequest(attachment_rid=old_rid)
        )
        mock_attachment_cls._from_proto.assert_called_once_with(source_clients, raw)
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

        source_clients = _make_source_clients(raw)

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

        source_clients = _make_source_clients(raw)

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

        source_clients = _make_source_clients(raw)

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

        source_clients = _make_source_clients(raw)

        mock_source_att = MagicMock()
        mock_source_att.rid = old_rid
        mock_source_att.name = "report.pdf"
        mock_source_att._clients = source_clients
        mock_source_att.get_contents.return_value = io.BytesIO(b"pdf-bytes")

        new_att = MagicMock(rid=_rid(100))
        ctx.destination_client.create_attachment_from_io.return_value = new_att

        migrator.copy_from(mock_source_att)

        call_args = ctx.destination_client.create_attachment_from_io.call_args
        assert call_args[0][0] is mock_source_att.get_contents.return_value  # content
        assert call_args[0][1] == "report.pdf"  # title
        assert call_args[1]["description"] == "Monthly report"
        assert call_args[1]["properties"] == {"team": "eng"}
        assert call_args[1]["labels"] == ["report", "monthly"]


# ---------------------------------------------------------------------------
# copy_attachments: archived filtering and failure containment
# ---------------------------------------------------------------------------


def _make_source_attachment(n: int, is_archived: bool = False) -> MagicMock:
    attachment = MagicMock()
    attachment.rid = _rid(n)
    attachment.name = f"file-{n}.png"
    attachment.is_archived = is_archived
    attachment._clients = _make_source_clients(_make_raw_attachment(attachment.rid, title=attachment.name))
    return attachment


def test_copy_attachments_skips_archived() -> None:
    """Archived attachments are neither uploaded nor returned."""
    ctx = _make_context()
    active, archived = _make_source_attachment(1), _make_source_attachment(2, is_archived=True)
    ctx.destination_client.create_attachment_from_io.return_value = MagicMock(rid=_rid(101))

    copied = AttachmentMigrator(ctx).copy_attachments([active, archived], "asset a")

    assert [a.rid for a in copied] == [_rid(101)]
    ctx.destination_client.create_attachment_from_io.assert_called_once()


def test_another_owners_copy_keeps_the_failed_owners_skip() -> None:
    """An attachment shared by a run and an asset stays skipped for the run whose copy failed."""
    ctx = _make_context()
    source = _make_source_attachment(1)
    migrator = AttachmentMigrator(ctx)
    ctx.destination_client.create_attachment_from_io.side_effect = ValueError("upload rejected")
    migrator.copy_attachments([source], "run r")
    ctx.destination_client.create_attachment_from_io.side_effect = None
    ctx.destination_client.create_attachment_from_io.return_value = MagicMock(rid=_rid(101))

    migrator.copy_attachments([source], "asset a")

    assert [s.source_rid for s in ctx.migration_state.skipped_resources] == [f"{source.rid} on run r"]


@patch("nominal.experimental.migration.migrator.attachment_migrator.Attachment")
def test_workbook_copy_keeps_owner_skip_until_owner_copy_succeeds(mock_attachment_cls: MagicMock) -> None:
    """A workbook's copy of a failed attachment leaves its skip; only the owner's own retry clears it."""
    ctx = _make_context()
    source = _make_source_attachment(1)
    migrator = AttachmentMigrator(ctx)
    ctx.destination_client.create_attachment_from_io.side_effect = ValueError("upload rejected")
    migrator.copy_attachments([source], "run r")

    destination = MagicMock(rid=_rid(101))
    ctx.destination_client.create_attachment_from_io.reset_mock(side_effect=True)
    ctx.destination_client.create_attachment_from_io.return_value = destination
    ctx.destination_client.get_attachment.return_value = destination

    mock_attachment_cls._from_proto.return_value = source
    assert migrator.migrate_by_rid(source._clients, source.rid) is destination
    assert [s.source_rid for s in ctx.migration_state.skipped_resources] == [f"{source.rid} on run r"]

    assert migrator.copy_attachments([source], "run r") == [destination]
    assert ctx.migration_state.skipped_resources == []
    ctx.destination_client.create_attachment_from_io.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ValueError("upload rejected"), id="permanent"),
        pytest.param(urllib3.exceptions.ProtocolError("connection reset"), id="transient-exhausted"),
    ],
)
def test_copy_attachments_contains_upload_failure(error: Exception, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed upload is recorded as a skip, left unmapped, and does not stop the next attachment."""
    monkeypatch.setattr("nominal.experimental.migration.utils.retry_utils.time.sleep", lambda _delay: None)
    ctx = _make_context()
    failing, healthy = _make_source_attachment(1), _make_source_attachment(2)

    def upload(content: object, title: str, *args: object, **kwargs: object) -> MagicMock:
        if title == failing.name:
            raise error
        return MagicMock(rid=f"dest-{title}")

    ctx.destination_client.create_attachment_from_io.side_effect = upload

    copied = AttachmentMigrator(ctx).copy_attachments([failing, healthy], "run r")

    assert [a.rid for a in copied] == [f"dest-{healthy.name}"]
    assert ctx.migration_state.get_mapped_rid(ResourceType.ATTACHMENT, failing.rid) is None
    assert ctx.migration_state.skipped_resources == [
        SkippedResource(ResourceType.ATTACHMENT.value, f"{failing.rid} on run r", f"copy failed: {error}")
    ]


def test_copy_attachments_retries_transient_upload_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient upload failure is retried in place, so the attachment copies without a skip."""
    monkeypatch.setattr("nominal.experimental.migration.utils.retry_utils.time.sleep", lambda _delay: None)
    ctx = _make_context()
    source = _make_source_attachment(1)
    ctx.destination_client.create_attachment_from_io.side_effect = [
        urllib3.exceptions.ProtocolError("connection reset"),
        MagicMock(rid=_rid(101)),
    ]

    copied = AttachmentMigrator(ctx).copy_attachments([source], "run r")

    assert [a.rid for a in copied] == [_rid(101)]
    assert ctx.migration_state.get_mapped_rid(ResourceType.ATTACHMENT, source.rid) == _rid(101)
    assert ctx.migration_state.skipped_resources == []
