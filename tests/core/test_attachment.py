from __future__ import annotations

import io
from typing import Callable
from unittest.mock import MagicMock, patch

import grpc
import pytest
from google.protobuf import timestamp_pb2

from nominal.core.attachment import Attachment
from nominal.core.client import NominalClient
from nominal.exceptions import NominalNotFoundError
from nominal.protos.attachments.v2 import attachments_pb2

_RID = "ri.attachments.test.attachment.1"


def _proto_attachment(rid: str = _RID, **kwargs) -> attachments_pb2.Attachment:
    return attachments_pb2.Attachment(rid=rid, created_at=timestamp_pb2.Timestamp(seconds=1), **kwargs)


def _attachment(clients: MagicMock) -> Attachment:
    return Attachment._from_proto(clients, _proto_attachment(title="original", labels=["keep"]))


def test_update_masks_passed_fields_and_clears_explicit_empties() -> None:
    """Only passed fields are in the update mask; an explicitly empty collection is listed so the server clears it."""
    clients = MagicMock()
    attachment = _attachment(clients)
    clients.attachment_v2.UpdateAttachment.return_value = attachments_pb2.UpdateAttachmentResponse(
        attachment=_proto_attachment(title="renamed")
    )

    returned = attachment.update(name="renamed", labels=[])

    request = clients.attachment_v2.UpdateAttachment.call_args.args[0]
    assert list(request.update_mask.paths) == ["title", "labels"]
    assert request.attachment == attachments_pb2.Attachment(rid=_RID, title="renamed")
    assert returned is attachment
    assert attachment.name == "renamed"
    assert attachment.labels == ()


def test_update_without_fields_refreshes_instead_of_sending_an_empty_mask() -> None:
    """update() with nothing to change re-fetches the attachment, since gRPC rejects an empty update mask."""
    clients = MagicMock()
    attachment = _attachment(clients)
    clients.attachment_v2.GetAttachment.return_value = attachments_pb2.GetAttachmentResponse(
        attachment=_proto_attachment(title="latest")
    )

    returned = attachment.update()

    clients.attachment_v2.UpdateAttachment.assert_not_called()
    assert returned is attachment
    assert attachment.name == "latest"


@pytest.mark.parametrize(("set_state", "is_archived"), [(Attachment.archive, True), (Attachment.unarchive, False)])
def test_archive_state_is_updated_through_its_mask_path(
    set_state: Callable[[Attachment], None], is_archived: bool
) -> None:
    """archive()/unarchive() update only the is_archived path of the attachment."""
    clients = MagicMock()
    attachment = _attachment(clients)

    set_state(attachment)

    request = clients.attachment_v2.UpdateAttachment.call_args.args[0]
    assert list(request.update_mask.paths) == ["is_archived"]
    assert request.attachment == attachments_pb2.Attachment(rid=_RID, is_archived=is_archived)


def test_from_proto_keeps_nanosecond_created_at_and_detaches_properties() -> None:
    """Conversion keeps sub-microsecond creation time and does not alias the transport's properties map."""
    raw = attachments_pb2.Attachment(
        rid=_RID,
        properties={"team": "eng"},
        created_at=timestamp_pb2.Timestamp(seconds=1_700_000_000, nanos=123_456_789),
    )

    attachment = Attachment._from_proto(MagicMock(), raw)
    raw.properties["team"] = "ops"

    assert attachment.created_at == 1_700_000_000_123_456_789
    assert attachment.properties == {"team": "eng"}


def test_get_attachments_splits_requests_at_the_batch_limit() -> None:
    """More RIDs than one BatchGetAttachments request accepts are fetched in successive batches."""
    clients = MagicMock()
    clients.attachment_v2.BatchGetAttachments.side_effect = lambda request: (
        attachments_pb2.BatchGetAttachmentsResponse(
            attachments=[_proto_attachment(rid) for rid in request.attachment_rids]
        )
    )
    rids = [f"ri.attachments.test.attachment.{i}" for i in range(1001)]

    attachments = NominalClient(_clients=clients).get_attachments(rids)

    calls = clients.attachment_v2.BatchGetAttachments.call_args_list
    assert [len(call.args[0].attachment_rids) for call in calls] == [1000, 1]
    assert [a.rid for a in attachments] == rids


def test_create_attachment_from_io_creates_in_the_resolved_workspace() -> None:
    """The created attachment references the uploaded file and is placed in the client's resolved workspace."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.test"
    clients.attachment_v2.CreateAttachment.return_value = attachments_pb2.CreateAttachmentResponse(
        attachment=_proto_attachment(title="report.pdf")
    )

    with patch("nominal.core.client.upload_multipart_io", return_value="s3://bucket/report.pdf"):
        attachment = NominalClient(_clients=clients).create_attachment_from_io(
            io.BytesIO(b"data"), "report.pdf", labels=["monthly"]
        )

    request = clients.attachment_v2.CreateAttachment.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.test"
    assert request.attachment == attachments_pb2.Attachment(
        s3_path="s3://bucket/report.pdf", title="report.pdf", labels=["monthly"]
    )
    assert attachment.name == "report.pdf"


def test_get_attachment_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status from the attachment service surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    clients.attachment_v2.GetAttachment.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        NominalClient(_clients=clients).get_attachment(_RID)
