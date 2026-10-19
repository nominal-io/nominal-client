from __future__ import annotations

import io
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator
from unittest.mock import MagicMock, patch

import grpc
import pytest
import requests
from google.protobuf import timestamp_pb2

from nominal.core.asset import Asset
from nominal.core.attachment import Attachment
from nominal.core.client import NominalClient
from nominal.core.filetype import FileType, FileTypes
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.attachments.v2 import attachments_pb2

_RID = "ri.attachments.test.attachment.1"


def _proto_attachment(rid: str = _RID, **kwargs) -> attachments_pb2.Attachment:
    return attachments_pb2.Attachment(rid=rid, created_at=timestamp_pb2.Timestamp(seconds=1), **kwargs)


def _attachment(clients: MagicMock) -> Attachment:
    return Attachment._from_proto(clients, _proto_attachment(title="original", labels=["keep"]))


_CONTENTS = b"attachment-bytes" * 4096


class _ObjectStorageHandler(BaseHTTPRequestHandler):
    """Serves `_CONTENTS` at /object and rejects every other path, like an expired presigned URL."""

    def do_GET(self) -> None:
        if self.path != "/object":
            self.send_error(403)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(_CONTENTS)))
        self.end_headers()
        self.wfile.write(_CONTENTS)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def object_storage_url() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), _ObjectStorageHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.mark.parametrize(
    ("changes", "paths", "sent"),
    [
        (
            {"name": "renamed", "labels": ["new"]},
            ["title", "labels"],
            attachments_pb2.Attachment(rid=_RID, title="renamed", labels=["new"]),
        ),
        (
            {"description": "notes", "properties": {}},
            ["description", "properties"],
            attachments_pb2.Attachment(rid=_RID, description="notes"),
        ),
        (
            {"properties": {"team": "eng"}, "labels": []},
            ["properties", "labels"],
            attachments_pb2.Attachment(rid=_RID, properties={"team": "eng"}),
        ),
    ],
)
def test_update_masks_passed_fields_and_clears_explicit_empties(
    changes: dict[str, Any], paths: list[str], sent: attachments_pb2.Attachment
) -> None:
    """Only passed fields are sent, each under its mask path; an explicitly empty collection is masked to clear it."""
    clients = MagicMock()
    attachment = _attachment(clients)
    clients.attachment.UpdateAttachment.return_value = attachments_pb2.UpdateAttachmentResponse(
        attachment=_proto_attachment(title="renamed")
    )

    returned = attachment.update(**changes)

    request = clients.attachment.UpdateAttachment.call_args.args[0]
    assert list(request.update_mask.paths) == paths
    assert request.attachment == sent
    assert returned is attachment
    assert attachment.name == "renamed"
    assert attachment.labels == ()


def test_update_without_fields_refreshes_instead_of_sending_an_empty_mask() -> None:
    """update() with nothing to change re-fetches the attachment, since gRPC rejects an empty update mask."""
    clients = MagicMock()
    attachment = _attachment(clients)
    clients.attachment.GetAttachment.return_value = attachments_pb2.GetAttachmentResponse(
        attachment=_proto_attachment(title="latest")
    )

    returned = attachment.update()

    clients.attachment.UpdateAttachment.assert_not_called()
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

    request = clients.attachment.UpdateAttachment.call_args.args[0]
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
    clients.attachment.BatchGetAttachments.side_effect = lambda request: (
        attachments_pb2.BatchGetAttachmentsResponse(
            attachments=[_proto_attachment(rid) for rid in request.attachment_rids]
        )
    )
    rids = [f"ri.attachments.test.attachment.{i}" for i in range(1001)]

    attachments = NominalClient(_clients=clients).get_attachments(rids)

    calls = clients.attachment.BatchGetAttachments.call_args_list
    assert [len(call.args[0].attachment_rids) for call in calls] == [1000, 1]
    assert [a.rid for a in attachments] == rids


def test_from_proto_reads_an_unset_creator_as_none() -> None:
    """An attachment without a creator converts to a missing creator RID rather than an empty one."""
    assert Attachment._from_proto(MagicMock(), _proto_attachment()).created_by_rid is None


def test_list_attachments_leaves_out_inaccessible_attachments(fake_rpc_error) -> None:
    """An asset still lists its accessible attachments when another linked attachment is missing or inaccessible."""
    clients = MagicMock()
    raw_asset = asset_pb2.Asset(rid="ri.asset.test.asset.1", attachments=[_RID, "ri.attachments.test.attachment.gone"])
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(responses={raw_asset.rid: raw_asset})
    clients.attachment.BatchGetAttachments.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    def get_attachment(request: attachments_pb2.GetAttachmentRequest) -> attachments_pb2.GetAttachmentResponse:
        if request.attachment_rid != _RID:
            raise fake_rpc_error(grpc.StatusCode.NOT_FOUND)
        return attachments_pb2.GetAttachmentResponse(attachment=_proto_attachment())

    clients.attachment.GetAttachment.side_effect = get_attachment

    attachments = Asset._from_proto(clients, raw_asset).list_attachments()

    assert [a.rid for a in attachments] == [_RID]


@pytest.mark.parametrize(
    ("name", "file_type", "expected_filename"),
    [
        ("report.pdf", FileTypes.BINARY, "report.pdf"),
        ("report.pdf", FileType(".pdf", "application/pdf"), "report.pdf"),
        ("report", FileType(".pdf", "application/pdf"), "report.pdf"),
        ("report.weird", FileType(".pdf", "application/pdf"), "report.weird"),
    ],
)
def test_attachment_filename_defaults_preserve_title_and_workspace(name, file_type, expected_filename) -> None:
    """Attachment filenames default missing extensions while preserving the requested title and workspace."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.test"
    clients.attachment.CreateAttachment.return_value = attachments_pb2.CreateAttachmentResponse(
        attachment=_proto_attachment(title=name)
    )

    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value="s3://bucket/report.pdf") as upload:
        attachment = NominalClient(_clients=clients).create_attachment_from_io(
            io.BytesIO(b"data"), name, file_type=file_type, labels=["monthly"]
        )

    assert upload.call_args.args[3] == expected_filename
    request = clients.attachment.CreateAttachment.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.test"
    assert request.attachment == attachments_pb2.Attachment(
        s3_path="s3://bucket/report.pdf", title=name, labels=["monthly"]
    )
    assert attachment.name == name


def test_get_attachment_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status from the attachment service surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    clients.attachment.GetAttachment.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        NominalClient(_clients=clients).get_attachment(_RID)


def test_write_streams_contents_from_the_presigned_uri(object_storage_url: str, tmp_path: Path) -> None:
    """write() streams the bytes served at the attachment's presigned URI into the destination file."""
    clients = MagicMock(header_provider=None)
    clients.attachment.GetAttachmentUri.return_value = attachments_pb2.GetAttachmentUriResponse(
        uri=f"{object_storage_url}/object"
    )
    destination = tmp_path / "nested" / "attachment.bin"

    _attachment(clients).write(destination)

    assert clients.attachment.GetAttachmentUri.call_args.args[0].attachment_rid == _RID
    assert destination.read_bytes() == _CONTENTS


def test_get_contents_raises_when_storage_rejects_the_download(object_storage_url: str) -> None:
    """A rejected download raises rather than returning the storage error body as attachment contents."""
    clients = MagicMock(header_provider=None)
    clients.attachment.GetAttachmentUri.return_value = attachments_pb2.GetAttachmentUriResponse(
        uri=f"{object_storage_url}/expired"
    )

    with pytest.raises(requests.HTTPError):
        _attachment(clients).get_contents()
