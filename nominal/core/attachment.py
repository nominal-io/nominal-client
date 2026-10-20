from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Protocol, Self, Sequence, cast

from google.protobuf import field_mask_pb2

from nominal._utils.iterator_tools import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.api_types import NominalProperties
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.networking import create_multipart_request_session
from nominal.exceptions import NominalNotFoundError
from nominal.protos.attachments.v2 import attachments_pb2, attachments_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

# BatchGetAttachments accepts at most this many RIDs per request; the Conjure endpoint it replaces had no limit.
_BATCH_GET_LIMIT = 1000
# Seconds a content download may wait to connect or between reads.
_DOWNLOAD_TIMEOUT = 30.0


@dataclass(frozen=True)
class Attachment(HasRid, RefreshableGrpcMixin[attachments_pb2.Attachment]):
    """An uploaded file that can be attached to assets or runs."""

    rid: str
    name: str
    description: str
    properties: NominalProperties
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC
    is_archived: bool

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def attachment(self) -> attachments_pb2_grpc.AttachmentServiceStub: ...

    def _get_latest_api(self) -> attachments_pb2.Attachment:
        return _get_attachment(self._clients, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: NominalProperties | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace attachment metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note:
            This replaces the metadata rather than appending it. To append to labels or properties, merge them before
            calling this method. E.g.:

            ```python
            new_labels = ["new-label-a", "new-label-b", *attachment.labels]
            attachment = attachment.update(labels=new_labels)
            ```

        Raises:
            NominalError: If the update request fails.
        """
        # Only fields named in the update mask are replaced, so a listed empty collection clears that field.
        attachment = attachments_pb2.Attachment(rid=self.rid)
        update_mask = field_mask_pb2.FieldMask()
        if name is not None:
            attachment.title = name
            update_mask.paths.append("title")
        if description is not None:
            attachment.description = description
            update_mask.paths.append("description")
        if properties is not None:
            attachment.properties.update(properties)
            update_mask.paths.append("properties")
        if labels is not None:
            attachment.labels.extend(labels)
            update_mask.paths.append("labels")
        if not update_mask.paths:
            # gRPC rejects an empty update mask; refresh to keep returning the latest server state.
            return self.refresh()

        request = attachments_pb2.UpdateAttachmentRequest(attachment=attachment, update_mask=update_mask)
        with translate_grpc_errors():
            response = self._clients.attachment.UpdateAttachment(request)
        return self._refresh_from_api(response.attachment)

    def get_contents(self) -> BinaryIO:
        """Retrieve the contents of this attachment.
        Returns a file-like object in binary mode for reading.

        The contents are streamed from object storage through a short-lived presigned URI, the same storage that
        attachment uploads are written to. Close the returned stream when finished, e.g. with a `with` block.

        Raises:
            NominalError: If the download URI cannot be retrieved.
            requests.RequestException: If downloading the contents fails.
        """
        request = attachments_pb2.GetAttachmentUriRequest(attachment_rid=self.rid)
        with translate_grpc_errors():
            uri = self._clients.attachment.GetAttachmentUri(request).uri
        session = create_multipart_request_session(pool_size=1, header_provider=self._clients.header_provider)
        response = session.get(uri, stream=True, timeout=_DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        # With stream=True the body is still unread, and `raw` reads it as a binary file-like object.
        return cast(BinaryIO, response.raw)

    def write(self, path: PathLike, mkdir: bool = True) -> None:
        """Write an attachment to the filesystem.

        `path` should be the path you want to save to, i.e. a file, not a directory.
        """
        path = Path(path)
        if mkdir:
            path.parent.mkdir(exist_ok=True, parents=True)
        with self.get_contents() as contents, open(path, "wb") as wf:
            shutil.copyfileobj(contents, wf)

    def archive(self) -> None:
        """Archive this attachment.
        Archived attachments are not deleted, but are hidden from the UI.

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the archive request fails.
        """
        self._set_archived(True)

    def unarchive(self) -> None:
        """Unarchive this attachment, allowing it to be viewed in the UI.

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the unarchive request fails.
        """
        self._set_archived(False)

    def _set_archived(self, is_archived: bool) -> None:
        request = attachments_pb2.UpdateAttachmentRequest(
            attachment=attachments_pb2.Attachment(rid=self.rid, is_archived=is_archived),
            update_mask=field_mask_pb2.FieldMask(paths=["is_archived"]),
        )
        with translate_grpc_errors():
            self._clients.attachment.UpdateAttachment(request)

    @classmethod
    def _from_proto(cls, clients: _Clients, attachment: attachments_pb2.Attachment) -> Self:
        return cls(
            rid=attachment.rid,
            name=attachment.title,
            description=attachment.description,
            properties=MappingProxyType(dict(attachment.properties)),
            labels=tuple(attachment.labels),
            created_at=attachment.created_at.ToNanoseconds(),
            is_archived=attachment.is_archived,
            _clients=clients,
            # proto3 reports an unset creator as an empty string.
            created_by_rid=attachment.created_by or None,
        )


def _get_attachments(clients: Attachment._Clients, rids: Iterable[str]) -> Sequence[attachments_pb2.Attachment]:
    """The attachments with these RIDs, in no particular order.

    Raises:
        NominalNotFoundError: If any RID does not resolve to an accessible attachment.
        NominalError: If a retrieval request fails.
    """
    attachments: list[attachments_pb2.Attachment] = []
    for rid_batch in batched(rids, _BATCH_GET_LIMIT):
        request = attachments_pb2.BatchGetAttachmentsRequest(attachment_rids=rid_batch)
        with translate_grpc_errors():
            attachments.extend(clients.attachment.BatchGetAttachments(request).attachments)
    return attachments


def _get_accessible_attachments(
    clients: Attachment._Clients, rids: Iterable[str]
) -> Sequence[attachments_pb2.Attachment]:
    """The attachments with these RIDs, in no particular order. Missing or inaccessible RIDs are left out.

    Raises:
        NominalError: If a retrieval request fails for any other reason.
    """
    attachments: list[attachments_pb2.Attachment] = []
    for rid_batch in batched(rids, _BATCH_GET_LIMIT):
        try:
            attachments.extend(_get_attachments(clients, rid_batch))
        except NominalNotFoundError:
            # BatchGetAttachments rejects a whole batch if any RID is missing or inaccessible, reporting both as
            # NOT_FOUND so hidden attachments stay hidden; fetch this batch one RID at a time instead.
            for rid in rid_batch:
                try:
                    attachments.append(_get_attachment(clients, rid))
                except NominalNotFoundError:
                    continue
    return attachments


def _get_attachment(clients: Attachment._Clients, rid: str) -> attachments_pb2.Attachment:
    """The attachment with the given RID.

    Raises:
        NominalNotFoundError: If no attachment with that RID is accessible.
        NominalError: If the retrieval request fails.
    """
    request = attachments_pb2.GetAttachmentRequest(attachment_rid=rid)
    with translate_grpc_errors():
        return clients.attachment.GetAttachment(request).attachment
