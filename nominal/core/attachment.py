from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Protocol, Sequence, cast

from google.protobuf import field_mask_pb2
from nominal_api import attachments_api
from typing_extensions import Self

from nominal._utils.iterator_tools import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.api_types import NominalProperties
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.attachments.v2 import attachments_pb2, attachments_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

# BatchGetAttachments accepts at most this many RIDs per request; the Conjure endpoint it replaces had no limit.
_BATCH_GET_LIMIT = 1000


@dataclass(frozen=True)
class Attachment(HasRid, RefreshableGrpcMixin[attachments_pb2.Attachment]):
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
        # Conjure service kept only for streaming contents, which has no gRPC counterpart.
        @property
        def attachment(self) -> attachments_api.AttachmentService: ...
        @property
        def attachment_v2(self) -> attachments_pb2_grpc.AttachmentServiceStub: ...

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

                new_labels = ["new-label-a", "new-label-b", *attachment.labels]
                attachment = attachment.update(labels=new_labels)

        Raises:
            NominalError: If the update request fails.
        """
        # The update mask, not the message, decides which fields are replaced: a listed empty value clears that field.
        updates = {"title": name, "description": description, "properties": properties, "labels": labels}
        paths = [path for path, value in updates.items() if value is not None]
        if not paths:
            # gRPC rejects an empty update mask; refresh to keep returning the latest server state.
            return self.refresh()

        request = attachments_pb2.UpdateAttachmentRequest(
            attachment=attachments_pb2.Attachment(
                rid=self.rid,
                title=name or "",
                description=description or "",
                properties=properties,
                labels=labels,
            ),
            update_mask=field_mask_pb2.FieldMask(paths=paths),
        )
        with translate_grpc_errors():
            response = self._clients.attachment_v2.UpdateAttachment(request)
        return self._refresh_from_api(response.attachment)

    def get_contents(self) -> BinaryIO:
        """Retrieve the contents of this attachment.
        Returns a file-like object in binary mode for reading.
        """
        response = self._clients.attachment.get_content(self._clients.auth_header, self.rid)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)

    def write(self, path: PathLike, mkdir: bool = True) -> None:
        """Write an attachment to the filesystem.

        `path` should be the path you want to save to, i.e. a file, not a directory.
        """
        path = Path(path)
        if mkdir:
            path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, "wb") as wf:
            shutil.copyfileobj(self.get_contents(), wf)

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
            self._clients.attachment_v2.UpdateAttachment(request)

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
            created_by_rid=attachment.created_by,
        )


def _get_attachment(clients: Attachment._Clients, rid: str) -> attachments_pb2.Attachment:
    """The attachment with the given RID.

    Raises:
        NominalNotFoundError: If no attachment with that RID is accessible.
        NominalError: If the retrieval request fails.
    """
    request = attachments_pb2.GetAttachmentRequest(attachment_rid=rid)
    with translate_grpc_errors():
        return clients.attachment_v2.GetAttachment(request).attachment


def _iter_get_attachments(clients: Attachment._Clients, rids: Iterable[str]) -> Iterable[Attachment]:
    """The attachments with the given RIDs, in no particular order.

    Raises:
        NominalNotFoundError: If any RID does not resolve to an accessible attachment.
        NominalError: If a retrieval request fails.
    """
    for rid_batch in batched(rids, _BATCH_GET_LIMIT):
        request = attachments_pb2.BatchGetAttachmentsRequest(attachment_rids=rid_batch)
        with translate_grpc_errors():
            response = clients.attachment_v2.BatchGetAttachments(request)
        for attachment in response.attachments:
            yield Attachment._from_proto(clients, attachment)
