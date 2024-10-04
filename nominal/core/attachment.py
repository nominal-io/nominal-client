from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence, cast

from typing_extensions import Self

from .._api.combined import attachments_api
from ._clientsbunch import ClientsBunch
from ._utils import HasRid, update_dataclass


@dataclass(frozen=True)
class Attachment(HasRid):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: ClientsBunch = field(repr=False)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace attachment metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b", *attachment.labels]
            attachment = attachment.update(labels=new_labels)
        """
        request = attachments_api.UpdateAttachmentRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=name,
        )
        response = self._clients.attachment.update(self._clients.auth_header, request, self.rid)
        attachment = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, attachment, fields=self.__dataclass_fields__)
        return self

    def get_contents(self) -> BinaryIO:
        """Retrieve the contents of this attachment.
        Returns a file-like object in binary mode for reading.
        """
        response = self._clients.attachment.get_content(self._clients.auth_header, self.rid)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)

    def write(self, path: Path, mkdir: bool = True) -> None:
        """Write an attachment to the filesystem.

        `path` should be the path you want to save to, i.e. a file, not a directory.
        """
        if mkdir:
            path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, "wb") as wf:
            shutil.copyfileobj(self.get_contents(), wf)

    @classmethod
    def _from_conjure(cls, clients: ClientsBunch, attachment: attachments_api.Attachment) -> Self:
        return cls(
            rid=attachment.rid,
            name=attachment.title,
            description=attachment.description,
            properties=MappingProxyType(attachment.properties),
            labels=tuple(attachment.labels),
            _clients=clients,
        )


def _iter_get_attachments(clients: ClientsBunch, rids: Iterable[str]) -> Iterable[Attachment]:
    request = attachments_api.GetAttachmentsRequest(attachment_rids=list(rids))
    response = clients.attachment.get_batch(clients.auth_header, request)
    for a in response.response:
        yield Attachment._from_conjure(clients, a)
