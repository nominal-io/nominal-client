from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import timedelta
from types import MappingProxyType
from typing import Mapping, Protocol, Sequence

from typing_extensions import Self

from nominal._api.scout_service_api import scout_video, scout_video_api
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, update_dataclass
from nominal.exceptions import NominalIngestError, NominalIngestFailed


@dataclass(frozen=True)
class Video(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def video(self) -> scout_video.VideoService: ...

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until video ingestion has completed.
        This method polls Nominal for ingest status after uploading a video on an interval.

        Raises
        ------
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known

        """
        while True:
            progress = self._clients.video.get_ingest_status(self._clients.auth_header, self.rid)
            if progress.type == "success":
                return
            elif progress.type == "inProgress":  # "type" strings are camelCase
                pass
            elif progress.type == "error":
                error = progress.error
                if error is not None:
                    error_messages = ", ".join([e.message for e in error.errors])
                    error_types = ", ".join([e.error_type for e in error.errors])
                    raise NominalIngestFailed(f"ingest failed for video {self.rid!r}: {error_messages} ({error_types})")
                raise NominalIngestError(
                    f"ingest status type marked as 'error' but with no instance for video {self.rid!r}"
                )
            else:
                raise NominalIngestError(f"unhandled ingest status {progress.type!r} for video {self.rid!r}")
            time.sleep(interval.total_seconds())

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace video metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in video.labels:
                new_labels.append(old_label)
            video = video.update(labels=new_labels)
        """
        # TODO(alkasm): properties SHOULD be optional here, but they're not.
        # For uniformity with other methods, will always "update" with current props on the client.
        request = scout_video_api.UpdateVideoMetadataRequest(
            description=description,
            labels=None if labels is None else list(labels),
            title=name,
            properties=dict(self.properties if properties is None else properties),
        )
        response = self._clients.video.update_metadata(self._clients.auth_header, request, self.rid)

        video = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, video, fields=self.__dataclass_fields__)
        return self

    def archive(self) -> None:
        """Archives the video, preventing it from showing up in the 'All Videos' pane in the UI."""
        self._clients.video.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchives the video, allowing it to show up in the 'All Videos' pane inthe UI."""
        self._clients.video.unarchive(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, video: scout_video_api.Video) -> Self:
        return cls(
            rid=video.rid,
            name=video.title,
            description=video.description,
            properties=MappingProxyType(video.properties),
            labels=tuple(video.labels),
            _clients=clients,
        )


def _get_video(clients: Video._Clients, video_rid: str) -> scout_video_api.Video:
    return clients.video.get(clients.auth_header, video_rid)
