from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol, Self

from nominal_api import scout_video, scout_video_api

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, update_dataclass
from nominal.exceptions import NominalIngestError, NominalIngestFailed
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoFile(HasRid):
    rid: str
    name: str
    description: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def video_file(self) -> scout_video.VideoFileService: ...

    def archive(self) -> None:
        """ """
        self._clients.video_file.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """ """
        self._clients.video_file.unarchive(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        ending_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        true_frame_rate: float | None = None,
        scale_factor: float | None = None,
    ) -> Self:
        """ """
        # If any of ending timestamp, true frame rate, or scale factor are defined,
        # update the scale parameter
        scale_parameter = None
        num_present = sum(int(v is not None) for v in (ending_timestamp, true_frame_rate, scale_factor))
        if num_present > 1:
            raise ValueError(
                "Expected at most one of 'ending_timestamp', 'true_frame_rate', and 'scale_factor' to be present"
            )

        if ending_timestamp is not None:
            scale_parameter = scout_video_api.ScaleParameter(
                ending_timestamp=_SecondsNanos.from_flexible(ending_timestamp).to_api()
            )
        elif true_frame_rate is not None:
            scale_parameter = scout_video_api.ScaleParameter(true_frame_rate=true_frame_rate)
        elif scale_factor is not None:
            scale_parameter = scout_video_api.ScaleParameter(scale_factor=scale_factor)

        request = scout_video_api.UpdateVideoFileRequest(
            title=name,
            description=description,
            scale_parameter=scale_parameter,
            starting_timestamp=None
            if starting_timestamp is None
            else _SecondsNanos.from_flexible(starting_timestamp).to_api(),
        )

        raw_video_file = self._clients.video_file.update(
            self._clients.auth_header,
            request,
            self.rid,
        )
        converted_video_file = self._from_conjure(self._clients, raw_video_file)
        update_dataclass(self, converted_video_file, fields=self.__dataclass_fields__)
        return self

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)):
        """ """
        while True:
            resp = self._clients.video_file.get_ingest_status(self._clients.auth_header, self.rid)
            status = resp.ingest_status
            if status.type == "success":
                return
            elif status.type == "inProgress":
                pass
            elif status.type == "error":
                error = status.error
                if error is not None:
                    raise NominalIngestFailed(f"ingest failed for video {self.rid!r}: {error.message} ({error.type})")
            else:
                raise NominalIngestError(f"Unhandled ingest status {status.type!r} for video {self.rid!r}")

            time.sleep(interval.total_seconds())

    @classmethod
    def _from_conjure(cls, clients: _Clients, video_file: scout_video_api.VideoFile) -> Self:
        return cls(
            rid=video_file.rid,
            name=video_file.title,
            description=video_file.description,
            _clients=clients,
        )
