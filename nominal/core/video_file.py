from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

from nominal_api import scout_catalog, scout_video, scout_video_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.core._video_types import (
    McapVideoFileMetadata,
    MiscVideoFileMetadata,
    VideoFileIngestOptions,
)
from nominal.core.exceptions import NominalIngestError, NominalIngestFailed
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoFile(HasRid, RefreshableMixin[scout_video_api.VideoFile]):
    rid: str
    name: str
    description: str | None
    created_at: IntegralNanosecondsUTC
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def video_file(self) -> scout_video.VideoFileService: ...
        @property
        def catalog(self) -> scout_catalog.CatalogService: ...

    def archive(self) -> None:
        """Archive the video file, disallowing it to appear when playing back the video"""
        self._clients.video_file.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive the video file, allowing it to appear when playing back the video"""
        self._clients.video_file.unarchive(self._clients.auth_header, self.rid)

    def _get_latest_api(self) -> scout_video_api.VideoFile:
        return self._clients.video_file.get(self._clients.auth_header, self.rid)

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
        """Update video file metadata.

        Args:
            name: Name of the video file
            description: Description of the video file
            starting_timestamp: Starting timestamp for the video file
            ending_timestamp: Ending timestamp for the video file
            true_frame_rate: Frame rate that the video file was recorded at, irregardless of the
                frame rate that the media plays at.
            scale_factor: Ratio of absolute time to media time for the video file. For example,
                a value of 2 would indicate that for every second of media, two seconds have elapsed
                in absolute time.

        Returns:
            Updated video file metadata.

        NOTE: only one of {ending_timestamp, true_frame_rate, scale_factor} may be present at one time.
        """
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
        updated_file = self._clients.video_file.update(
            self._clients.auth_header,
            request,
            self.rid,
        )
        return self._refresh_from_api(updated_file)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until video ingestion has completed.
        This method polls Nominal for ingest status after uploading a video file on an interval.

        Raises:
        ------
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """
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
                    raise NominalIngestFailed(
                        f"ingest failed for video {self.rid!r}: {error.message} ({error.error_type})"
                    )
            else:
                raise NominalIngestError(f"Unhandled ingest status {status.type!r} for video {self.rid!r}")

            time.sleep(interval.total_seconds())

    def get_file_ingest_options(self) -> VideoFileIngestOptions:
        """Get ingest options metadata for this video file.

        Retrieves metadata about the video file (such as timestamps, frame rate, and scale factor)
        that can be used when ingesting this video into a video channel. The returned options
        are either MCAP or MISC metadata depending on the video file type.

        Returns:
            Video file ingest options (either McapVideoFileMetadata or MiscVideoFileMetadata).

        Raises:
            ValueError: If the video file has an unexpected timestamp manifest type.
        """
        api_video_file = self._get_latest_api()
        if api_video_file._origin_metadata._timestamp_manifest._type == "mcap":
            mcap_manifest = api_video_file._origin_metadata._timestamp_manifest._mcap
            topic = (
                mcap_manifest.mcap_channel_locator.topic
                if mcap_manifest and mcap_manifest.mcap_channel_locator and mcap_manifest.mcap_channel_locator.topic
                else ""
            )
            return McapVideoFileMetadata(mcap_channel_locator_topic=topic)
        else:
            # TODO(sean): We need to add support for if starting timestamp isn't present, aka we have frame timestamps
            # from S3.
            if api_video_file._origin_metadata._timestamp_manifest._no_manifest is None:
                raise ValueError(
                    f"Expected no_manifest timestamp manifest for non-MCAP video file, "
                    f"but got type: {api_video_file._origin_metadata._timestamp_manifest._type}"
                )
            return MiscVideoFileMetadata(
                starting_timestamp=_SecondsNanos.from_api(
                    api_video_file._origin_metadata._timestamp_manifest._no_manifest.starting_timestamp
                ).to_nanoseconds(),
                ending_timestamp=_SecondsNanos.from_api(
                    api_video_file._segment_metadata.max_absolute_timestamp
                ).to_nanoseconds()
                if api_video_file._segment_metadata and api_video_file._segment_metadata.max_absolute_timestamp
                else None,
            )

    @classmethod
    def _from_conjure(cls, clients: _Clients, video_file: scout_video_api.VideoFile) -> Self:
        return cls(
            rid=video_file.rid,
            name=video_file.title,
            description=video_file.description,
            created_at=_SecondsNanos.from_flexible(video_file.created_at).to_nanoseconds(),
            _clients=clients,
        )
