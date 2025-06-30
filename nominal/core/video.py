from __future__ import annotations

import json
import logging
import pathlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import BytesIO, TextIOBase, TextIOWrapper
from types import MappingProxyType
from typing import BinaryIO, Mapping, Protocol, Sequence

from nominal_api import api, ingest_api, scout_video, scout_video_api, upload_api
from typing_extensions import Self

from nominal._utils import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._multipart import path_upload_name, upload_multipart_io
from nominal.core._utils import HasRid
from nominal.core.filetype import FileType, FileTypes
from nominal.core.video_file import VideoFile
from nominal.exceptions import NominalIngestError, NominalIngestFailed
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Video(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def video(self) -> scout_video.VideoService: ...
        @property
        def upload(self) -> upload_api.UploadService: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        @property
        def video_file(self) -> scout_video.VideoFileService: ...

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until video ingestion has completed.
        This method polls Nominal for ingest status after uploading a video on an interval.

        Raises:
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
        """Archive this video.
        Archived videos are not deleted, but are hidden from the UI.
        """
        self._clients.video.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchives this video, allowing it to show up in the 'All Videos' pane in the UI."""
        self._clients.video.unarchive(self._clients.auth_header, self.rid)

    def add_file(
        self,
        path: pathlib.Path | str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
    ) -> VideoFile:
        """Append to a video from a file-path to H264-encoded video data.

        Args:
            path: Path to the video file to add to an existing video within Nominal
            start: Starting timestamp of the video file in absolute UTC time
            frame_timestamps: Per-frame absolute nanosecond timestamps. Most usecases should instead use the 'start'
                parameter, unless precise per-frame metadata is available and desired.
            description: Description of the video file.
                NOTE: this is currently not displayed to users and may be removed in the future.

        Returns:
            Reference to the created video file.
        """
        path = pathlib.Path(path)
        file_type = FileType.from_video(path)

        with path.open("rb") as video_file:
            return self.add_from_io(
                video_file,
                name=path_upload_name(path, file_type),
                start=start,
                frame_timestamps=frame_timestamps,
                description=description,
                file_type=file_type,
            )

    add_file_to_video = add_file

    def add_from_io(
        self,
        video: BinaryIO,
        name: str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MP4,
    ) -> VideoFile:
        """Append to a video from a file-like object containing video data encoded in H264 or H265.

        Args:
            video: File-like object containing video data encoded in H264 or H265.
            name: Name of the file to use when uploading to S3.
            start: Starting timestamp of the video file in absolute UTC time
            frame_timestamps: Per-frame absolute nanosecond timestamps. Most usecases should instead use the 'start'
                parameter, unless precise per-frame metadata is available and desired.
            description: Description of the video file.
                NOTE: this is currently not displayed to users and may be removed in the future.
            file_type: Metadata about the type of video file, e.g., MP4 vs. MKV.

        Returns:
            Reference to the created video file.
        """
        if isinstance(video, TextIOBase):
            raise TypeError(f"video {video} must be open in binary mode, rather than text mode")

        timestamp_manifest = _build_video_file_timestamp_manifest(
            self._clients.auth_header, self._clients.workspace_rid, self._clients.upload, start, frame_timestamps
        )
        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header, self._clients.workspace_rid, video, name, file_type, self._clients.upload
        )
        request = ingest_api.IngestRequest(
            ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        existing=ingest_api.ExistingVideoIngestDestination(
                            video_rid=self.rid,
                            video_file_details=ingest_api.VideoFileIngestDetails(description, [], {}),
                        )
                    ),
                    timestamp_manifest=timestamp_manifest,
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting video: no video created")

        return VideoFile._from_conjure(
            self._clients,
            self._clients.video_file.get(self._clients.auth_header, response.details.video.video_file_rid),
        )

    add_to_video_from_io = add_from_io

    def add_mcap(
        self,
        path: pathlib.Path,
        topic: str,
        description: str | None = None,
    ) -> VideoFile:
        """Append to a video from a file-path to an MCAP file containing video data.

        Args:
            path: Path to the video file to add to an existing video within Nominal
            topic: Topic pointing to video data within the MCAP file.
            description: Description of the video file.
                NOTE: this is currently not displayed to users and may be removed in the future.

        Returns:
            Reference to the created video file.
        """
        path = pathlib.Path(path)
        file_type = FileType.from_path(path)
        if file_type != FileTypes.MCAP:
            raise ValueError(f"mcap path '{path}' must end in `{FileTypes.MCAP.extension}`")

        with path.open("rb") as video_file:
            return self.add_mcap_from_io(
                video_file,
                name=path_upload_name(path, file_type),
                topic=topic,
                description=description,
                file_type=file_type,
            )

    add_mcap_to_video = add_mcap

    def add_mcap_from_io(
        self,
        mcap: BinaryIO,
        name: str,
        topic: str,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MCAP,
    ) -> VideoFile:
        """Append to a video from a file-like binary stream with MCAP data containing video data.

        Args:
            mcap: File-like binary object containing MCAP data to upload.
            name: Name of the file to create in S3 during upload
            topic: Topic pointing to video data within the MCAP file.
            description: Description of the video file.
                NOTE: this is currently not displayed to users and may be removed in the future.
            file_type: Metadata about the type of video (e.g. MCAP).

        Returns:
            Reference to the created video file.
        """
        if isinstance(mcap, TextIOBase):
            raise TypeError(f"dataset {mcap} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header, self._clients.workspace_rid, mcap, name, file_type, self._clients.upload
        )
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        existing=ingest_api.ExistingVideoIngestDestination(
                            video_rid=self.rid,
                            video_file_details=ingest_api.VideoFileIngestDetails(
                                file_labels=[],
                                file_properties={},
                                file_description=description,
                            ),
                        )
                    ),
                    timestamp_manifest=scout_video_api.VideoFileTimestampManifest(
                        mcap=scout_video_api.McapTimestampManifest(api.McapChannelLocator(topic=topic))
                    ),
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting mcap video: no video created")

        return VideoFile._from_conjure(
            self._clients,
            self._clients.video_file.get(self._clients.auth_header, response.details.video.video_file_rid),
        )

    add_mcap_to_video_from_io = add_mcap_from_io

    def list_files(self) -> Sequence[VideoFile]:
        """List all video files associated with the video."""
        raw_videos = self._clients.video_file.list_files_in_video(self._clients.auth_header, self.rid)
        return [VideoFile._from_conjure(self._clients, raw_video) for raw_video in raw_videos]

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


def _upload_frame_timestamps(
    auth_header: str,
    workspace_rid: str | None,
    upload_client: upload_api.UploadService,
    frame_timestamps: Sequence[IntegralNanosecondsUTC],
) -> str:
    """Uploads per-frame video timestamps to S3 and provides a path to the uploaded resource."""
    # Dump timestamp array into an in-memory file-like IO object
    json_io = BytesIO()
    text_json_io = TextIOWrapper(json_io)
    json.dump(frame_timestamps, text_json_io)
    text_json_io.flush()
    json_io.seek(0)

    logger.debug("Uploading timestamp manifests to s3")
    return upload_multipart_io(
        auth_header,
        workspace_rid,
        json_io,
        "timestamp_manifest",
        FileTypes.JSON,
        upload_client,
    )


def _build_video_file_timestamp_manifest(
    auth_header: str,
    workspace_rid: str | None,
    upload_client: upload_api.UploadService,
    start: datetime | IntegralNanosecondsUTC | None = None,
    frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
) -> scout_video_api.VideoFileTimestampManifest:
    if None not in (start, frame_timestamps):
        raise ValueError("Only one of 'start' or 'frame_timestamps' are allowed")
    elif frame_timestamps is not None:
        manifest_s3_path = _upload_frame_timestamps(auth_header, workspace_rid, upload_client, frame_timestamps)
        return scout_video_api.VideoFileTimestampManifest(s3path=manifest_s3_path)
    elif start is not None:
        # TODO(drake): expose scale parameter to users
        return scout_video_api.VideoFileTimestampManifest(
            no_manifest=scout_video_api.NoTimestampManifest(
                starting_timestamp=_SecondsNanos.from_flexible(start).to_api()
            )
        )
    else:
        raise ValueError("One of 'start' or 'frame_timestamps' must be provided")


def _get_video(clients: Video._Clients, video_rid: str) -> scout_video_api.Video:
    return clients.video.get(clients.auth_header, video_rid)
