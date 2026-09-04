from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import overload

from nominal_api import scout_catalog, scout_video_api
from typing_extensions import Self

from nominal.core._video_types import _scale_parameter
from nominal.core.dataset_file import DatasetFile
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True, kw_only=True)
class VideoDatasetFile(DatasetFile):
    """A video file stored as a dataset channel.

    A specialization of `DatasetFile` for video files. Supports all inherited dataset-file
    behavior (refresh/download/delete/poll/etc.) and exposes read-only aggregate metadata
    produced by segmentation.
    """

    # Private, unsupported ingest provenance. Excluded from repr and equality.
    _timestamp_manifest: scout_video_api.VideoFileTimestampManifest = field(repr=False, compare=False)

    num_frames: int | None = None
    num_segments: int | None = None
    media_duration_seconds: float | None = None
    media_frame_rate: float | None = None
    scale_factor: float | None = None

    @overload
    def update(
        self,
        *,
        name: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
    ) -> Self: ...

    @overload
    def update(
        self,
        *,
        name: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        ending_timestamp: datetime | IntegralNanosecondsUTC,
    ) -> Self: ...

    @overload
    def update(
        self,
        *,
        name: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        true_frame_rate: float,
    ) -> Self: ...

    @overload
    def update(
        self,
        *,
        name: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        scale_factor: float,
    ) -> Self: ...

    def update(
        self,
        *,
        name: str | None = None,
        starting_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        ending_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        true_frame_rate: float | None = None,
        scale_factor: float | None = None,
    ) -> Self:
        """Update this video file's name and/or timing. Updates the current instance, and returns it.

        Args:
            name: New name (title) for the file.
            starting_timestamp: New absolute start timestamp. The file's earliest segment is shifted to
                this timestamp and every other segment in the file is shifted by the same offset.
            ending_timestamp: Absolute timestamp the file's last frame should land on; rescales the
                file's frame timestamps around its start.
            true_frame_rate: Frame rate the video was recorded at, irregardless of the frame rate the
                media plays at; rescales the file's frame timestamps around its start.
            scale_factor: Ratio of absolute time to media time. For example, a value of 2 indicates
                that for every second of media, two seconds have elapsed in absolute time.

        Returns:
            This file, refreshed with its new bounds and recomputed segment metadata.

        Raises:
            ValueError: no field was provided, or more than one of {ending_timestamp, true_frame_rate,
                scale_factor} was provided.

        NOTE: only one of {ending_timestamp, true_frame_rate, scale_factor} may be present at one time.
        NOTE: video channels do not carry per-file descriptions, so unlike the legacy
            `VideoFile.update` there is no `description` parameter.
        """
        if all(value is None for value in (name, starting_timestamp, ending_timestamp, true_frame_rate, scale_factor)):
            raise ValueError(
                "At least one of 'name', 'starting_timestamp', 'ending_timestamp', 'true_frame_rate', "
                "or 'scale_factor' must be provided"
            )
        scale_parameter = _scale_parameter(
            ending_timestamp=ending_timestamp, true_frame_rate=true_frame_rate, scale_factor=scale_factor
        )

        self._clients.video.batch_update_video_channel_dataset_files(
            self._clients.auth_header,
            scout_video_api.BatchUpdateVideoChannelDatasetFilesRequest(
                dataset_rid=self.dataset_rid,
                updates=[
                    scout_video_api.VideoChannelDatasetFileUpdate(
                        dataset_file_id=self.id,
                        title=name,
                        start=None
                        if starting_timestamp is None
                        else _SecondsNanos.from_flexible(starting_timestamp).to_api(),
                        scale_parameter=scale_parameter,
                    )
                ],
            ),
        )
        # The response carries only the updated bounds, so refresh instead: that also picks up the
        # segment metadata the backend recomputes after a rescale.
        return self.refresh()

    @classmethod
    def _from_conjure(cls, clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        if dataset_file.metadata is None or dataset_file.metadata.video is None:
            raise ValueError(f"dataset file {dataset_file.id!r} has no video metadata")
        video_meta = dataset_file.metadata.video
        segment = video_meta.segment_metadata
        base = DatasetFile._from_conjure(clients, dataset_file)
        return cls(
            id=base.id,
            dataset_rid=base.dataset_rid,
            name=base.name,
            bounds=base.bounds,
            uploaded_at=base.uploaded_at,
            ingested_at=base.ingested_at,
            deleted_at=base.deleted_at,
            ingest_status=base.ingest_status,
            timestamp_channel=base.timestamp_channel,
            timestamp_type=base.timestamp_type,
            file_tags=base.file_tags,
            tag_columns=base.tag_columns,
            _clients=base._clients,
            _ingest_error_message=base._ingest_error_message,
            _timestamp_manifest=video_meta.timestamp_manifest,
            num_frames=None if segment is None else segment.num_frames,
            num_segments=None if segment is None else segment.num_segments,
            media_duration_seconds=None if segment is None else segment.media_duration_seconds,
            media_frame_rate=None if segment is None else segment.media_frame_rate,
            scale_factor=None if segment is None else segment.scale_factor,
        )
