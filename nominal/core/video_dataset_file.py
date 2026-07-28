from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

from nominal_api import scout_catalog, scout_video_api
from typing_extensions import Self

from nominal.core._video_ingest import resolve_video_channel_for_file, video_channel_series
from nominal.core.dataset_file import DatasetFile, _parse_common_file_fields
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

    channel: str | None = None
    """The video channel this file backs, when known.

    Populated on upload. `None` for files read back through the generic dataset-file paths, since the
    Catalog row does not record the channel; `update()` resolves it on demand in that case.
    """

    num_frames: int | None = None
    num_segments: int | None = None
    media_duration_seconds: float | None = None
    media_frame_rate: float | None = None
    scale_factor: float | None = None

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
            ValueError: no field was provided, more than one of {ending_timestamp, true_frame_rate,
                scale_factor} was provided, or this file's video channel could not be resolved.

        NOTE: only one of {ending_timestamp, true_frame_rate, scale_factor} may be present at one time.
        NOTE: video channels do not carry per-file descriptions, so unlike the legacy
            `VideoFile.update` there is no `description` parameter.
        """
        scale_inputs = (ending_timestamp, true_frame_rate, scale_factor)
        if sum(value is not None for value in scale_inputs) > 1:
            raise ValueError(
                "Expected at most one of 'ending_timestamp', 'true_frame_rate', and 'scale_factor' to be present"
            )
        if name is None and starting_timestamp is None and all(value is None for value in scale_inputs):
            raise ValueError(
                "At least one of 'name', 'starting_timestamp', 'ending_timestamp', 'true_frame_rate', "
                "or 'scale_factor' must be provided"
            )

        if ending_timestamp is not None:
            scale_parameter = scout_video_api.ScaleParameter(
                ending_timestamp=_SecondsNanos.from_flexible(ending_timestamp).to_api()
            )
        elif true_frame_rate is not None:
            scale_parameter = scout_video_api.ScaleParameter(true_frame_rate=true_frame_rate)
        elif scale_factor is not None:
            scale_parameter = scout_video_api.ScaleParameter(scale_factor=scale_factor)
        else:
            scale_parameter = None

        channel = self.channel if self.channel is not None else self._resolve_channel()
        self._clients.video.batch_update_video_channel_dataset_files(
            self._clients.auth_header,
            scout_video_api.BatchUpdateVideoChannelDatasetFilesRequest(
                channel_series=video_channel_series(self.dataset_rid, channel),
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
        refreshed = self.refresh()
        return refreshed if refreshed.channel is not None else replace(refreshed, channel=channel)

    def _resolve_channel(self) -> str:
        """Discover which video channel this file backs (the Catalog row does not record it)."""
        return resolve_video_channel_for_file(self._clients, self.dataset_rid, self.id, self.bounds)

    @classmethod
    def _from_conjure(cls, clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        if dataset_file.metadata is None or dataset_file.metadata.video is None:
            raise ValueError(f"dataset file {dataset_file.id!r} has no video metadata")
        video_meta = dataset_file.metadata.video
        segment = video_meta.segment_metadata
        return cls(
            **_parse_common_file_fields(clients, dataset_file),
            _timestamp_manifest=video_meta.timestamp_manifest,
            num_frames=None if segment is None else segment.num_frames,
            num_segments=None if segment is None else segment.num_segments,
            media_duration_seconds=None if segment is None else segment.media_duration_seconds,
            media_frame_rate=None if segment is None else segment.media_frame_rate,
            scale_factor=None if segment is None else segment.scale_factor,
        )
