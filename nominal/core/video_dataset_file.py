from __future__ import annotations

from dataclasses import dataclass, field

from nominal_api import scout_catalog, scout_video_api
from typing_extensions import Self

from nominal.core.dataset_file import DatasetFile, _parse_common_file_fields


@dataclass(frozen=True, kw_only=True)
class VideoDatasetFile(DatasetFile):
    """A video file stored as a dataset channel.

    A specialization of `DatasetFile` for video files. Supports all inherited dataset-file
    behavior (refresh/download/delete/poll/etc.) and exposes read-only aggregate metadata
    produced by segmentation. `update()` is intentionally not yet available.
    """

    # Private, unsupported ingest provenance. Excluded from repr and equality.
    _timestamp_manifest: scout_video_api.VideoFileTimestampManifest = field(repr=False, compare=False)

    num_frames: int | None = None
    num_segments: int | None = None
    media_duration_seconds: float | None = None
    media_frame_rate: float | None = None
    scale_factor: float | None = None

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
