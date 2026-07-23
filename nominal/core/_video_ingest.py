from __future__ import annotations

from datetime import datetime
from typing import Mapping, Sequence

from nominal_api import api, ingest_api, scout_video_api, upload_api

from nominal.core._utils.networking import HeaderProvider
from nominal.core.video import _upload_frame_timestamps
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def build_video_timestamp_manifest(
    auth_header: str,
    workspace_rid: str | None,
    upload_client: upload_api.UploadService,
    *,
    start: datetime | IntegralNanosecondsUTC | None = None,
    frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
    mcap_topic: str | None = None,
    header_provider: HeaderProvider | None = None,
) -> scout_video_api.VideoFileTimestampManifest:
    """Build a timestamp manifest for dataset-backed video ingest.

    Exactly one of `start`, `frame_timestamps`, or `mcap_topic` must be provided.
    """
    provided = [p for p in (start, frame_timestamps, mcap_topic) if p is not None]
    if len(provided) != 1:
        raise ValueError("exactly one of 'start', 'frame_timestamps', or 'mcap_topic' must be provided")

    if mcap_topic is not None:
        return scout_video_api.VideoFileTimestampManifest(
            mcap=scout_video_api.McapTimestampManifest(api.McapChannelLocator(topic=mcap_topic))
        )
    if frame_timestamps is not None:
        s3_path = _upload_frame_timestamps(
            auth_header, workspace_rid, upload_client, frame_timestamps, header_provider=header_provider
        )
        return scout_video_api.VideoFileTimestampManifest(s3path=s3_path)
    if start is not None:
        return scout_video_api.VideoFileTimestampManifest(
            no_manifest=scout_video_api.NoTimestampManifest(
                starting_timestamp=_SecondsNanos.from_flexible(start).to_api()
            )
        )
    raise AssertionError("unreachable: exactly one of 'start', 'frame_timestamps', or 'mcap_topic' was provided")


def build_video_ingest_options(
    target_rid: str,
    channel: str,
    tags: Mapping[str, str] | None,
    s3_path: str,
    timestamp_manifest: scout_video_api.VideoFileTimestampManifest,
    overwrite_overlapping: bool,
) -> ingest_api.IngestOptions:
    """Build IngestOptions for a VideoOptsV2 ingest into an existing dataset channel."""
    return ingest_api.IngestOptions(
        video_v2=ingest_api.VideoOptsV2(
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            target=ingest_api.DatasetIngestTarget(
                existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=target_rid)
            ),
            timestamp_manifest=timestamp_manifest,
            channel=channel,
            tags={**(tags or {})},
            over_write_segments=overwrite_overlapping or None,
        )
    )
