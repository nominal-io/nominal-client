from __future__ import annotations

from typing import Mapping

from nominal_api import ingest_api, scout_video_api


def build_video_ingest_options(
    target_rid: str,
    *,
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
