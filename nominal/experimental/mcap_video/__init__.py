"""Derive direct-MCAP video registrations from the recording itself.

EXPERIMENTAL / UNSTABLE. Backed by the in-development `nominal.video.v3.DirectMcapVideoService`,
whose request contract is still changing and may break without notice.

Direct-MCAP playback leaves the frames where they are: Nominal stores a pointer to the object plus
the decoder configuration and chunk timing needed to play it, and the browser range-reads the object
itself. Registering a recording therefore means describing what is inside it, which this derives:

    from nominal.experimental.mcap_video import scan_mcap_video

    scan = scan_mcap_video("recording.mcap")
    scan.video_topics        # must NOT be ingested -- ingesting converts them to HLS
    scan.telemetry_topics    # ingest these
    scan.channels            # the derived decoder configuration, one per video topic

Scanning and registering are separate steps on purpose: the derived configuration and the
topic-to-channel mapping are both worth reading before anything is persisted.

Requires the `mcap` extra: `pip install 'nominal[mcap]'`.
"""

from __future__ import annotations

try:
    from nominal.experimental.mcap_video._scan import (  # noqa: F401
        VIDEO_SCHEMA_NAMES,
        McapVideoScanError,
        default_channel_name,
        scan_mcap_video,
    )
except ImportError as e:
    if "mcap" in str(e):
        raise ImportError(
            "nominal[mcap] is required to scan MCAP video. Install it with: pip install 'nominal[mcap]'"
        ) from e
    raise

from nominal.experimental.mcap_video._types import (  # noqa: F401
    MAX_CHANNELS_PER_FILE,
    MAX_CHUNK_RANGES_PER_CHANNEL,
    MAX_CHUNK_RANGES_PER_REQUEST,
    McapBitstreamFormat,
    McapChunkRange,
    McapMessageEncoding,
    McapVideoChannelSpec,
    McapVideoCodec,
    McapVideoScan,
)

__all__ = [
    "MAX_CHANNELS_PER_FILE",
    "MAX_CHUNK_RANGES_PER_CHANNEL",
    "MAX_CHUNK_RANGES_PER_REQUEST",
    "McapBitstreamFormat",
    "McapChunkRange",
    "McapMessageEncoding",
    "McapVideoChannelSpec",
    "McapVideoCodec",
    "McapVideoScan",
    "McapVideoScanError",
    "VIDEO_SCHEMA_NAMES",
    "default_channel_name",
    "scan_mcap_video",
]
