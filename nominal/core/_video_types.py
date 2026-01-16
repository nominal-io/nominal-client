from dataclasses import dataclass
from typing import TypeAlias

from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class McapVideoFileMetadata:
    """Metadata for MCAP video files.

    Attributes:
        mcap_channel_locator_topic: Topic name pointing to video data within the MCAP file.
            Empty string if topic is not available.
    """

    mcap_channel_locator_topic: str


@dataclass(frozen=True)
class MiscVideoFileMetadata:
    """Metadata for non-MCAP (miscellaneous) video files.

    Attributes:
        starting_timestamp: Starting timestamp of the video file in absolute UTC time.
        ending_timestamp: Optional ending timestamp of the video file.
        true_frame_rate: Optional true frame rate that the video was recorded at,
            regardless of the media playback frame rate.
        scale_factor: Optional scale factor representing the ratio of absolute time
            to media time (e.g., 2.0 means 2 seconds of absolute time per 1 second of media).
    """

    starting_timestamp: IntegralNanosecondsUTC
    ending_timestamp: IntegralNanosecondsUTC | None


# Type alias for video file ingest options - can be either MCAP or MISC metadata
VideoFileIngestOptions: TypeAlias = McapVideoFileMetadata | MiscVideoFileMetadata
