from __future__ import annotations

import dataclasses
from typing import Literal, Union

from typing_extensions import TypeAlias


@dataclasses.dataclass(frozen=True)
class VideoResolution:
    resolution_width: int | None = None
    """Width of the video, in pixels. Auto-inferred if None based on aspect ratio.
    NOTE: MUST be divisible by 2.
    """

    resolution_height: int | None = None
    """Height of the video, in pixels. Auto-inferred if None based on aspect ratio.
    NOTE: MUST be divisible by 2.
    """

    allow_upscaling: bool = False
    """If true, allow upscaling beyond original resolution (e.g. 1080p -> 4k)"""

    def __post_init__(self) -> None:
        """Validate that provided resolution is valid"""
        if self.resolution_height is not None:
            if self.resolution_height <= 0 or self.resolution_height % 2 != 0:
                raise ValueError(
                    f"Provided resolution height is invalid-- must be positive and even integer"
                    f", received {self.resolution_height}"
                )

        if self.resolution_width is not None:
            if self.resolution_width <= 0 or self.resolution_width % 2 != 0:
                raise ValueError(
                    f"Provided resolution width is invalid-- must be positive and even integer"
                    f", received {self.resolution_width}"
                )

    def scale_factor(self) -> str:
        """Output a video filter flag usable with Ffmpeg to rescale the resolution of a video."""
        # If the user has not provided a width, auto-compute a width that keeps the existing
        # aspect ratio while also ensuring that the width is divisible by 2 (required for h264)
        width_str = "-2"
        if self.resolution_width is not None:
            if self.allow_upscaling:
                width_str = str(self.resolution_width)
            else:
                width_str = f"'min({self.resolution_width}, iw)'"

        # If the user has not provided a height, auto-compute a height that keeps the existing
        # aspect ratio while also ensuring that the height is divisible by 2 (required for h264)
        height_str = "-2"
        if self.resolution_height is not None:
            if self.allow_upscaling:
                height_str = str(self.resolution_height)
            else:
                height_str = f"'min({self.resolution_height}, ih)'"

        # Set scale to desired resolution, and set the Sample Aspect Ratio (SAR) to be 1:1,
        # meaning that each pixel of the video presents as a square when viewing in a video player
        return f"scale={width_str}:{height_str},setsar=1/1"


STANDARD_DEFINITION = VideoResolution(resolution_height=480, resolution_width=640)
HIGH_DEFINITION = VideoResolution(resolution_height=720, resolution_width=1280)
FULL_HD = VideoResolution(resolution_height=1080, resolution_width=1920)
QUAD_HD = VideoResolution(resolution_height=1440, resolution_width=2560)
ULTRA_HD = VideoResolution(resolution_height=2160, resolution_width=3840)

ResolutionSpecifier: TypeAlias = Literal[
    "480p",
    "720p",
    "1080p",
    "1440p",
    "2160p",
]

AnyResolutionType: TypeAlias = Union[ResolutionSpecifier, VideoResolution]


def _resolution_from_specifier(specifier: ResolutionSpecifier) -> VideoResolution:
    return {
        "480p": STANDARD_DEFINITION,
        "720p": HIGH_DEFINITION,
        "1080p": FULL_HD,
        "1440p": QUAD_HD,
        "2160p": ULTRA_HD,
    }[specifier]


def scale_factor_from_resolution(resolution: AnyResolutionType) -> str:
    """Build a video filter that scales the video using ffmpeg.

    Args:
        resolution: resolution specifier or explicit / custom resolution to scale video to

    Returns:
        Video filter specifier that can be used with ffmpeg to re-scale video contents
    """
    if isinstance(resolution, VideoResolution):
        return resolution.scale_factor()
    else:
        return _resolution_from_specifier(resolution).scale_factor()
