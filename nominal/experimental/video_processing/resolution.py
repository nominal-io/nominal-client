from __future__ import annotations

import dataclasses
from typing import Literal, TypeAlias


@dataclasses.dataclass(frozen=True)
class VideoResolution:
    resolution_width: int | None = None
    """Maximum width of the video, in pixels. Unbounded if None.
    NOTE: MUST be divisible by 2.
    """

    resolution_height: int | None = None
    """Maximum height of the video, in pixels. Unbounded if None.
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
        """Output a video filter flag usable with Ffmpeg to rescale a video to fit within this resolution."""
        # Scale both dimensions by the same factor so the video fits within the requested bounds without
        # distortion. Width is measured in display pixels (iw*sar) so anamorphic sources keep their aspect ratio.
        factor = "1"
        if self.resolution_width is not None and self.resolution_height is not None:
            factor = f"min({self.resolution_width}/(iw*sar),{self.resolution_height}/ih)"
        elif self.resolution_width is not None:
            factor = f"{self.resolution_width}/(iw*sar)"
        elif self.resolution_height is not None:
            factor = f"{self.resolution_height}/ih"
        if not self.allow_upscaling:
            factor = f"min({factor},1)"

        # Round down to even dimensions (required for h264), biased slightly to absorb float error, and
        # clamped to at least 2px since ffmpeg treats 0 as "keep the source size"
        width_str = f"'max(2,trunc(iw*sar*({factor})/2+0.01)*2)'"
        height_str = f"'max(2,trunc(ih*({factor})/2+0.01)*2)'"

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

AnyResolutionType: TypeAlias = ResolutionSpecifier | VideoResolution


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
