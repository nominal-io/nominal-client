from __future__ import annotations

from dataclasses import dataclass

from nominal_api import scout_video_api

from nominal.core.exceptions import NominalVideoScaleModeError
from nominal.ts import IntegralNanosecondsUTC, _InferrableTimestampType, _SecondsNanos


@dataclass(init=True, repr=False, eq=False, order=False, unsafe_hash=False)
class McapVideoDetails:
    mcap_channel_locator_topic: str


@dataclass(init=True, repr=False, eq=False, order=False, unsafe_hash=False)
class TimestampOptions:
    starting_timestamp: IntegralNanosecondsUTC
    ending_timestamp: IntegralNanosecondsUTC
    scaling_factor: float
    true_framerate: float


def _scale_parameter(
    ending_timestamp: _InferrableTimestampType | None = None,
    true_frame_rate: float | None = None,
    scale_factor: float | None = None,
) -> scout_video_api.ScaleParameter | None:
    """Build the scale parameter for a video whose media frame rate differs from the recording rate.

    The three arguments describe the same quantity three ways, so at most one may be given; returns
    None when none are.
    """
    provided = [value for value in (ending_timestamp, true_frame_rate, scale_factor) if value is not None]
    if len(provided) > 1:
        raise NominalVideoScaleModeError()
    if ending_timestamp is not None:
        return scout_video_api.ScaleParameter(ending_timestamp=_SecondsNanos.from_flexible(ending_timestamp).to_api())
    if true_frame_rate is not None:
        return scout_video_api.ScaleParameter(true_frame_rate=true_frame_rate)
    if scale_factor is not None:
        return scout_video_api.ScaleParameter(scale_factor=scale_factor)
    return None
