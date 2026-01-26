from dataclasses import dataclass

from nominal.ts import IntegralNanosecondsUTC


@dataclass(init=True, repr=False, eq=False, order=False, unsafe_hash=False)
class McapVideoDetails:
    mcap_channel_locator_topic: str


@dataclass(init=True, repr=False, eq=False, order=False, unsafe_hash=False)
class TimestampOptions:
    starting_timestamp: IntegralNanosecondsUTC
    ending_timestamp: IntegralNanosecondsUTC
    scaling_factor: float
    true_framerate: float
