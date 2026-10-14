from nominal.core import ChannelDataType
from nominal.core.datasource import CreateChannelRequest

# One request per channel. Existing channels are updated rather than
# duplicated, so this is safe to re-run as part of a setup script.
dataset.batch_update_or_create_channels(
    [
        CreateChannelRequest("altitude", ChannelDataType.DOUBLE, unit="m"),
        CreateChannelRequest("airspeed", ChannelDataType.DOUBLE, unit="m/s"),
        CreateChannelRequest("motor_rpm", ChannelDataType.INT, unit="1/min"),
        CreateChannelRequest("fault_code", ChannelDataType.STRING),
    ]
)

# A repeated channel name fails the whole batch, so deduplicate first.
