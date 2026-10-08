from nominal.core import ChannelDataType, NominalClient

client = NominalClient.from_profile("default")
dataset = client.create_dataset("Frosty Flight")

# Pin the type before any data arrives. A channel whose first values happen to
# be whole numbers would otherwise be inferred as INT, and later fractional
# values would not fit it.
dataset.add_channel(
    "altitude",
    ChannelDataType.DOUBLE,
    unit="m",
    description="Barometric altitude above mean sea level",
)

# A numeric-looking status code that must stay categorical
dataset.add_channel("fault_code", ChannelDataType.STRING)
