from nominal.core import NominalClient

# Assumes authentication has already been performed.
# Replace "default" with your profile name.
client = NominalClient.from_profile("default")

# Using utility from previous section on creating assets
# Hardcoding platform and serial number for this example, but typically this would be
# computed dynamically based on metadata about the data files, such as the filepath.
asset = retrieve_asset(client, platform="shimmer", serial_num="SN-001")

# Create dataset (only required for the first upload for a given data source)
dataset = client.create_dataset(
    # Human readable name for the dataset
    name="MCAP Flight Data",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Flight data from onboard telemetry platform",
)

# Add the MCAP file to the dataset.
# NOTE: can use `include_topics` and `exclude_topics` arguments to customize what data
#       actually gets ingested into nominal. By default, all protobuf-encoded topics
#       get ingested.
dataset.add_mcap("path/to/data.mcap")

# Attach the dataset to the asset
asset.add_dataset(
    # reference name for this dataset within the asset
    # This can be used later to retrieve a reference to this dataset via the same reference name for
    # future flight tests
    "mcap_flight_data",
    dataset,
)

# Video frames from the same MCAP land on a channel of the same dataset,
# so telemetry and video share one time domain.
# The `topic` is the MCAP topic carrying the video data; per-frame timestamps
# come from that topic's messages.
dataset.add_mcap_video(
    "path/to/data.mcap",
    # Name of the video channel within the dataset
    channel="mcap_flight_video",
    topic="video_data_topic",
)
