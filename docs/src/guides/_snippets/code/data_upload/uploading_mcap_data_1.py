# Get or create the dataset linked to this asset.
dataset = asset.get_or_create_dataset(
    data_scope_name="mcap_flight_data",
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
#       actually gets ingested into nominal.
mcap_file = dataset.add_mcap("path/to/data.mcap")
uploaded_files.append(mcap_file)

# Video frames from the same MCAP land on a channel of the same dataset,
# so telemetry and video share one time domain.
# The `topic` is the MCAP topic carrying the video data; per-frame timestamps
# come from that topic's messages.
mcap_video_file = dataset.add_mcap_video(
    "path/to/data.mcap",
    # Name of the video channel within the dataset
    channel="mcap_flight_video",
    topic="video_data_topic",
)
uploaded_files.append(mcap_video_file)
