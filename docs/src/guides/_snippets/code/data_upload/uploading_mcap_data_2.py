# Reuse the same refname for the next file.
dataset = asset.get_or_create_dataset("mcap_flight_data", name="MCAP Flight Data")
mcap_file = dataset.add_mcap("path/to/other/data.mcap")
uploaded_files.append(mcap_file)

# Video frames from the MCAP land on a channel of the same dataset
mcap_video_file = dataset.add_mcap_video(
    "path/to/other/data.mcap",
    channel="mcap_flight_video",
    topic="video_data_topic",
)
uploaded_files.append(mcap_video_file)
