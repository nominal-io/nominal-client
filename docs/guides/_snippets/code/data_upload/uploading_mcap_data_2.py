# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("mcap_flight_data")
dataset.add_mcap("path/to/other/data.mcap")

# Video frames from the MCAP land on a channel of the same dataset
dataset.add_mcap_video(
    "path/to/other/data.mcap",
    channel="mcap_flight_video",
    topic="video_data_topic",
)
