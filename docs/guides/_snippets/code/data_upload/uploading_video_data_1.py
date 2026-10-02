import datetime

from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Using utility from previous section on creating assets
# Hardcoding platform and serial number for this example, but typically this would be
# computed dynamically based on metadata about the data files, such as the filepath.
asset = retrieve_asset(client, platform="shimmer", serial_num="SN-001")

# Video is stored as a channel on a dataset. Create the dataset once per data
# source; each camera becomes a channel on it.
# Make sure to run nominal.experimental.video_processing.normalize_video()
# on your raw video files first!
dataset = client.create_dataset(
    name="Front center camera video",
    # Optional description for the dataset, useful for storing notes and details
    # about the video (e.g., details about the camera)
    description="Front-facing go pro",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
        "position": "front_center",
        "video_type": "rgb",
    },
)

video_file = dataset.add_video(
    path="path/to/video.mp4",
    # Name of the video channel within the dataset that these frames belong to
    channel="front_center_camera",
    # Starting timestamp, in absolute time, for this particular video file
    start=datetime.datetime(year=2025, month=2, day=7, hour=14, minute=38, second=19),
)

# Attach the dataset to the asset
asset.add_dataset("front_center_camera", dataset)
