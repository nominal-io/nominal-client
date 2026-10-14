import datetime

# Video is stored as a channel on a dataset. Get or create the dataset for
# this camera, then add files to its video channel.
# Make sure to run nominal.experimental.video_processing.normalize_video()
# on your raw video files first!
dataset = asset.get_or_create_dataset(
    data_scope_name="front_center_camera",
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
    path=normalized_path,
    # Name of the video channel within the dataset that these frames belong to
    channel="front_center_camera",
    # Starting timestamp, in absolute time, for this particular video file
    start=datetime.datetime(2024, 10, 9, 13, 3, 5, tzinfo=datetime.timezone.utc),
)
uploaded_files.append(video_file)
