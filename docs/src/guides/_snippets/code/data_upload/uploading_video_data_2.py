import datetime

# Reuse the same refname for the next file.
dataset = asset.get_or_create_dataset(
    "front_center_camera", name="Front center camera video"
)
video_file = dataset.add_video(
    "path/to/re-encoded/video_pt2.mp4",
    channel="front_center_camera",
    start=datetime.datetime(2024, 10, 9, 13, 17, 3, tzinfo=datetime.timezone.utc),
)
uploaded_files.append(video_file)
