import datetime

# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("front_center_camera")
dataset.add_video(
    "path/to/video_pt2.mp4",
    channel="front_center_camera",
    start=datetime.datetime(year=2025, month=2, day=7, hour=15, minute=17, second=3),
)
