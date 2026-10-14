from datetime import datetime
from pathlib import Path

from nominal.core import NominalClient
from nominal.experimental.video_processing import normalize_video

# Convert the downloaded .mov to a supported container and encoding.
normalized_path = Path(video_path).with_suffix(".mp4")
normalize_video(video_path, normalized_path)
video_path = str(normalized_path)

client = NominalClient.from_profile("default")

# Video is stored as a channel on a dataset, alongside your other test data
dataset = client.create_dataset(name="Engine Fire Tests")

video_file = dataset.add_video(
    path=video_path,
    channel="engine_cam",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)
