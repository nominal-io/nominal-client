from datetime import datetime

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# Video is stored as a channel on a dataset, alongside your other test data
dataset = client.create_dataset(name="Engine Fire Tests")

video_file = dataset.add_video(
    path=video_path,
    channel="engine_cam",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)
