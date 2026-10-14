from datetime import datetime

# The features dataset already exists, so add the video to it as a channel.
# Both now share one time domain, and one dataset carries the whole analysis.
video_file = dataset.add_video(
    path=mp4_video_path,
    channel="rt_detr_video",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)

video_file.poll_until_ingestion_completed()

print(f"Video file ingested: {video_file.id}")
