from datetime import datetime

# The features dataset already exists, so add the video to it as a channel.
# Both now share one time domain, and one dataset carries the whole analysis.
video_file = dataset.add_video(
    path=mp4_video_path,
    channel="rt_detr_video",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)

print(f"Video file created: {video_file.id}")
