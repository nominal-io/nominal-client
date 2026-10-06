from datetime import datetime, timedelta

import cv2

video = cv2.VideoCapture(video_path)

# Get the frames per second and total frame count
fps = video.get(cv2.CAP_PROP_FPS)
frame_count = video.get(cv2.CAP_PROP_FRAME_COUNT)

# Calculate the duration
duration_seconds = frame_count / fps
print(f"Duration: {duration_seconds} seconds")

video.release()

start_time = datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S")
end_time = start_time + timedelta(seconds=duration_seconds)
