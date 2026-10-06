import cv2
from datetime import datetime, timedelta
import polars as pl

frame_timestamp_dict = dict(timestamps=[], frame=[])

# Load the video
cap = cv2.VideoCapture(raw_video_path)

# Get the video's frames per second (fps) to calculate frame duration
fps = cap.get(cv2.CAP_PROP_FPS)
frame_duration = 1000 / fps  # Duration of each frame in milliseconds

frame_number = 0
date_string = "2011-11-11 11:11:11"
start_timestamp = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

# Read the video frame by frame and print the duration for each frame
while cap.isOpened():
    ret, frame = cap.read()

    if not ret:
        break

    # Get the timestamp for the current frame
    timestamp_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
    start_timestamp = timestamp + timedelta(milliseconds=timestamp_ms)
    frame_timestamp_dict["timestamps"].append(new_timestamp)
    frame_timestamp_dict["frame"].append(frame_number)

    frame_number += 1

# Release the video capture object
cap.release()

df_frame_timestamps = pl.DataFrame(frame_timestamp_dict)
df_video_with_timestamps = df_video.join(df_frame_timestamps, on="frame")

df_video_with_timestamps.head()
