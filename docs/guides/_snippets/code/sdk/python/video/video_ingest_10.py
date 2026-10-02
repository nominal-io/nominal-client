import cv2

# Load the video
# video_path = 'raptor_fire_first_5_seconds.mp4'
# ☝️ Replace with your video_path or use the example video_path from above
cap = cv2.VideoCapture(video_path)

# Get video properties
frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))  # Width of the video frames
frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))  # Height of the video frames
fps = cap.get(cv2.CAP_PROP_FPS)  # Frames per second (fps)
frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))  # Total number of frames

# Calculate the duration of the video in seconds
duration = frame_count / fps

# Print video properties
print(f"Video Size: {frame_width}x{frame_height} (width x height)")
print(f"FPS: {fps}")
print(f"Total Frames: {frame_count}")
print(f"Duration: {duration:.2f} seconds")

# Release the video capture object
cap.release()
