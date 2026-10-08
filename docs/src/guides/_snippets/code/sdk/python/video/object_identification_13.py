import cv2

# Load the video
cap = cv2.VideoCapture(raw_video_path)

schema = {
    "frame": pl.Int64,  # Column 'frame' as integer
    "object": pl.Utf8,  # Column 'object' as string
    "score": pl.Float64,  # Column 'score' as float
    "x_min": pl.Float64,  # Column 'x_min' as integer
    "y_min": pl.Float64,  # Column 'y_min' as integer
    "x_max": pl.Float64,  # Column 'x_max' as integer
    "y_max": pl.Float64,  # Column 'y_max' as integer
}

df_video = pl.DataFrame(schema=schema)

while cap.isOpened():
    # Read the current frame
    ret, frame = cap.read()

    if not ret:
        break

    # Convert the OpenCV frame (BGR format) to a Pillow image (RGB format)
    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)  # Convert BGR to RGB
    pillow_image = Image.fromarray(frame_rgb)

    current_frame = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    frame_objects = get_objects_from_pil_image(pillow_image, current_frame)

    df_video = pl.concat([df_video, frame_objects])

# Release the video capture object and close all windows
cap.release()

print("Number of objects identified:", len(df_video))
