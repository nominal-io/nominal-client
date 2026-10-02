import cv2
from IPython.display import clear_output

neon_colors = {
    "car": (57, 255, 20),  # Neon Green
    "boat": (0, 255, 255),  # Neon Cyan
    "pottedplant": (255, 0, 255),  # Neon Magenta
    "horse": (255, 215, 0),  # Neon Gold
    "cat": (255, 69, 0),  # Neon Orange Red
    "clock": (173, 255, 47),  # Neon Green Yellow
    "cow": (255, 105, 180),  # Neon Pink
    "bicycle": (0, 255, 0),  # Neon Lime
    "bird": (255, 20, 147),  # Neon Deep Pink
    "traffic light": (0, 255, 127),  # Neon Spring Green
    "umbrella": (127, 255, 0),  # Neon Chartreuse
    "kite": (255, 99, 71),  # Neon Tomato
    "truck": (255, 255, 0),  # Neon Yellow
    "person": (255, 69, 0),  # Neon Orange
    "parking meter": (0, 191, 255),  # Neon Deep Sky Blue
    "bus": (255, 215, 0),  # Neon Gold
    "train": (138, 43, 226),  # Neon Blue Violet
    "motorbike": (255, 0, 255),  # Neon Magenta
    "backpack": (255, 105, 180),  # Neon Hot Pink
    "dog": (0, 255, 0),  # Neon Lime Green
    "sheep": (255, 20, 147),  # Neon Deep Pink
    "stop sign": (255, 69, 0),  # Neon Orange Red
    "book": (57, 255, 20),  # Neon Green
    "aeroplane": (0, 255, 255),  # Neon Cyan
    "cell phone": (255, 0, 255),  # Neon Magenta
    "skateboard": (255, 215, 0),  # Neon Gold
    "bench": (255, 99, 71),  # Neon Tomato
    "handbag": (0, 255, 127),  # Neon Spring Green
    "suitcase": (173, 255, 47),  # Neon Green Yellow
    "bear": (255, 105, 180),  # Neon Pink
    "chair": (0, 255, 0),  # Neon Lime
    "fire hydrant": (255, 69, 0),  # Neon Orange Red
}

# Load the video
cap = cv2.VideoCapture(raw_video_path)

# Get the video properties
frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
fps = int(cap.get(cv2.CAP_PROP_FPS))

# Define the codec and create a VideoWriter object to save the output video
output_path = "all_scores_bounding_box_output.mp4"
fourcc = cv2.VideoWriter_fourcc(*"mp4v")  # Codec for .mp4 files
out = cv2.VideoWriter(output_path, fourcc, fps, (frame_width, frame_height))

thickness = 2  # Thickness of the bounding box lines

while cap.isOpened():
    # Read the current frame
    ret, frame = cap.read()

    if not ret:
        break

    current_frame = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    df_frame = df_video.filter(pl.col("frame") == current_frame)

    if len(df_frame) > 0:
        for row_index in range(len(df_frame)):
            row = df_frame[row_index]
            top_left = (int(row["x_min"][0]), int(row["y_max"][0]))
            bottom_right = (int(row["x_max"][0]), int(row["y_min"][0]))
            color = neon_colors[row["object"][0]]

            # Draw the bounding box on the frame
            print(top_left, bottom_right, color, thickness)
            frame_with_box = cv2.rectangle(
                frame, top_left, bottom_right, color, thickness
            )

            # Choose the font, size, color, and thickness
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 1  # Font size
            text = row["object"][0]
            # Annotate the frame with text
            cv2.putText(
                frame_with_box,
                text,
                bottom_right,
                font,
                font_scale,
                color,
                thickness,
                cv2.LINE_AA,
            )
    else:
        frame_with_box = frame

    clear_output(wait=True)
    plot_frame(frame_with_box)

    # Write the frame with the bounding box to the output video
    out.write(frame_with_box)

# Release the video capture and writer objects
cap.release()
out.release()

print(f"Video saved successfully at {output_path}")
