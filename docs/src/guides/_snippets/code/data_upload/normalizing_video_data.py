from nominal.experimental.video_processing import normalize_video

# Perform all conversions necessary to ingest this video into nominal, and output to the specified path
input_path = "path/to/video.mp4"
normalized_path = "path/to/re-encoded/video.mp4"
normalize_video(input_path, normalized_path)
