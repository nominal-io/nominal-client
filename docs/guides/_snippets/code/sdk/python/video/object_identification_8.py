from nominal.experimental.video_processing import normalize_video
from pathlib import Path

mp4_video_path = video_path.replace(".mov", ".mp4")

normalize_video(Path(video_path), Path(mp4_video_path))
