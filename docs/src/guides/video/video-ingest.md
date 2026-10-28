---
myst:
  html_meta:
    description: "Convert a video for playback, upload it as a channel on a dataset and review it in a run."
---

# Video ingest

{.lead}
Upload a video file as a {term}`channel` on a {term}`dataset` with {py:meth}`~nominal.core.Dataset.add_video`.
With the right start time, it plays in sync with the telemetry in the same {term}`run`.

```{include} /guides/_snippets/connect-the-client.md
```

## Convert the video

`add_video` takes H.264 or H.265 video. For smooth playback, it also needs YUV420p pixels and regular keyframes.
{py:func}`~nominal.experimental.video_processing.normalize_video` converts a file to that format with [FFmpeg](https://www.ffmpeg.org/download.html), which must be installed with `ffprobe`:

```python
from nominal.experimental.video_processing import normalize_video

normalize_video("engine_fire.mov", "engine_fire.mp4", force=False)  # force=False keeps an existing output
```

It writes H.264 with a keyframe about every 2 seconds, converting any audio to AAC.
`resolution` scales the video down, keeping its aspect ratio.
Normalizing can change the number of frames, and logs a warning when it does.

## Upload with a start time

`start` is the absolute time of the first frame. Use the real capture time, or the video won't line up with its telemetry.

```python
from datetime import datetime, timezone

dataset = client.create_dataset("Engine fire tests")
file = dataset.add_video(
    "engine_fire.mp4",
    channel="engine_cam",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
)
file.poll_until_ingestion_completed()
```

If you know each frame's capture time, pass them as epoch nanoseconds in `frame_timestamps` instead of `start`.

More files on the same channel add to it.
`overwrite_overlapping=True` deletes earlier segments that overlap the new file.

For MCAP files, see [MCAP video ingest](/guides/video/mcap-video-ingest.md).
The standalone `Video` APIs, such as `client.create_video()`, are deprecated in favor of video channels.

## Review it in a run

Create a run over the file's time range and attach the dataset:

```python
assert file.bounds is not None  # set once ingestion succeeds
run = client.create_run("Engine fire", start=file.bounds.start, end=file.bounds.end)
run.add_dataset("engine_video", dataset)
```

In a {term}`workbook` on the run, add a video panel and select `engine_cam`. See [Runs](/guides/runs.md) for other ways to organize data.

## Inspect and remove files

{py:meth}`~nominal.core.Dataset.list_video_files` lists a dataset's video files and their metadata:

```python
for video in dataset.list_video_files():
    print(video.name, video.num_frames, video.media_frame_rate, video.bounds)
```

To delete one file, call {py:meth}`~nominal.core.DatasetFile.delete` on it.
`dataset.archive()` archives the whole dataset, including its telemetry.

To process frames before uploading, see [Object identification](/guides/video/object-identification.md).
