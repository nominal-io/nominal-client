---
myst:
  html_meta:
    description: "Prepare a video, upload it to a dataset, and attach it to a run using its ingested timestamps."
---

# Video ingest in Python

{.lead}
Upload a video as a named channel on a dataset, then use its ingested timestamps to
create a run.

Video channels share the dataset's time domain with telemetry, so a correct capture
time lets you review them together.

The examples below form one workflow. They use a short sample clip and an illustrative
UTC start time; replace both with your own recording and capture time for real test data.

## Connect to Nominal

Install `nominal` and `huggingface-hub` in your Python environment. The latter is only
needed to download the sample.

Configure a profile using the [authentication guide](/guides/authentication.md), then create a client:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
```

## Download sample video

```python
from pathlib import Path

from huggingface_hub import hf_hub_download

source_path = Path(hf_hub_download(
    repo_id="nominal-io/raptor-engine-fire",
    filename="raptor_fire_first_5_seconds.mov",
    repo_type="dataset",
))
```

For your own recording, set `source_path = Path("path/to/recording.mov")` instead.

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727576460/raptor_fire_first_5_seconds_wixsgu.mov
```

## Correct video format

To normalize video, install FFmpeg with H.264 support, including the `ffprobe`
executable, from [FFmpeg's downloads](https://www.ffmpeg.org/download.html) or your
OS package manager.

{py:meth}`~nominal.core.Dataset.add_video` accepts H.264 or H.265 video.
For playback, use YUV420p pixel format and regularly spaced keyframes.
The SDK's {py:func}`~nominal.experimental.video_processing.normalize_video` helper
re-encodes to H.264 with YUV420p, converts existing audio to AAC, and places keyframes
approximately every two seconds by default. It writes an MP4 or MKV file.

```python
from nominal.experimental.video_processing import normalize_video

video_path = Path("raptor_fire_normalized.mp4")
normalize_video(source_path, video_path, force=False)
```

`force=False` protects an existing output file. Choose a new output path or explicitly
use `force=True` to replace it. If your video already meets the format requirements,
skip conversion and set `video_path = source_path`.

Keyframe spacing affects seeking and playback, especially over constrained networks;
keep the two-second default unless you have a reason to change it. Normalization can
also reduce resolution with the `resolution` argument, preserving the aspect ratio.
This trades image detail for a smaller video. The `num_threads` argument controls the
thread count passed to FFmpeg; when omitted, FFmpeg chooses it.

Normalization may change the frame count and logs a warning if it does. If you have
per-frame capture timestamps, verify that they still match the output frames before
uploading with `frame_timestamps`.

## Upload with a capture time

`start` is the absolute time of the first frame. Use a timezone-aware datetime or
integer nanoseconds since the Unix epoch. The sample time below is illustrative;
using it for your own video would misalign the video with its telemetry.

```python
from datetime import datetime, timezone

start_time = datetime(2011, 11, 11, 11, 11, 11, tzinfo=timezone.utc)
dataset = client.create_dataset(name="Engine Fire Tests")
video_file = dataset.add_video(
    path=video_path,
    channel="engine_cam",
    start=start_time,
)
video_file.poll_until_ingestion_completed()
```

The upload returns a {py:class}`~nominal.core.VideoDatasetFile` while ingestion is
still running. Keep that object and wait before using its ingested metadata or bounds.
The polling method refreshes the file and raises an SDK error if ingestion fails.
See [ingestion jobs](/guides/ingest/ingestion-jobs.md) for error handling.

To add video to an existing dataset, retrieve it with `client.get_dataset("<DATASET_RID>")`
instead of creating one. You can upload additional files to the same video channel.
Overlapping segments are not overwritten by default; `overwrite_overlapping=True`
deletes overlapping segments on that channel.

For recordings with individual frame capture times, supply `frame_timestamps` instead
of `start`: a sequence of absolute timestamps in integer nanoseconds since the Unix
epoch, one for each frame. Supply exactly one of these two arguments.

:::{note}
Standalone `Video` APIs such as `client.create_video()` and `Run.add_video()` are
deprecated. Use a dataset's video channel and attach that dataset to a run.
:::

## Add the video to a run

This example creates a standalone run and attaches the dataset directly. Use the
uploaded file's bounds so the run covers this clip, even if the dataset later contains
other recordings. No local duration estimate is needed.

```python
bounds = video_file.bounds
if bounds is None:
    raise RuntimeError("The ingested video file has no time bounds")

run = client.create_run(
    name="Engine Fire Run",
    start=bounds.start,
    end=bounds.end,
    description="Run for Raptor engine fire.",
)
run.add_dataset(ref_name="engine_fire_video", dataset=dataset)
```

The reference name identifies this dataset within the run. In Nominal, add a video
panel and select the `engine_cam` channel to review the clip alongside your test data.

If the dataset already belongs to an asset, you can instead create a run over that
asset's time range. Asset-associated runs and direct dataset attachments are distinct
ways to make data available; see the [Runs overview](/guides/runs/overview.md) for both workflows.

## Inspect uploaded video files

To resume later, retrieve the dataset using its RID, then list its video files:

```python
dataset = client.get_dataset("<DATASET_RID>")
for video_file in dataset.list_video_files():
    print(video_file.name)
    print("Frames:", video_file.num_frames)
    print("Frame rate:", video_file.media_frame_rate)
    print("Media duration (seconds):", video_file.media_duration_seconds)
    print("Time bounds:", video_file.bounds)
```

Metadata fields may be `None` when unavailable. Media duration describes the encoded
video; use `bounds` for its absolute time range in Nominal.

## Archive the dataset or remove a file

`dataset.archive()` archives the **entire dataset**, including its telemetry and all
video channels. `dataset.unarchive()` restores it. Archiving is not a way to hide just
one clip.

To delete a single uploaded video file, call {py:meth}`~nominal.core.DatasetFile.delete`
on that file. This removes data rather than archiving it; select the intended file
before calling it.

## Preview locally

In a Jupyter notebook, preview the normalized MP4 with IPython:

```python
from IPython.display import Video, display

display(Video(str(video_path), embed=True))
```

Embedding copies the video into the notebook, so use it for short clips. Browser codec
support varies; converting a MOV to an H.264 MP4 can help with local playback.
For processing video frames before upload, continue to the
[object identification guide](/guides/video/object-identification.md).
