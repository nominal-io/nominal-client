---
myst:
  html_meta:
    description: "Load a video file in Python and upload it to the Nominal platform"
---

# Video ingest in Python

{.lead}
Load a video file in Python and upload it to the Nominal platform

With recent advancements in machine learning, robotics, and autonomy,
video is now an essential test artifact for modern hardware development teams.

```{include} /guides/_snippets/about-nominal-video.md
```

This guide demonstrates how to upload a video file to Nominal in Python.
Other guides in the Video section demonstrate basic Python video analysis
and computer vision for video pre-processing prior to Nominal upload.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Download sample video

```python
dataset_repo_id = "nominal-io/raptor-engine-fire"
dataset_filename = "raptor_fire_first_5_seconds.mov"
```

```{include} /guides/_snippets/sample-data.md
```

Since the dataset is a video file, rename `dataset_path`:

```python
video_path = dataset_path
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727576460/raptor_fire_first_5_seconds_wixsgu.mov
```

## Correct video format

```{include} /guides/_snippets/python/normalizing-video-data.md
```

## Upload video to Nominal

Once uploaded to Nominal, video test artifacts can be analyzed collaboratively in no-code workflows and
integrated with checks to signal off-Nominal video features.

Video is stored as a **video channel** on a dataset, next to the rest of your test data. You give
the channel a name when you upload, and it then behaves like any other channel: it belongs to the
dataset's time domain, and you drag it onto a video panel from channel search.

:::{note}

Standalone `Video` objects (`client.create_video()`, `Video.add_file()`, `Run.add_video()`) are
deprecated in favor of video channels. Using them emits a `LegacyVideoDeprecationWarning`. Attach
the dataset that carries the video channel instead.
:::

Nominal requires a video start time for video file upload. If the absolute time that the video was
captured is not important, you can use an arbitrary datetime like `datetime.now()` or
`2011-11-11 11:11:11`.

```{include} /guides/_snippets/video-start-times.md
```

```python
from datetime import datetime

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# Video is stored as a channel on a dataset, alongside your other test data
dataset = client.create_dataset(name="Engine Fire Tests")

video_file = dataset.add_video(
    path=video_path,
    channel="engine_cam",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)
```

## Add the video to a run

Once you've uploaded the video, it's easy to add its dataset to a run -
Nominal's container for multi-modal datasets that belong to the same test run and time domain.

```{include} /guides/_snippets/what-is-a-run.md
```

### Create an empty run

The below code will create an empty run container.
Any type of data that Nominal supports (video channels, CSV files, database connections, etc)
and has an overlapping time domain can be attached to this run container.

First, use [OpenCV](https://pypi.org/project/opencv-python/) to get the video length in seconds.
Nominal Runs expect both a start and end timestamp.
The example above uses `2011-11-11 11:11:11` as an arbitrary start time.
To get the run end time, add the video duration (in seconds) to this start time.
If [OpenCV](https://pypi.org/project/opencv-python/) is not yet installed for Python, install it with `pip install opencv-python`.

```python
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
```

With the run start and end times as variables, create the empty run container:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

engine_fire_run = client.create_run(
    name="Engine Fire Run",
    start=start_time,
    end=end_time,
    description="Run for Raptor engine fire.",
)

engine_fire_run
```

### Attach the dataset to a run

Use {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`
to add the dataset carrying the video channel to the run:

```python
engine_fire_run.add_dataset(
    ref_name="engine fire video",
    dataset=dataset,
)
```

```{include} /guides/_snippets/what-is-a-ref-name.md
```

## Retrieve a video

Retrieve the dataset with its RID (resource ID), which can be copy pasted for any dataset on the
[Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets), then list its video files:

```python
dataset = client.get_dataset(
    "ri.catalog.cerulean-staging.dataset.d6c6e12c-05b3-4bb0-9f45-97609b7c9da2"
)

for video_file in dataset.list_video_files():
    print(video_file.name, video_file.media_duration_seconds, "seconds")
```

## Archive a video

Archiving the dataset prevents it from displaying on the
[Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets).

```python
dataset.archive()
```

To unarchive it:

```python
dataset.unarchive()
```

Now the dataset will display again on the [Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets).

To remove a single video file rather than the whole dataset, call `delete()` on the file returned by
`Dataset.add_video()` or `Dataset.list_video_files()`.

## Appendix

### Display video inline

```{include} /guides/_snippets/inline-video.md
```

### Inspect video metadata with OpenCV

The free [OpenCV](https://pypi.org/project/opencv-python/) Python library is invaluable for video inspection
and low-level video editing.

Below is a simple script that demonstrates how to capture basic video file properties such as
video duration, frames per second, frame size, and total frame count.

You can install OpenCV for Python with `pip install opencv-python`.

```python
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
```

```
Video Size: 640x360 (width x height)
FPS: 30.0
Total Frames: 150
Duration: 5.00 seconds
```

If interested in more advanced video processing in Python, check out the [object identification guide](/guides/video/object-identification.md).
