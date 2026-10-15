---
myst:
  html_meta:
    description: "Use the latest in ML to ID objects in Python before uploading video to"
---

# Object identification in Python

{.lead}
Use the latest in ML to ID objects in Python before uploading video to

Object detection is crucial in autonomy and manufacturing for enhancing efficiency, safety,
and precision. In autonomous systems like vehicles or robots, it enables real-time navigation,
obstacle avoidance, and interaction with dynamic environments. In manufacturing,
object detection facilitates automation by enabling machines to recognize, sort,
and manipulate parts, improving quality control, productivity, and reducing human error.

```{include} /guides/_snippets/about-nominal-video.md
```

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Download video files

```python
dataset_repo_id = "nominal-io/drone-flight-object-identification"
dataset_filename = "all_scores_bounding_box_output.mov"
```

```{include} /guides/_snippets/sample-data.md
```

Since the dataset is a video file, rename `dataset_path`:

```python
video_path = dataset_path
```

## Display video

```{include} /guides/_snippets/inline-video.md
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727580510/montage_video_owslrd.mov
```

(For faster loading, only 20s of the [full 225 MB video](https://huggingface.co/datasets/nominal-io/drone-flight-object-identification/blob/main/all_scores_bounding_box_output.mov) is shown above).

## Inspect video metadata

After downloading the video, you can inspect its properties with [OpenCV](https://pypi.org/project/opencv-python/).
Download OpenCV with `pip3 install opencv-python`.

```python
import cv2

cap = cv2.VideoCapture(video_path)
frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
cap.release()

print(f"Total number of frames: {frame_count}")
```

```
Total number of frames: 6591
```

## Extracted CV data

For convenience, the computer vision ("CV") features extracted from this video are available on Nominal's
[Hugging Face](https://huggingface.co/datasets/nominal-io/drone-flight-object-identification).

[RT-DETR](https://huggingface.co/docs/transformers/en/model_doc/rt_detr), a pre-trained ML model,
was used to generate this data. If you're interested in how to extract computer vision data for
your own video, please see the Appendix.

Try an example to download and inspect this data. First, install [Polars](https://pola.rs/) and [PyArrow](https://arrow.apache.org/docs/python/) if not yet installed:

```bash
pip3 install polars pyarrow
```

Next, run this code block to download and inspect the data:
```python
import polars as pl

df_computer_vision = pl.read_csv(
    "hf://datasets/nominal-io/drone-flight-object-identification/object_detection_metadata.csv"
)
df_computer_vision.head().select(df_computer_vision.columns[:7])
```

<table border="1" class="dataframe"><thead><tr><th>frame</th><th>object</th><th>score</th><th>x_min</th><th>y_min</th><th>x_max</th><th>y_max</th></tr><tr><td>i64</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>5</td><td>&quot;car&quot;</td><td>0.444338</td><td>973.15</td><td>350.76</td><td>1172.47</td><td>369.04</td></tr><tr><td>5</td><td>&quot;car&quot;</td><td>0.403373</td><td>433.44</td><td>351.53</td><td>531.85</td><td>368.34</td></tr><tr><td>5</td><td>&quot;motorbike&quot;</td><td>0.351543</td><td>298.63</td><td>350.76</td><td>438.73</td><td>369.57</td></tr><tr><td>6</td><td>&quot;motorbike&quot;</td><td>0.40321</td><td>308.95</td><td>348.02</td><td>437.47</td><td>369.22</td></tr><tr><td>6</td><td>&quot;car&quot;</td><td>0.400091</td><td>432.22</td><td>349.05</td><td>533.11</td><td>367.95</td></tr></tbody></table>

```python
df_computer_vision.head().select(df_computer_vision.columns[7:12])
```

<table border="1" class="dataframe"><thead><tr><th>timestamps</th><th>total_object_count</th><th>motorbike_count</th><th>car_count</th><th>person_count</th></tr><tr><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>&quot;2011-11-11T11:11:11.208333&quot;</td><td>3</td><td>1</td><td>2</td><td>0</td></tr><tr><td>&quot;2011-11-11T11:11:11.208333&quot;</td><td>3</td><td>1</td><td>2</td><td>0</td></tr><tr><td>&quot;2011-11-11T11:11:11.208333&quot;</td><td>3</td><td>1</td><td>2</td><td>0</td></tr><tr><td>&quot;2011-11-11T11:11:11.250000&quot;</td><td>6</td><td>2</td><td>3</td><td>1</td></tr><tr><td>&quot;2011-11-11T11:11:11.250000&quot;</td><td>6</td><td>2</td><td>3</td><td>1</td></tr></tbody></table>

As you can see, each row of this table represents an object that the ML model
([RT-DETR](https://huggingface.co/docs/transformers/en/model_doc/rt_detr)) identified.

Specifically, each row includes a label for the object identified, the video frame number,
the object's position within the frame, the video frame's timestamp, and a count for all
other objects identified in the same frame.

Upload this data and video to Nominal for data review and automated check authoring.

## Upload to Nominal

Upload both the annotated video and extracted features dataset, then group them together as a *Run.*

```{include} /guides/_snippets/what-is-a-run.md
```

### Upload dataset

Since the extracted features CSV is already in a [Polars](https://pola.rs/)
dataframe, convert it with `.to_pandas()` and upload it to Nominal with the `upload_dataframe()` function from `nominal.thirdparty.pandas`.

```python
import nominal.thirdparty.pandas as npm
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

dataset = npm.upload_dataframe(
    client,
    df_computer_vision.to_pandas(),
    name="Computer Vision Features: Drone Festival Flight",
    timestamp_column="timestamps",
    timestamp_type="iso_8601",
)

print("Uploaded dataset:", dataset.rid)
```

### Upload video

Upload the video file as a **video channel** on the feature dataset you just created, using
{py:obj}`Dataset.add_video() <nominal.core.Dataset.add_video>`.
Keeping the video and the extracted features on one dataset puts them in the same time domain, so
they line up in a workbook without any further wiring.

First, the video must be in one of the formats that the platform accepts (MKV, MP4, MP2T).
For this step, you will need `ffmpeg` installed (see [Correct video format](/guides/video/video-ingest.md#correct-video-format)).

```python
from pathlib import Path

from nominal.experimental.video_processing import normalize_video

mp4_video_path = video_path.replace(".mov", ".mp4")

normalize_video(Path(video_path), Path(mp4_video_path))
```

Video upload requires a start time. If the start time of your video capture is not
important, you can choose an arbitrary time like `datetime.now()` or `2011-11-11 11:11:11`.
Since Nominal uses timestamps to cross-correlate between datasets, make sure that
whichever start time you choose makes sense for the other datasets in the run.

```python
from datetime import datetime

# The features dataset already exists, so add the video to it as a channel.
# Both now share one time domain, and one dataset carries the whole analysis.
video_file = dataset.add_video(
    path=mp4_video_path,
    channel="rt_detr_video",
    start=datetime.strptime("2011-11-11 11:11:11", "%Y-%m-%d %H:%M:%S"),
)

print(f"Video file created: {video_file.id}")
```

### Create an empty run

```{include} /guides/_snippets/what-is-a-run.md
```

Set the run start and end times with minimum and maximum values from the `timestamp` column.

```python
computer_vision_run = client.create_run(
    name="RT-DETR model analysis",
    start=df_computer_vision["timestamps"].min(),
    end=df_computer_vision["timestamps"].max(),
    description="Run analysis of RT-DETR model output on single drone flight footage.",
)

computer_vision_run
```

### Add dataset & video to run

Add the dataset - which now carries both the extracted features and the video channel - to the run
with {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.

```python
computer_vision_run.add_dataset(
    ref_name="rt_detr_analysis",
    dataset=dataset,
)
```

On the [Nominal runs page](https://app.gov.nominal.io/runs), click on *"RT-DETR model analysis"* (login required).
If you go to the *"Data sources"* tab of the run, you'll now see the Video and CSV file associated with this run:

![run-datasources](/guides/images/b2/a4bce67a-8c99-4795-a5f3-fb403330d562.png)

## Create a workbook

Now that your data is organized in a run, it's easy to create a workbook for ad-hoc analysis on the Nominal platform.

This workbook synchronizes the extracted feature data with the playback of the annotated video.
Feature data like *object count* and ML model *confidence score* can be inspected frame-by-frame.
Checks that signal anomalous behavior can also be defined and applied to future video ingests.

```{video} /guides/images/b2/CleanShot_2024-09-25_at_18.35.32_nbtuko.mp4
:loop:
:alt: A workbook syncing object counts and confidence scores with the annotated video
```

## Appendix

This section outlines the general steps for applying a pre-trained ML model to a video. The model chosen
is [RT-DETR](https://huggingface.co/docs/transformers/en/model_doc/rt_detr) - an object identification model.
Other types of ML image models can be applied as well (such as depth detection, temperature analysis, etc).
Choose an ML model or video analysis technique that is most helpful for your hardware testing goals.
Please [contact our team](https://nominal.io/request-demo) if you'd like to discuss!

For automating the ingestion of computer vision artifacts in Nominal, please see the previous section.

### Identify objects per frame

The function below takes a [PIL image](https://pillow.readthedocs.io/en/stable/reference/Image.html)
and returns a Polars dataframe with all of the objects in the image identified.

Use this function to step through the video frame-by-frame and identify each object.

```python
import polars as pl
import torch
from PIL import Image
from transformers import RTDetrForObjectDetection, RTDetrImageProcessor


def get_objects_from_pil_image(image, frame):
    """
    This function takes an image in PIL format and returns a Polars dataframe with all of the identified objects
    """
    schema = {
        "frame": pl.Int64,  # Column 'frame' as integer
        "object": pl.Utf8,  # Column 'object' as string
        "score": pl.Float64,  # Column 'score' as float
        "x_min": pl.Float64,  # Column 'x_min' as integer
        "y_min": pl.Float64,  # Column 'y_min' as integer
        "x_max": pl.Float64,  # Column 'x_max' as integer
        "y_max": pl.Float64,  # Column 'y_max' as integer
    }
    df_video_frame = pl.DataFrame(schema=schema)

    image_processor = RTDetrImageProcessor.from_pretrained("PekingU/rtdetr_r50vd")
    model = RTDetrForObjectDetection.from_pretrained("PekingU/rtdetr_r50vd")

    inputs = image_processor(images=image, return_tensors="pt")

    with torch.no_grad():
        outputs = model(**inputs)

    results = image_processor.post_process_object_detection(
        outputs, target_sizes=torch.tensor([image.size[::-1]]), threshold=0.3
    )

    for result in results:
        for score, label_id, box in zip(
            result["scores"], result["labels"], result["boxes"]
        ):
            score, label = score.item(), label_id.item()
            box = [round(i, 2) for i in box.tolist()]
            new_row = pl.DataFrame(
                {
                    "frame": [frame],
                    "object": [model.config.id2label[label]],
                    "score": [score],
                    "x_min": [box[0]],
                    "y_min": [box[1]],
                    "x_max": [box[2]],
                    "y_max": [box[3]],
                }
            )
            df_video_frame = pl.concat([df_video_frame, new_row])

    return df_video_frame
```

### Step through video frames

The below script steps through each frame in the video and uses `get_objects_from_pil_image()` (see above)
to identify each object. Each identified object is added as a row to the Polars dataframe `df_video`.

:::{warning}

Depending on the length of your video, its resolution, and your machine, this script can take several hours to run. To process 30min of footage on an M3 Macbook, expect at least an hour.
:::

```python
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
```

```
Number of objects identified: 291558
```

In less than 5 minutes of video, the RT-DETR model identified almost 300k objects!

### Enrich metadata

The scripts below add timestamp and object count columns to `df_video`.

#### Timestamp column

`df_video` only has a frame count column. This script adds a `timestamp` column and
assigns each frame an absolute time (starting with '2011-11-11 11:11:11' for the first frame).

```{include} /guides/_snippets/video-start-times.md
```

```python
from datetime import datetime, timedelta

import cv2
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
```

#### Object count

The script below adds columns that count each object per video frame.
For example, if the `boat_count` column is 6, then 6 boats were identified in that frame.

```python
object_count_data = dict(
    frame=[],
    total_object_count=[],
    motorbike_count=[],
    car_count=[],
    person_count=[],
    boat_count=[],
    bus_count=[],
    truck_count=[],
)

for frame_count in range(df_video_with_timestamps["frame"].max()):
    df_single_frame = df_video_with_timestamps.filter(pl.col("frame") == frame_count)
    object_count_data["frame"].append(frame_count)
    object_count_data["total_object_count"].append(len(df_single_frame))
    object_count_data["motorbike_count"].append(
        len(df_single_frame.filter(pl.col("object") == "motorbike"))
    )
    object_count_data["car_count"].append(
        len(df_single_frame.filter(pl.col("object") == "car"))
    )
    object_count_data["person_count"].append(
        len(df_single_frame.filter(pl.col("object") == "person"))
    )
    object_count_data["boat_count"].append(
        len(df_single_frame.filter(pl.col("object") == "boat"))
    )
    object_count_data["bus_count"].append(
        len(df_single_frame.filter(pl.col("object") == "bus"))
    )
    object_count_data["truck_count"].append(
        len(df_single_frame.filter(pl.col("object") == "truck"))
    )

df_object_count = pl.DataFrame(object_count_data)
df_video_w_object_count = df_video_with_timestamps.join(df_object_count, on="frame")

df_video_w_object_count.head()
```

### Annotate video

Finally, the below script adds a color-coded bounding box and label to each object identified in each frame.
The result is a fully annotated video.

```python
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
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727580510/montage_video_owslrd.mov
```

(For faster loading, only 20s of the [full 225 MB video](https://huggingface.co/datasets/nominal-io/drone-flight-object-identification/blob/main/all_scores_bounding_box_output.mov) is shown above).
