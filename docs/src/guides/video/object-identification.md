---
myst:
  html_meta:
    description: "Upload object detections from a video next to the video itself, so you can review them frame by frame."
---

# Object identification

{.lead}
Upload a video with the objects a computer vision model found in each frame, such as counts and confidence scores.
In a run, the detections chart in sync with the video, and [checklists](/guides/checklists/authoring.md) can flag frames that need review.

The examples also need `pip install polars huggingface_hub`.

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727580510/montage_video_owslrd.mov
```

```{include} /guides/_snippets/connect-the-client.md
```

## Download the sample

The sample is drone footage annotated by the [RT-DETR](https://huggingface.co/docs/transformers/en/model_doc/rt_detr) model, and a CSV with one row per detected object.
The video is 225 MB.

```python
import polars as pl
from huggingface_hub import hf_hub_download

repo = {"repo_id": "nominal-io/drone-flight-object-identification", "repo_type": "dataset"}
video_path = hf_hub_download(filename="all_scores_bounding_box_output.mov", **repo)
detections = pl.read_csv(hf_hub_download(filename="object_detection_metadata.csv", **repo))
```

Each row has the frame number, timestamp, object label, confidence `score`, bounding box, and per-frame counts such as `car_count` and `total_object_count`.
The first frame is at `2011-11-11T11:11:11` UTC.

## Upload the detections and the video

Put both in one dataset so they share timestamps.
Convert the video first, as in [Video ingest](/guides/video/video-ingest.md#convert-the-video):

```python
from datetime import datetime, timezone

from nominal.experimental.video_processing import normalize_video
from nominal.thirdparty.pandas import upload_dataframe

dataset = upload_dataframe(
    client,
    detections.to_pandas(),
    "Drone flight detections",
    "timestamps",
    timestamp_type="iso_8601",
)

normalize_video(video_path, "drone.mp4")
start = datetime(2011, 11, 11, 11, 11, 11, tzinfo=timezone.utc)
dataset.add_video("drone.mp4", channel="rt_detr_video", start=start).poll_until_ingestion_completed()
```

The video's `start` must match the detections' first timestamp, or the two won't line up.

## Review the detections

Create a run over the detections' time range:

```python
end = datetime.fromisoformat(str(detections["timestamps"].max())).replace(tzinfo=timezone.utc)
run = client.create_run("RT-DETR drone analysis", start=start, end=end)
run.add_dataset("rt_detr", dataset)
print(run.nominal_url)
```

In a workbook on the run, add a video panel for `rt_detr_video` and charts of the counts and scores.

```{video} /guides/images/b2/CleanShot_2024-09-25_at_18.35.32_nbtuko.mp4
:loop:
:alt: A workbook syncing object counts and confidence scores with the annotated video
```

## Detect objects in your own video

To make a detections table from your own video, read its frames with [OpenCV](https://pypi.org/project/opencv-python/) and run a model on each.
This example uses RT-DETR from [transformers](https://huggingface.co/docs/transformers/), and needs `pip install opencv-python torch transformers pillow`.
Long videos can take hours.

```python
from datetime import timedelta

import cv2
import torch
from PIL import Image
from transformers import RTDetrForObjectDetection, RTDetrImageProcessor

processor = RTDetrImageProcessor.from_pretrained("PekingU/rtdetr_r50vd")
model = RTDetrForObjectDetection.from_pretrained("PekingU/rtdetr_r50vd")

rows = []
cap = cv2.VideoCapture("my_video.mp4")
while True:
    ok, frame = cap.read()
    if not ok:
        break
    timestamp = start + timedelta(milliseconds=cap.get(cv2.CAP_PROP_POS_MSEC))
    image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
    with torch.no_grad():
        outputs = model(**processor(images=image, return_tensors="pt"))
    result = processor.post_process_object_detection(
        outputs, target_sizes=torch.tensor([image.size[::-1]]), threshold=0.3
    )[0]
    for score, label in zip(result["scores"], result["labels"]):
        rows.append({"timestamp": timestamp, "object": model.config.id2label[label.item()], "score": score.item()})
cap.release()

detections = pl.DataFrame(rows)
```

Count objects per frame with Polars, such as `detections.group_by("timestamp", "object").len()`, and upload the result as above.
