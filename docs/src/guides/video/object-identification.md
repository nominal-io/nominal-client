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

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_1.py
:language: python
```

```{include} /guides/_snippets/sample-data.md
```

Since the dataset is a video file, rename `dataset_path`:

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_2.py
:language: python
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

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_3.py
:language: python
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
```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_5.py
:language: python
```

<table border="1" class="dataframe"><thead><tr><th>frame</th><th>object</th><th>score</th><th>x_min</th><th>y_min</th><th>x_max</th><th>y_max</th></tr><tr><td>i64</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>5</td><td>&quot;car&quot;</td><td>0.444338</td><td>973.15</td><td>350.76</td><td>1172.47</td><td>369.04</td></tr><tr><td>5</td><td>&quot;car&quot;</td><td>0.403373</td><td>433.44</td><td>351.53</td><td>531.85</td><td>368.34</td></tr><tr><td>5</td><td>&quot;motorbike&quot;</td><td>0.351543</td><td>298.63</td><td>350.76</td><td>438.73</td><td>369.57</td></tr><tr><td>6</td><td>&quot;motorbike&quot;</td><td>0.40321</td><td>308.95</td><td>348.02</td><td>437.47</td><td>369.22</td></tr><tr><td>6</td><td>&quot;car&quot;</td><td>0.400091</td><td>432.22</td><td>349.05</td><td>533.11</td><td>367.95</td></tr></tbody></table>

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_6.py
:language: python
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

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_7.py
:language: python
```

### Upload video

Upload the video file as a **video channel** on the feature dataset you just created, using
{py:obj}`Dataset.add_video() <nominal.core.Dataset.add_video>`.
Keeping the video and the extracted features on one dataset puts them in the same time domain, so
they line up in a workbook without any further wiring.

First, the video must be in one of the formats that the platform accepts (MKV, MP4, MP2T).
For this step, you will need `ffmpeg` installed (see [Correct video format](/guides/video/video-ingest.md#correct-video-format)).

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_8.py
:language: python
```

Video upload requires a start time. If the start time of your video capture is not
important, you can choose an arbitrary time like `datetime.now()` or `2011-11-11 11:11:11`.
Since Nominal uses timestamps to cross-correlate between datasets, make sure that
whichever start time you choose makes sense for the other datasets in the run.

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_9.py
:language: python
```

### Create an empty run

```{include} /guides/_snippets/what-is-a-run.md
```

Set the run start and end times with minimum and maximum values from the `timestamp` column.

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_10.py
:language: python
```

### Add dataset & video to run

Add the dataset - which now carries both the extracted features and the video channel - to the run
with {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_11.py
:language: python
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

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_12.py
:language: python
```

### Step through video frames

The below script steps through each frame in the video and uses `get_objects_from_pil_image()` (see above)
to identify each object. Each identified object is added as a row to the Polars dataframe `df_video`.

:::{warning}

Depending on the length of your video, its resolution, and your machine, this script can take several hours to run. To process 30min of footage on an M3 Macbook, expect at least an hour.
:::

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_13.py
:language: python
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

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_15.py
:language: python
```

#### Object count

The script below adds columns that count each object per video frame.
For example, if the `boat_count` column is 6, then 6 boats were identified in that frame.

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_16.py
:language: python
```

### Annotate video

Finally, the below script adds a color-coded bounding box and label to each object identified in each frame.
The result is a fully annotated video.

```{literalinclude} /guides/_snippets/code/sdk/python/video/object_identification_17.py
:language: python
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1727580510/montage_video_owslrd.mov
```

(For faster loading, only 20s of the [full 225 MB video](https://huggingface.co/datasets/nominal-io/drone-flight-object-identification/blob/main/all_scores_bounding_box_output.mov) is shown above).
