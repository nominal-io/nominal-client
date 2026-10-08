---
myst:
  html_meta:
    description: "Save and upload chart animations as video with Python and Nominal"
---

# Save plot animations as video in Python

{.lead}
Save and upload chart animations as video with Python and Nominal

For time-resolved simulations, you may want to save a dynamic (animated) plot
as a video so that it can be played back in sync with other simulation variables.
This guide describes how to accomplish this with the "double pendulum simulation" as an example.

```{include} /guides/_snippets/about-nominal-video.md
```

This guide demonstrates how to upload a video file to Nominal in Python.
Other guides in the Video section demonstrate basic Python video analysis
and computer vision for video pre-processing prior to Nominal upload.

## Double pendulum simulation

The double pendulum is a classic physics problem: two pendulums attached end to end, where
the motion of one pendulum influences the other, creating complex, chaotic dynamics. The
system demonstrates how even simple systems can exhibit unpredictable behavior due to
sensitivity to initial conditions.

If you've never seen the double pendulum simulation, below is a time-resolved animation of its solution.
Note that the simulation length is 25 seconds.

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_1.py
:language: python
```

```{video} /guides/images/b2/CleanShot_2024-10-17_at_19.05.06_nnna1m.mp4
:loop:
:alt: double-pendulum
```

## Convert animation to video

To convert the Plotly animation to a video, install the `imageio` library with the `ffmpeg` plugin:

`pip3 install imageio[ffmpeg]`

Prior to running the above command, you'll also want to install the underlying `ffmpeg` library.
On a Mac you can install this with `brew install ffmpeg`. For installation instructions for
other operating systems, please see the official [FFmpeg download page](https://www.ffmpeg.org/download.html).

For more information about this plugin, please refer to the
[imageio docs](https://imageio.readthedocs.io/en/v2.10.0/reference/_backends/imageio.plugins.ffmpeg.html).

Now modify the simulation code above to save an image for each animation frame
with Plotly's `write_image()` function. At the end of the simulation, stitch the
images together into a video with imageio's
[`get_writer()`](https://imageio.readthedocs.io/en/v2.9.0/userapi.html#imageio.get_writer) function.

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_2.py
:language: python
```

Note that the simulation video has been saved to *double_pendulum.mp4*.

## Display video inline

```{include} /guides/_snippets/inline-video.md
```

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1729218249/double_pendulum_ozyjtj.mp4
```

## Save simulation channels

Run the same simulation a final time to extract channels such as the pendulum
fulcrum's heights, momentums, PEs and KEs.

:::{note}

Simulations are often time-consuming to run. In practice, extract the
simulation result, video frames, and important channel values in a single pass. For the pedagogical
purposes of this tutorial, the extraction of these 3 artifacts is separated into 3
identical but separate simulation runs.
:::

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_3.py
:language: python
```

![simulation-dataframe](/guides/images/b2/8b65a3ab-469e-407f-bdb9-5fa2a6a6a900.png)

The channel data is saved in a CSV file: *double_pendulum_full_headers.csv*.

Finally, upload the channels and video to Nominal so they can be visualized together.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create a run

Start by creating a run to attach the video and channel data to.

```{include} /guides/_snippets/what-is-a-run.md
```

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_4.py
:language: python
```

## Add video to the dataset

Nominal video uploads need an absolute start time. Grab this start time from the channel dataframe:

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_5.py
:language: python
```
`2024-10-16T22:47:54.600400`

Upload the video as a channel on the same dataset that holds the pendulum measurements:

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_6.py
:language: python
```

That dataset is already attached to the run, so the new video channel appears there as soon as it
finishes ingesting:

```{literalinclude} /guides/_snippets/code/sdk/python/video/video_animations_7.py
:language: python
```

## Adjust Video Start

Nominal has many tools to precisely sync video playback with channel data, which is typically
extracted from the video or recorded independently at the same time.

If you select the dataset on the [datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets)
(login required), then you can adjust metadata such as a video file's start timestamp. In Python,
use `VideoDatasetFile.update(starting_timestamp=...)` on a file from `Dataset.list_video_files()`.

![video-start-date](/guides/images/b2/31f8a27b-ba94-45fb-803b-92ffd524a814.png)

## Create a workbook

The simulation video and channel data are now uploaded on the Nominal platform,
where they can be collaboratively analyzed and benchmarked against real-world data.

Below is an example Nominal workbook that syncs the pendulum fulcrum heights with the pendulum simulation video playback:

```{video} /guides/images/b2/CleanShot_2024-10-17_at_09.12.40_hoivoh.mp4
:loop:
:alt: simulation-workbook
```

Other channels such as pendulum fulcrum momentum, acceleration, and kinetic energy can also be plotted. Workbook transforms and formulas
can be used to calculate derived channels such as the RMS acceleration.

As an exercise, consider solving the simulation with a different ODE solver and comparing the results in a Nominal workbook.
