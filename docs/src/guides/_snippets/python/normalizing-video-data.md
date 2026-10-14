For a video file to be successfully ingested into Nominal, it needs to satisfy the following conditions:

- be H264 or H265 encoded,
- use YUV420p color space, and
- contain an audio track.

It is also recommended that the video has reasonably-spaced key-frames (at most ~10s between key-frames) for optimal playback performance.

:::{note}

These constraints do not apply for [MCAP-based Videos](/guides/video/mcap-video-ingest.md).
:::

To help customers ingest a wide variety of video formats, the Python client provides the following utility function:

```{literalinclude} /guides/_snippets/code/data_upload/normalizing_video_data.py
:language: python
```

::::{note}

To use this functionality, you must have `ffmpeg` installed.
You can find [pre-built binaries on their website](https://www.ffmpeg.org/download.html), or install it via your operating system package manager.

:::{warning}

If you intend to **distribute** the 'ffmpeg' binary, e.g. along with your scripts, you should also include its source code, as per the terms of [FFMPEG's GPLv3 license](https://ffmpeg.org/legal.html).
:::
::::

:::::{tip}

Need help determining what video encoding settings you are using?
Off the shelf tools such as [VLC](https://www.videolan.org/vlc/) can be used to inspect the codec information of a video file.

Here are some examples of the VLC "Media Information" window showing codec details of common situations with videos

::::{dropdown} Video that would successfully ingest into Nominal

:::{figure} /guides/images/python/good_video.png
:alt: VLC Media Information

This video is ready to go!
:::
::::

::::{dropdown} Video that has an unsupported color space

:::{figure} /guides/images/python/bad_color_space_h264.png
:alt: VLC Media Information

This video is using ITU-R BT.709 instead of YUV420P
:::
::::

::::{dropdown} Video that has an unsupported video codec

:::{figure} /guides/images/python/unsupported_codec_av1.png
:alt: VLC Media Information

This video is using av1 instead of h264 or h265
:::
::::

::::{dropdown} Video that is missing an audio track

:::{figure} /guides/images/python/no_audio_stream.png
:alt: VLC Media Information

This video doesn't have an audio track
:::
::::
:::::
