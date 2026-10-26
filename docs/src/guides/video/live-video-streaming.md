---
myst:
  html_meta:
    description: "Stream live video from cameras, files, networks or your own code to a video channel."
---

# Live video streaming

{.lead}
{py:class}`~nominal.experimental.video.VideoStream` sends live video to a video channel on a dataset, from a camera, a file, a network stream or your own code.
Each frame carries its capture time, so the video plays in sync with telemetry in [workbooks](https://docs.nominal.io/core/documentation/platform/workbooks/live-video-streaming).

The examples also need the [`video` extra](/guides/install.md#extras).
Live video is experimental, so the API may change between releases.

```{include} /guides/_snippets/connect-the-client.md
```

## Install GStreamer

The `video` extra also needs [GStreamer](https://gstreamer.freedesktop.org/) 1.20 or later:

::::{tab-set}

:::{tab-item} macOS

```shell
brew install gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly libnice-gstreamer
```
:::

:::{tab-item} Ubuntu / Debian

```shell
sudo apt install gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav gstreamer1.0-nice libssl3
```
:::

:::{tab-item} Windows

Install the runtime from the [GStreamer downloads page](https://gstreamer.freedesktop.org/download/#windows).
:::
::::

## Stream a camera

```python
from nominal.experimental.video import Src, VideoStream

dataset = client.create_dataset("Live camera")
with VideoStream.create(dataset, Src.camera(), channel="camera") as stream:
    stream.run(30)  # seconds
```

Video goes to Nominal over WebRTC, using the WHIP protocol.

## Sources

Raw sources are encoded with the stream's options. Encoded sources pass through as they are.

| Source | Type | Input |
| --- | --- | --- |
| `Src.camera(device_index=0)` | Raw | A local camera |
| `Src.display(monitor_index=0)` | Raw | The screen |
| `Src.file(location)` | Raw | A video file, such as MP4 or MKV |
| `Src.images(path, fps)` | Raw | A folder of PNG or JPEG images |
| `Src.test()` | Raw | A test pattern |
| `Src.app(width, height, format="RGB")` | Raw | Pixel frames from your code |
| `Src.app_image(format)` | Raw | JPEG or PNG bytes from your code |
| `Src.app_encoded(codec=Codec.H264)` | Encoded | H.264 or H.265 frames from your code |
| `Src.rtsp(url)` | Encoded | An RTSP stream |
| `Src.udp_rtp(port)`, `Src.udp_mpegts(port)` | Encoded | RTP or MPEG-TS over UDP |
| `Src.custom(description)` | Encoded | A GStreamer bin that ends with a parser such as `h264parse` |

## Send frames from your code

`Src.app` takes raw pixels. With `auto_timestamp=False`, give each frame its capture time in epoch nanoseconds:

```python
import time

with VideoStream.create(dataset, Src.app(640, 480, format="RGB", auto_timestamp=False), channel="custom") as stream:
    stream.send_frame(frame_bytes, timestamp_ns=time.time_ns())  # 640 × 480 × 3 bytes
```

By default, `auto_timestamp=True` stamps frames with the time they're sent.

## Stream options

`StreamOptions` sets encoding and reconnection:

```python
from nominal.experimental.video import Codec, ReconnectOptions, StreamOptions

opts = StreamOptions(
    codec=Codec.H264,  # or Codec.H265
    bitrate=4000,  # kbps
    resolution="1280x720",
    fps=30,
    keyframe_interval=30,  # frames
    overlay=False,  # draw a timestamp on the video
    reconnect=ReconnectOptions(retry_delay_s=2.0, max_retries=None),  # None retries forever
)
with VideoStream.create(dataset, Src.camera(), opts, channel="camera") as stream:
    stream.run(30)
```

`bitrate`, `resolution`, `fps`, `overlay`, `flip` and `crop` apply to raw sources.
For encoded sources, set `transcode=True` to decode and encode them again so the options apply.
`congestion_control`, on by default, lowers the bitrate when packets are lost.

## View the stream

To watch the stream in a workbook, see [Live video in workbooks](https://docs.nominal.io/core/documentation/platform/workbooks/live-video-streaming).
