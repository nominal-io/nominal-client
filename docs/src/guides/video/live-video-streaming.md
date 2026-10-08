---
myst:
  html_meta:
    description: "Stream live video from any source to the Nominal platform using the Nominal Python SDK."
---

# Live video streaming

{.lead}
Stream live video from any source to the Nominal platform using the Nominal Python SDK.

Nominal supports live video streaming, allowing you to broadcast a video feed directly to the platform in real time.
Live video streams can be viewed alongside telemetry data in [workbooks](https://docs.nominal.io/core/documentation/platform/workbooks/live-video-streaming), enabling synchronized analysis of visual and sensor data during active tests.

## How it works

The Nominal Python SDK's video module connects a **source** to a **sink** and handles encoding, timestamping, and transport in between.
Video is delivered to the Nominal platform over **WebRTC** using the WHIP (WebRTC-HTTP Ingestion Protocol) standard.

On the viewer side, workbooks connect to the stream using WHEP (WebRTC-HTTP Egress Protocol) for low-latency playback directly in the browser.

Every frame carries an absolute wall-clock timestamp (nanoseconds) through the entire pipeline, so video stays synchronized with telemetry data in workbooks.

Sources are either **raw** or **encoded**:

- **Raw sources** (`Src.camera`, `Src.test`, `Src.app`, `Src.app_image`, `Src.file`, `Src.images`, `Src.display`). The SDK encodes the video using the codec and options you specify.
- **Encoded sources** (`Src.udp_rtp`, `Src.udp_mpegts`, `Src.rtsp`, `Src.custom`, `Src.app_encoded`). The video is already compressed and is passed through without re-encoding.

## Prerequisites

Live video streaming requires [GStreamer](https://gstreamer.freedesktop.org/).

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

Download and install the GStreamer runtime libraries from the [GStreamer downloads page](https://gstreamer.freedesktop.org/download/#windows).
:::
::::

Install the Nominal Python SDK with the video extra:

```shell
pip install nominal[video]
```

## Start a basic live stream

```python
from nominal.core import NominalClient
from nominal.experimental.video import VideoStream, Src

client = NominalClient.from_profile("default")
dataset = client.create_dataset("live stream test")

with VideoStream.create(dataset, Src.camera(), channel="camera") as stream:
    stream.run(30)
```

This creates a video channel on a Nominal dataset and streams live frames from your default camera for 30 seconds.

## Video sources

The `Src` class provides multiple input sources for live streaming.

| Source | Description |
|--------|-------------|
| `Src.test()` | GStreamer test pattern |
| `Src.camera(device_index=0, resolution=None)` | Local camera (`/dev/video0` on Linux, AVFoundation on macOS, DirectShow on Windows) |
| `Src.udp_rtp(port)` | Receive RTP over UDP |
| `Src.udp_mpegts(port)` | Receive MPEG-TS over UDP |
| `Src.rtsp(url)` | Pull from an RTSP stream |
| `Src.file(location, start_ns=None)` | Play from a video file (MP4, MKV, etc.) |
| `Src.images(path, fps, start_ns=None)` | Stream a directory of PNG/JPEG images at a given framerate |
| `Src.app(width, height, format="RGB", auto_timestamp=True)` | Push raw pixel frames from Python |
| `Src.app_encoded(codec=Codec.H264)` | Push pre-encoded frames from Python (H.264 or H.265). Skips encoding |
| `Src.app_image(format)` | Push JPEG or PNG image bytes from Python (`ImageFormat.Jpeg` or `ImageFormat.Png`). Decoded automatically |
| `Src.display(monitor_index=0)` | Capture the screen |
| `Src.custom(description)` | Arbitrary GStreamer bin ending with a parser (e.g. `h264parse`) |

## Stream options

Use `StreamOptions` to control encoding, resolution, framerate, and more:

```python
from nominal.core import NominalClient
from nominal.experimental.video import VideoStream, Src, StreamOptions, ReconnectOptions

client = NominalClient.from_profile("default")
dataset = client.create_dataset("live stream test")

opts = StreamOptions(
    codec="H264",              # H264 (default) or H265
    bitrate=4000,             # encoding bitrate in kbps
    resolution="1280x720",    # e.g. "1280x720" — rescale before encoding
    overlay=False,            # draw a timestamp overlay on the video
    fps=30,                   # cap framerate
    keyframe_interval=30,     # frames between keyframes
    flip=None,                # FlipMode.Horizontal/Vertical/Rotate90/Rotate180/Rotate270
    crop=None,                # Crop(top=0, bottom=0, left=0, right=0) — pixels to remove
    reconnect=ReconnectOptions(
        retry_delay_s=2.0,        # seconds to wait between rebuild attempts
        max_retries=None,         # max attempts before giving up; None = retry forever
        disconnect_grace_s=2.0,   # (WHIP only) seconds WHIP may stay disconnected before rebuild
    ),
    transcode=False,          # decode + re-encode encoded sources so options above take effect
    congestion_control=True,  # adjust bitrate on packet loss/PLI (WHIP only)
)

with VideoStream.create(dataset, Src.camera(), opts, channel="camera") as stream:
    stream.run(30)
```

`bitrate`, `resolution`, `overlay`, `fps`, `flip`, and `crop` only apply to **raw** sources.
For **encoded** sources, set `transcode=True` to decode and re-encode so these options take effect.

## Sending frames from Python

Use `Src.app()` to push raw pixel frames directly from Python.
This is useful when you already have frame data in memory. For example, from a custom capture pipeline or image processing code.

```python

from nominal.core import NominalClient
from nominal.experimental.video import VideoStream, Src

client = NominalClient.from_profile("default")
dataset = client.create_dataset("custom frame stream")

with VideoStream.create(
    dataset, Src.app(width=640, height=480, format="RGB", auto_timestamp=False), channel="custom"
) as stream:
    timestamp_ns = time.time_ns()
    stream.send_frame(frame_buffer, timestamp_ns=timestamp_ns)
```

`frame_buffer` is a `bytes` object containing raw pixel data (640 × 480 × 3 bytes for RGB).
`timestamp_ns` is an absolute wall-clock timestamp in nanoseconds, used to synchronize the video with telemetry data in workbooks.

Set `auto_timestamp=False` when you are providing your own timestamps via `timestamp_ns`.
When `auto_timestamp=True` (the default), the SDK assigns wall-clock timestamps automatically.

## View the live stream

Once a stream is running, you can view it in a Nominal workbook.
See [Live video in workbooks](https://docs.nominal.io/core/documentation/platform/workbooks/live-video-streaming) to learn how to connect to and view a live stream from the workbook UI.
