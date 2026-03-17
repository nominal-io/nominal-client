from __future__ import annotations

import sys

try:
    from nominal_video import (  # noqa: F401
        Codec,
        Crop,
        FlipMode,
        Frame,
        ImageFormat,
        ReconnectOptions,
        Src,
        Stream,
        StreamOptions,
    )

    from nominal.experimental.video._video_stream import VideoStream  # noqa: F401
except ImportError as e:
    if "nominal_video" in str(e):
        raise ImportError(
            "nominal[video] is required for live video streaming. "
            "Install it with: pip install 'nominal[video]'"
        ) from e
    if sys.platform == "darwin":
        _gst_instructions = (
            "  brew install gstreamer gst-plugins-base gst-plugins-good "
            "gst-plugins-bad gst-plugins-ugly libnice-gstreamer"
        )
    elif sys.platform == "win32":
        _gst_instructions = "  Download and install GStreamer 1.20+ from https://gstreamer.freedesktop.org/download/"
    else:
        _gst_instructions = (
            "  sudo apt install \\\n"
            "      gstreamer1.0-plugins-base gstreamer1.0-plugins-good \\\n"
            "      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly \\\n"
            "      gstreamer1.0-libav gstreamer1.0-nice libssl3"
        )
    raise ImportError(
        f"GStreamer 1.20+ is required for live video streaming.\n{_gst_instructions}"
    ) from e

__all__ = [
    "Codec",
    "Crop",
    "FlipMode",
    "Frame",
    "ImageFormat",
    "ReconnectOptions",
    "Src",
    "Stream",
    "StreamOptions",
    "VideoStream",
]
