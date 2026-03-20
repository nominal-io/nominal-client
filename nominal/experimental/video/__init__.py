from __future__ import annotations

try:
    from nominal_video import (  # noqa: F401
        Codec,
        Crop,
        FlipMode,
        ImageFormat,
        ReconnectOptions,
        Src,
        Stream,
        StreamOptions,
    )

    from nominal.experimental.video._video_stream import VideoStream  # noqa: F401
except ImportError as e:
    # If the error mentions nominal_video itself, the package is not installed.
    # Otherwise, nominal_video is installed but failed to load (e.g. GStreamer is missing).
    if "nominal_video" in str(e):
        raise ImportError(
            "nominal[video] is required for live video streaming. Install it with: pip install 'nominal[video]'"
        ) from e
    raise ImportError(
        "GStreamer 1.20+ is required for live video streaming. "
        "See https://gstreamer.freedesktop.org/download/ for installation instructions."
    ) from e

__all__ = [
    "Codec",
    "Crop",
    "FlipMode",
    "ImageFormat",
    "ReconnectOptions",
    "Src",
    "Stream",
    "StreamOptions",
    "VideoStream",
]
