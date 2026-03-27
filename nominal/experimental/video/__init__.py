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
        "Error loading rust bindings for live video streaming. "
        "This can be caused by missing GStreamer 1.20+, incompatible Rust bindings for your architecture, "
        "or other platform issues. See https://gstreamer.freedesktop.org/download/ for GStreamer installation."
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
