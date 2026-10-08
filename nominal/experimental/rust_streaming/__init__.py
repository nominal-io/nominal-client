"""Deprecated alias of the rust write stream, which now lives in `nominal.core`. Will be removed in a future release."""

from __future__ import annotations

import warnings

from nominal.core._stream.rust_write_stream import RustWriteStream

# `warnings` skips the import machinery's frames, so stacklevel=2 names the line importing this module.
warnings.warn(
    "nominal.experimental.rust_streaming is deprecated and will be removed in a future release. Get a rust "
    "stream from `get_write_stream()` on a dataset or streaming connection, where it is the default, and "
    "annotate it as `nominal.core.DataStream`.",
    UserWarning,
    stacklevel=2,
)

__all__ = ["RustWriteStream"]
