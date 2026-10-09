"""Deprecated alias of the rust write stream, which now lives in `nominal.core`. Will be removed in a future release."""

from __future__ import annotations

import warnings

from nominal.core._stream.rust_write_stream import RustWriteStream

# `warnings` skips the import machinery's frames, so stacklevel=2 names the line importing this module.
warnings.warn(
    "nominal.experimental.rust_streaming is deprecated and will be removed in a future release. Use "
    "`get_write_stream()` on a dataset or streaming connection. Rust is the default backend when available. "
    "Use `nominal.core.DataStream` for type annotations.",
    UserWarning,
    stacklevel=2,
)

__all__ = ["RustWriteStream"]
