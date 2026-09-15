from __future__ import annotations

try:
    from nominal.experimental.rust_streaming.rust_write_stream import RustWriteStream  # noqa: F401
except ImportError as e:
    raise ImportError(
        "nominal-streaming is required for rust streaming. It ships pre-compiled binaries for a "
        "subset of platforms and interpreters; install it with: pip install nominal-streaming"
    ) from e

__all__ = ["RustWriteStream"]
