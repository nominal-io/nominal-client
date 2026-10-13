"""Deprecated alias of the rust write stream, which now lives in `nominal.core`. Its parent package warns on import."""

from nominal.core._stream.rust_write_stream import RustWriteStream

__all__ = ["RustWriteStream"]
