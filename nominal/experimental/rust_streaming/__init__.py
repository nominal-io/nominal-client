"""Deprecated: rust streaming is no longer experimental, and is what `get_write_stream()` returns by default."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nominal.core._stream.rust_write_stream import RustWriteStream

__all__ = ["RustWriteStream"]


def __getattr__(name: str) -> object:
    # Resolved on access rather than bound at import: a warning raised while the module imports can
    # only blame the import machinery, while this one names the caller's line.
    if name != "RustWriteStream":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from nominal.core._stream.rust_write_stream import RustWriteStream

    warnings.warn(
        "nominal.experimental.rust_streaming is deprecated: get a rust stream from `get_write_stream()` on a "
        "dataset or streaming connection, where it is the default, and annotate it as `nominal.core.DataStream`.",
        UserWarning,
        stacklevel=2,
    )
    return RustWriteStream
