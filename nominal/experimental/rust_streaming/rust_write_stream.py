from __future__ import annotations

import warnings

warnings.warn(
    "Importing RustWriteStream from nominal.experimental.rust_streaming is deprecated. "
    "Import from nominal.core._stream.rust_write_stream instead, or use data_format='rust' "
    "when calling get_write_stream().",
    DeprecationWarning,
    stacklevel=2,
)

from nominal.core._stream.rust_write_stream import RustWriteStream

__all__ = ["RustWriteStream"]
