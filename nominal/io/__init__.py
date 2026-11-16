"""Read files of many formats into one long-form frame, and add formats of your own.

``nominal.io.read(path)`` picks a reader from the file's extension (and a quick look inside it
when several formats share one); ``format="name"`` always overrides. ``read_chunks`` streams
large files. `list_formats` shows what's installed.

Readers produce `Chunk` objects (numpy arrays), not DataFrames; ``read`` converts to the frame library
you ask for with ``engine="pandas"`` (the default) or ``engine="polars"``.

Formats are plugins. A package adds one by declaring a `ReaderSpec` under the
``nominal.io.readers`` entry-point group, and its code is imported only when a file of that
format is read. See `Reader`, `register_reader` and `nominal.io.testing.check_reader`.
"""

from nominal.io._reader import MissingDependencyError, Reader, ReaderSpec
from nominal.io._registry import (
    ENTRY_POINT_GROUP,
    get_reader,
    list_formats,
    read,
    read_chunks,
    register,
    register_reader,
    resolve_format,
    unregister,
)
from nominal.io.schema import COLUMNS, ENGINES, Chunk, ChunkBuilder

__all__ = [
    "COLUMNS",
    "ENGINES",
    "Chunk",
    "ChunkBuilder",
    "ENTRY_POINT_GROUP",
    "MissingDependencyError",
    "Reader",
    "ReaderSpec",
    "get_reader",
    "list_formats",
    "read",
    "read_chunks",
    "register",
    "register_reader",
    "resolve_format",
    "unregister",
]
