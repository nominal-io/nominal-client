"""Read files of many formats into one long-form DataFrame, and add formats of your own.

To get started, see :doc:`Read files into a DataFrame </guides/io/read-files>` and
:doc:`Add a file format </guides/io/write-a-reader>`.

`read` chooses a reader from the file's extension and returns the samples as a DataFrame;
`read_chunks` streams a large file in pieces. Formats are plugins: a package adds one by declaring a
`ReaderSpec` under the ``nominal.io.readers`` entry-point group, and its code is imported only when a file of that
format is read.

.. rubric:: Frame layout

A reader yields `Chunk` objects, which are columns of numpy arrays. `read` converts them to the DataFrame library
you ask for with ``engine=``, so no reader depends on pandas or polars. The pandas layout (``engine="pandas"``,
the default) has one row per sample:

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Contents
   * - ``timestamp``
     - ``datetime64[ns, UTC]``
     - Sample time.
   * - ``channel``
     - ``object`` (str)
     - Channel name, such as ``"daq.ai_0"``.
   * - ``value``
     - ``float64`` or ``object``
     - ``float64`` when every value is numeric, otherwise floats and strings mixed.
   * - ``tags``
     - ``object`` (dict)
     - Tags of the sample, ``{}`` when it has none.

Rows are sorted by timestamp, and rows with the same timestamp keep their order in the file.

The polars layout (``engine="polars"``) is experimental and may change. ``timestamp`` is ``Datetime("ns", "UTC")`` and
``channel`` is ``String``. ``value`` is ``Float64``, null for a string sample, and ``value_str`` is ``String``, null for
a numeric sample. ``tags`` is a list of ``{key, value}`` structs.

.. rubric:: How a format is chosen

`read` and `read_chunks` choose the format for a file in this order:

1. The format named by ``format=``, if given.
2. The formats that claim the longest matching extension, so ``.sigmf-meta`` beats ``.meta``.
3. If several remain, the format whose ``sniff`` method accepts the first bytes or header of the file. A format that
   can't be imported is skipped.
4. If that leaves more than one, `read` raises an error that names the candidates.
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
