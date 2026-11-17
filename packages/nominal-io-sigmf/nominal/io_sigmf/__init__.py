"""SigMF reader for `nominal.io`: registers the ``sigmf`` format.

Reads ``.sigmf-meta``, ``.sigmf-data`` and ``.sigmf`` archives.
"""

from nominal.io import ReaderSpec
from nominal.io_sigmf._reader import read_sigmf, read_sigmf_chunk

SPEC = ReaderSpec(
    "sigmf",
    (
        ".sigmf-meta",
        ".sigmf-data",
        ".sigmf",
    ),
    "nominal.io_sigmf._reader:SigmfReader",
    "SigMF recordings (IQ data from software-defined radios)",
    "pip install nominal[sigmf]",
)
"""The spec registered under the ``nominal.io.readers`` entry-point group."""

__all__ = ["SPEC", "read_sigmf", "read_sigmf_chunk"]
