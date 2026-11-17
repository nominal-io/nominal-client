"""WFDB reader for `nominal.io`: registers the ``wfdb`` format (``.hea`` record headers)."""

from nominal.io import ReaderSpec
from nominal.io_wfdb._reader import BEAT_SYMBOLS, read_wfdb, read_wfdb_chunk

SPEC = ReaderSpec(
    "wfdb",
    (".hea",),
    "nominal.io_wfdb._reader:WfdbReader",
    "PhysioNet WFDB records (ECGs and other physiological signals)",
    "pip install nominal[wfdb]",
)
"""The spec registered under the ``nominal.io.readers`` entry-point group."""

__all__ = ["BEAT_SYMBOLS", "SPEC", "read_wfdb", "read_wfdb_chunk"]
