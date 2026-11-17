"""Read PhysioNet WFDB records (ECGs and other physiological signals) into long-form data."""

from __future__ import annotations

from collections.abc import Iterator
from os import PathLike
from pathlib import Path
from typing import Any

import numpy as np

from nominal.io._reader import Reader
from nominal.io.schema import Chunk, ChunkBuilder, start_ns

BEAT_SYMBOLS = frozenset("NLRBAaJSVrFejnE/fQ?")
"""Annotation symbols that mark a beat, from PhysioNet's annotation table.

``N`` normal, ``V`` premature ventricular contraction, ``A`` atrial premature beat,
``L``/``R`` bundle branch block, ``/`` paced, ``F`` fusion, ``Q`` unclassifiable, and so on.
"""

_RECORD_SUFFIXES = {".hea", ".dat", ".atr"}


def read_wfdb_chunk(
    record: str | PathLike[str],
    *,
    start: Any = None,
    prefix: str | None = "wfdb",
    annotations: str | None = "atr",
    pn_dir: str | None = None,
    sampfrom: int = 0,
    sampto: int | None = None,
    channels: list[int] | None = None,
) -> Chunk:
    """Read a WFDB record, the format of PhysioNet's ECG and physiological databases.

    Each signal becomes a numeric channel, e.g. ``wfdb.MLII``, tagged with its
    ``units`` and the ``record`` name. Annotations, when the record has them, become
    string channels:

    - ``<prefix>.beat``: beat labels, such as ``"N"`` (normal) or ``"V"`` (premature
      ventricular); see `BEAT_SYMBOLS`.
    - ``<prefix>.rhythm``: rhythm changes, such as ``"N"`` (normal) or ``"B"``
      (ventricular bigeminy).
    - ``<prefix>.event``: any other annotation symbol, e.g. ``"~"`` for signal quality.

    Timestamps are ``start`` plus each sample's offset from the start of the record,
    so reading part of a record (``sampfrom``) keeps the same times.

    Needs wfdb (``pip install nominal[wfdb]``).

    Args:
        record: Path to a local record, with or without its ``.hea`` extension
            (``"data/wfdb/119"``), or a record name when ``pn_dir`` is given (``"119"``).
        start: Time of the record's first sample. Anything `pandas.Timestamp` accepts;
            a time without a zone is taken as UTC. Defaults to the time in the record's
            header; if the header has none, a warning is issued and the current time is used.
        prefix: Channel name prefix, or ``None`` for bare signal names.
        annotations: Extension of the annotation file to read (``"atr"`` for reference
            beat labels), or ``None`` to skip annotations. A missing file is skipped.
        pn_dir: PhysioNet database to download from, e.g. ``"mitdb"``; ``None`` reads locally.
        sampfrom: First sample to read.
        sampto: Sample to stop before; ``None`` reads to the end.
        channels: Indices of the signals to read; ``None`` reads all of them.

    Returns:
        The record as one `~nominal.io.schema.Chunk`; `read_wfdb` returns it as a DataFrame.
    """
    import wfdb

    path = Path(record)
    if path.suffix in _RECORD_SUFFIXES:
        path = path.with_suffix("")
    name = path.name
    rec = wfdb.rdrecord(str(path), pn_dir=pn_dir, sampfrom=sampfrom, sampto=sampto, channels=channels)
    t0 = start_ns(start, rec.base_datetime, name)
    ns_per_sample = 1e9 / float(rec.fs)

    def times(samples: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
        """Return the timestamps (ns since the epoch) of record sample numbers."""
        offsets: np.ndarray[Any, Any] = np.round(samples * ns_per_sample).astype("int64")
        return offsets + t0

    def channel(signal: str) -> str:
        return f"{prefix}.{signal}" if prefix else signal

    builder = ChunkBuilder()
    signal = rec.p_signal
    n = signal.shape[0]
    sample_times = times(np.arange(sampfrom, sampfrom + n))
    for j, (sig_name, units) in enumerate(zip(rec.sig_name, rec.units)):
        builder.add(sample_times, channel(sig_name), signal[:, j], {"record": name, "units": units})

    if annotations:
        try:
            ann = wfdb.rdann(str(path), annotations, pn_dir=pn_dir, sampfrom=sampfrom, sampto=sampto)
        except FileNotFoundError:
            ann = None
        if ann is not None:
            # rdann includes an annotation at sampto itself; keep those within the samples read
            keep = [i for i, s in enumerate(ann.sample) if sampfrom <= s < sampfrom + n]
            labels = [_annotation(ann.symbol[i], ann.aux_note[i]) for i in keep]
            builder.add_rows(
                times(np.asarray(ann.sample)[keep]),
                [channel(kind) for kind, _ in labels],
                [label for _, label in labels],
                {"record": name},
            )
    return builder.build()


def read_wfdb(record: str | PathLike[str], *, engine: str = "pandas", **options: Any) -> Any:
    """Read a WFDB record as one frame (``engine`` is ``"pandas"`` or ``"polars"``).

    ``options`` are those of `read_wfdb_chunk`.
    """
    return read_wfdb_chunk(record, **options).to_frame(engine)


def _annotation(symbol: str, note: str) -> tuple[str, str]:
    """Return the channel kind and value of one annotation."""
    if symbol in BEAT_SYMBOLS:
        return "beat", symbol
    if symbol == "+":  # rhythm change; the rhythm is in the note, e.g. "(N"
        return "rhythm", note.strip("\x00").lstrip("(")
    return "event", symbol


class WfdbReader(Reader):
    """PhysioNet WFDB records (ECGs and other physiological signals).

    See `read_wfdb_chunk` for the options, which `nominal.io.read` passes through.

    The recording is read whole, as one chunk; use the options to read a part of a long one.
    """

    def read_chunks(self, path: str | PathLike[str], **options: Any) -> Iterator[Chunk]:
        """Yield the recording as one chunk."""
        yield read_wfdb_chunk(path, **options)
