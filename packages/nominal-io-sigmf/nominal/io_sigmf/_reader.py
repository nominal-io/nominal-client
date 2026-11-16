"""Read SigMF recordings (IQ data from software-defined radios) into long-form data."""

from __future__ import annotations

from collections.abc import Iterator
from os import PathLike
from pathlib import Path
from typing import Any

import numpy as np

from nominal.io._reader import Reader
from nominal.io.schema import Chunk, ChunkBuilder, start_ns

_SUFFIXES = {".sigmf-meta", ".sigmf-data", ".sigmf"}


def read_sigmf_chunk(  # noqa: PLR0915
    path: str | PathLike[str],
    *,
    start: Any = None,
    prefix: str | None = "sigmf",
    start_index: int = 0,
    count: int | None = None,
    skip_checksum: bool = False,
) -> Chunk:
    """Read a SigMF recording, the common format for software-defined radio captures.

    Every sample is read. Complex (IQ) recordings give two numeric channels,
    ``<prefix>.I`` and ``<prefix>.Q``; real-valued ones give ``<prefix>.value``.
    Multi-channel recordings put the channel in the name: ``<prefix>.ch0.I``. These rows
    are tagged with the ``recording`` name. Two more channels come from the metadata:

    - ``<prefix>.frequency``: each capture's centre frequency in Hz, at its first sample.
    - ``<prefix>.annotation``: each annotation's label (or comment) at its first sample,
      and ``""`` after its last, so it reads as a span. Its tags hold whichever of
      ``comment``, ``freq_lower_edge`` and ``freq_upper_edge`` the annotation has.

    Timestamps are ``start`` plus each sample's offset from the start of the
    recording, so reading part of it (``start_index``) keeps the same times. Only the
    first capture's time is used; later captures are assumed to follow on directly.

    A second of a 1 MS/s recording is two million rows, so use ``start_index`` and
    ``count`` to read part of a long one.

    Needs sigmf (``pip install nominal[sigmf]``).

    Args:
        path: The recording: its ``.sigmf-meta`` or ``.sigmf-data`` file, its name
            without an extension, or a ``.sigmf`` archive.
        start: Time of the recording's first sample. Anything `pandas.Timestamp` accepts;
            a time without a zone is taken as UTC. Defaults to the first capture's
            ``core:datetime``; if it has none, a warning is issued and the current time is used.
        prefix: Channel name prefix, or ``None`` for bare names (``I``, ``Q``).
        start_index: First sample to read.
        count: Number of samples to read; ``None`` reads to the end.
        skip_checksum: Don't check the data against the ``core:sha512`` in the metadata.
            A mismatch otherwise raises an error: the data is corrupt or the checksum stale.

    Returns:
        The recording as one `~nominal.io.schema.Chunk`; `read_sigmf` returns it as a DataFrame.
    """
    from sigmf import sigmffile

    path = Path(path)
    name = path.name.removesuffix(path.suffix) if path.suffix in _SUFFIXES else path.name
    recording = sigmffile.fromfile(str(path), skip_checksum=skip_checksum)
    captures = recording.get_captures()
    fs = float(recording.get_global_field("core:sample_rate"))
    total = recording.sample_count
    count = total - start_index if count is None else min(count, total - start_index)
    stop = start_index + count
    t0 = start_ns(start, captures[0].get("core:datetime") if captures else None, name)

    def times(samples: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
        """Return the timestamps (ns since the epoch) of recording sample numbers."""
        offsets: np.ndarray[Any, Any] = np.round(samples * (1e9 / fs)).astype("int64")
        return offsets + t0

    def channel(suffix: str) -> str:
        return f"{prefix}.{suffix}" if prefix else suffix

    samples = recording.read_samples(start_index=start_index, count=count) if count > 0 else np.zeros(0)
    samples = samples.reshape(count, -1)  # one column per recorded channel
    sample_times = times(np.arange(start_index, stop))
    parts = [("I", samples.real), ("Q", samples.imag)] if np.iscomplexobj(samples) else [("value", samples)]
    multi = samples.shape[1] > 1
    builder = ChunkBuilder()
    for c in range(samples.shape[1]):
        for part, data in parts:
            builder.add(sample_times, channel(f"ch{c}.{part}" if multi else part), data[:, c], {"recording": name})

    # Centre frequency: each capture's in the samples read, plus the one in force at start_index.
    meta_times: list[int] = []
    meta_names: list[str] = []
    meta_values: list[Any] = []
    meta_tags: list[dict[str, str]] = []
    for i, capture in enumerate(captures):
        first = capture["core:sample_start"]
        end = captures[i + 1]["core:sample_start"] if i + 1 < len(captures) else total
        if "core:frequency" in capture and first < stop and end > start_index:
            meta_times.append(max(first, start_index))
            meta_names.append(channel("frequency"))
            meta_values.append(float(capture["core:frequency"]))
            meta_tags.append({"recording": name, "units": "Hz"})

    # Annotations: the label at the first sample, "" after the last.
    for annotation in recording.get_annotations():
        first = annotation["core:sample_start"]
        after = first + annotation.get("core:sample_count", 1)
        label = annotation.get("core:label") or annotation.get("core:comment") or "annotation"
        info = {"recording": name}
        for key in ("comment", "freq_lower_edge", "freq_upper_edge"):
            if f"core:{key}" in annotation:
                info[key] = str(annotation[f"core:{key}"])
        for sample, value in ((first, label), (after, "")):
            if start_index <= sample < stop:
                meta_times.append(sample)
                meta_names.append(channel("annotation"))
                meta_values.append(value)
                meta_tags.append(info)

    builder.add_rows(times(np.asarray(meta_times, dtype="int64")), meta_names, meta_values, meta_tags)
    return builder.build()


def read_sigmf(path: str | PathLike[str], *, engine: str = "pandas", **options: Any) -> Any:
    """Read a SigMF recording as one frame (``engine`` is ``"pandas"`` or ``"polars"``).

    ``options`` are those of `read_sigmf_chunk`.
    """
    return read_sigmf_chunk(path, **options).to_frame(engine)


class SigmfReader(Reader):
    """SigMF recordings (IQ data from software-defined radios).

    See `read_sigmf_chunk` for the options, which `nominal.io.read` passes through.

    The recording is read whole, as one chunk; use the options to read a part of a long one.
    """

    def read_chunks(self, path: str | PathLike[str], **options: Any) -> Iterator[Chunk]:
        """Yield the recording as one chunk."""
        yield read_sigmf_chunk(path, **options)
