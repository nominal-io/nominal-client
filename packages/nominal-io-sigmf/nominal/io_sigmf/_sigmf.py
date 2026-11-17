"""Just enough of the SigMF format to read a recording: JSON metadata plus a raw binary data file.

SigMF (https://sigmf.org) keeps a recording's metadata in a ``.sigmf-meta`` JSON file and its samples in a
``.sigmf-data`` file; a ``.sigmf`` archive is a tar of both. This reads them with the standard library and
numpy, so the reader needs no SigMF library.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import tarfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

import numpy as np

_DATATYPE = re.compile(r"(?P<kind>[cr])(?P<number>[fiu])(?P<bits>8|16|32|64)(?P<order>_le|_be)?")


@dataclass(frozen=True)
class Recording:
    """An open SigMF recording.

    Attributes:
        meta: The parsed ``.sigmf-meta`` JSON (``global``, ``captures`` and ``annotations``).
        sample_count: Samples per channel in the data file.
        channels: Number of interleaved channels.
        is_complex: Whether each sample is an I/Q pair.
    """

    meta: dict[str, Any]
    sample_count: int
    channels: int
    is_complex: bool
    _data: IO[bytes]
    _dtype: np.dtype[Any]

    def global_field(self, key: str, default: Any = None) -> Any:
        """Return a field of the metadata's ``global`` object."""
        return self.meta.get("global", {}).get(key, default)

    @property
    def captures(self) -> list[dict[str, Any]]:
        """The captures, in order of their first sample."""
        return sorted(self.meta.get("captures", []), key=lambda c: c.get("core:sample_start", 0))

    @property
    def annotations(self) -> list[dict[str, Any]]:
        """The annotations, as written."""
        return list(self.meta.get("annotations", []))

    def read(self, start: int, count: int) -> np.ndarray[Any, Any]:
        """Return ``count`` samples from sample ``start`` as ``float64``, shaped ``(count, channels, components)``.

        ``components`` is 2 (I and Q) for complex recordings and 1 otherwise.
        """
        components = 2 if self.is_complex else 1
        per_sample = self.channels * components
        self._data.seek(start * per_sample * self._dtype.itemsize)
        raw = np.frombuffer(self._data.read(count * per_sample * self._dtype.itemsize), dtype=self._dtype)
        return raw.reshape(-1, self.channels, components).astype(np.float64)


@contextlib.contextmanager
def open_recording(path: str | Path, *, skip_checksum: bool = False) -> Iterator[Recording]:
    """Open a recording from its ``.sigmf-meta`` or ``.sigmf-data`` file, a bare name, or a ``.sigmf`` archive.

    Raises:
        FileNotFoundError: If the metadata or data file is missing.
        ValueError: If the metadata is missing a required field, the datatype is unsupported, a capture has
            header bytes, or the data doesn't match its ``core:sha512`` (unless ``skip_checksum``).
    """
    path = Path(path)
    with contextlib.ExitStack() as stack:
        if path.suffix == ".sigmf":
            archive = stack.enter_context(tarfile.open(path))
            members = {Path(m.name).suffix: m for m in archive.getmembers() if m.isfile()}
            if ".sigmf-meta" not in members or ".sigmf-data" not in members:
                raise FileNotFoundError(f"{path} is missing its .sigmf-meta or .sigmf-data member")
            meta_file = archive.extractfile(members[".sigmf-meta"])
            data = archive.extractfile(members[".sigmf-data"])
            size = members[".sigmf-data"].size
            assert meta_file is not None and data is not None
            meta = json.load(meta_file)
        else:
            base = path.with_suffix("") if path.suffix in {".sigmf-meta", ".sigmf-data"} else path
            meta = json.loads(Path(f"{base}.sigmf-meta").read_text())
            data = stack.enter_context(open(f"{base}.sigmf-data", "rb"))
            size = Path(f"{base}.sigmf-data").stat().st_size
        yield _recording(meta, data, size, skip_checksum)


def _recording(meta: dict[str, Any], data: IO[bytes], size: int, skip_checksum: bool) -> Recording:
    global_ = meta.get("global", {})
    for required in ("core:datatype", "core:sample_rate"):
        if required not in global_:
            raise ValueError(f"the SigMF metadata has no {required!r}")
    match = _DATATYPE.fullmatch(str(global_["core:datatype"]))
    if match is None:
        raise ValueError(f"unsupported SigMF datatype {global_['core:datatype']!r}")
    order = ">" if match["order"] == "_be" else "<"
    dtype = np.dtype(f"{order}{match['number']}{int(match['bits']) // 8}")
    is_complex = match["kind"] == "c"
    channels = int(global_.get("core:num_channels", 1))
    if any(c.get("core:header_bytes", 0) for c in meta.get("captures", [])):
        raise ValueError("captures with core:header_bytes aren't supported")
    if not skip_checksum and "core:sha512" in global_:
        data.seek(0)
        digest = hashlib.sha512()
        while block := data.read(1 << 20):
            digest.update(block)
        if digest.hexdigest() != str(global_["core:sha512"]).lower():
            raise ValueError("the data doesn't match its core:sha512; it is corrupt or the checksum is stale")
    per_sample = channels * (2 if is_complex else 1) * dtype.itemsize
    return Recording(meta, size // per_sample, channels, is_complex, data, dtype)
