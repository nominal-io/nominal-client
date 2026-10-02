"""A small TDMS writer for synthesizing test fixtures. Test code only.

Writes little-endian files, one segment per ``write_segment`` call, each with
a full object list and contiguous raw data: the simplest layout the format
allows and the one LabVIEW's plain TDMS Write produces. Supported channel
types are the fixed-width numerics, booleans, complex128, strings and
timestamps; properties may be int, float, bool, str, numpy datetime64 or
``NiTimestamp``. Anything else is a mistake in the generator and raises.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from nominal.tdms import NiTimestamp

TOC_META, TOC_NEW_OBJ_LIST, TOC_RAW = 1 << 1, 1 << 2, 1 << 3
VERSION = 4713
NO_RAW = 0xFFFFFFFF
TYPE_STRING, TYPE_BOOL, TYPE_TIMESTAMP = 0x20, 0x21, 0x44
UNIX_MINUS_NI_EPOCH_S = 2_082_844_800

# numpy kind+itemsize -> tdsDataType
_NUMERIC_TYPES = {
    ("i", 1): 0x01,
    ("i", 2): 0x02,
    ("i", 4): 0x03,
    ("i", 8): 0x04,
    ("u", 1): 0x05,
    ("u", 2): 0x06,
    ("u", 4): 0x07,
    ("u", 8): 0x08,
    ("f", 4): 0x09,
    ("f", 8): 0x0A,
    ("c", 8): 0x08000C,
    ("c", 16): 0x10000D,
}


@dataclass
class Obj:
    path: str
    properties: dict = field(default_factory=dict)
    data: np.ndarray | None = None
    group_path: str | None = None  # channels only: the path of their group


def _quote(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


def root_object(properties: dict | None = None) -> Obj:
    return Obj("/", dict(properties or {}))


def group_object(name: str, properties: dict | None = None) -> Obj:
    return Obj(f"/{_quote(name)}", dict(properties or {}))


def channel_object(group_name: str, name: str, data, properties: dict | None = None) -> Obj:
    group_path = f"/{_quote(group_name)}"
    return Obj(f"{group_path}/{_quote(name)}", dict(properties or {}), np.asarray(data), group_path)


def _u32(v: int) -> bytes:
    return struct.pack("<I", v)


def _u64(v: int) -> bytes:
    return struct.pack("<Q", v)


def _string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return _u32(len(raw)) + raw


def _timestamp_bytes(value) -> bytes:
    """A NiTimestamp or datetime64 as the 16 on-disk bytes (fractions, then seconds)."""
    if isinstance(value, NiTimestamp):
        seconds, fractions = value.seconds, value.fractions
    else:
        ns = int(np.datetime64(value, "ns").astype(np.int64))
        seconds, rem = divmod(ns, 1_000_000_000)
        seconds += UNIX_MINUS_NI_EPOCH_S
        fractions = (rem << 64) // 1_000_000_000
    return struct.pack("<Qq", fractions, seconds)


def _property(name: str, value) -> bytes:
    out = _string(name)
    if isinstance(value, bool) or isinstance(value, np.bool_):
        return out + _u32(TYPE_BOOL) + bytes([1 if value else 0])
    if isinstance(value, (int, np.integer)):
        return out + _u32(0x03) + struct.pack("<i", int(value))
    if isinstance(value, (float, np.floating)):
        return out + _u32(0x0A) + struct.pack("<d", float(value))
    if isinstance(value, str):
        return out + _u32(TYPE_STRING) + _string(value)
    if isinstance(value, (NiTimestamp, np.datetime64)):
        return out + _u32(TYPE_TIMESTAMP) + _timestamp_bytes(value)
    raise TypeError(f"property {name!r}: cannot write a {type(value).__name__}")


def _channel_type(data: np.ndarray) -> int:
    if data.dtype == object:
        return TYPE_STRING
    if data.dtype.kind == "b":
        return TYPE_BOOL
    if data.dtype.kind == "M":
        return TYPE_TIMESTAMP
    try:
        return _NUMERIC_TYPES[(data.dtype.kind, data.dtype.itemsize)]
    except KeyError:
        raise TypeError(f"cannot write channel data of dtype {data.dtype}") from None


def _raw_bytes(data: np.ndarray, type_code: int) -> bytes:
    if type_code == TYPE_STRING:
        payloads = [str(s).encode("utf-8") for s in data]
        ends, total = [], 0
        for p in payloads:
            total += len(p)
            ends.append(total)
        return b"".join(_u32(e) for e in ends) + b"".join(payloads)
    if type_code == TYPE_BOOL:
        return data.astype(np.uint8).tobytes()
    if type_code == TYPE_TIMESTAMP:
        return b"".join(_timestamp_bytes(v) for v in data)
    return data.astype(data.dtype.newbyteorder("<")).tobytes()


class Writer:
    """``with Writer(path) as w: w.write_segment([objects...])``."""

    def __init__(self, path: Path | str) -> None:
        """Create or truncate the file at `path`."""
        self._fh = open(path, "wb")

    def __enter__(self) -> "Writer":
        """Support ``with Writer(path) as w``."""
        return self

    def __exit__(self, *exc) -> None:
        """Close the file."""
        self._fh.close()

    def write_segment(self, objects: list[Obj]) -> None:
        """One segment: the root, then every group in name order, then the channels as given.

        The root object is always written, with no properties when the caller
        gave none: LabVIEW's TDMS File Viewer reads the file object's
        properties and fails with error -2507 (invalid object) on a file whose
        segments never mention "/". Groups are listed even when only implied by
        a channel, so group order in the file is by name.
        """
        roots = [o for o in objects if o.path == "/"] or [Obj("/")]
        channels = [o for o in objects if o.group_path is not None]
        groups = {o.path: o for o in objects if o.path != "/" and o.group_path is None}
        for c in channels:
            groups.setdefault(c.group_path, Obj(c.group_path))
        objects = roots + [groups[k] for k in sorted(groups)] + channels
        meta = [_u32(len(objects))]
        raw = []
        for obj in objects:
            meta.append(_string(obj.path))
            if obj.data is None:
                meta.append(_u32(NO_RAW))
            else:
                type_code = _channel_type(obj.data)
                payload = _raw_bytes(obj.data, type_code)
                index = _u32(type_code) + _u32(1) + _u64(len(obj.data))
                if type_code == TYPE_STRING:
                    index += _u64(len(payload))
                meta.append(_u32(4 + len(index)) + index)
                raw.append(payload)
            meta.append(_u32(len(obj.properties)))
            meta.extend(_property(k, v) for k, v in obj.properties.items())
        meta_bytes = b"".join(meta)
        raw_bytes = b"".join(raw)
        toc = TOC_META | TOC_NEW_OBJ_LIST | (TOC_RAW if raw else 0)
        lead_in = b"TDSm" + _u32(toc) + _u32(VERSION) + _u64(len(meta_bytes) + len(raw_bytes)) + _u64(len(meta_bytes))
        self._fh.write(lead_in + meta_bytes + raw_bytes)
