"""A reader for NI TDMS files, written from NI's published format description.

A TDMS file is a sequence of segments. Each starts with a 28-byte lead-in (tag,
table-of-contents flags, version, and the offsets of the next segment and of
this segment's raw data), optionally carries metadata (objects -- the file
root, groups and channels -- with their raw-data indices and properties), and
optionally carries raw data. Metadata is incremental: an object keeps the
index and properties it was last given, and a segment without metadata reuses
the previous segment's list of channels that have data. Raw data is laid out
as a "chunk" holding one block per listed channel, and a segment may repeat
that chunk layout many times.

``TdmsReader.open`` walks every lead-in and metadata block once, recording where
each channel's values live as a list of chunk runs, and reads no raw data.
``Channel.values`` then reads exactly the requested sample range. Every
value of a channel, whether the segment is contiguous, interleaved or DAQmx, is
at ``chunk_start + k * chunk_stride + i * value_stride``, so one strided read
serves all three layouts.

Everything malformed or unimplemented raises ``TdmsError``; nothing else should
escape. Byte order follows each segment's own flag and is normalised to native
on the way out. Timestamps keep their full 2^-64 s precision as the structured
``TIMESTAMP_DTYPE`` rather than being rounded to a datetime64.
"""

from __future__ import annotations

import bisect
import logging
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator

import numpy as np
import numpy.typing as npt

from nominal.tdms._scaling import Scaling, UnsupportedScaling, parse_scaling

logger = logging.getLogger(__name__)


class TdmsError(ValueError):
    """The input is not a TDMS file, is damaged, or uses a feature this reader lacks."""


@dataclass(frozen=True)
class NiTimestamp:
    """A TDMS timestamp: seconds since 1904-01-01 UTC and 2^-64 s fractions."""

    seconds: int
    fractions: int


# How timestamp channel data is handed out, whatever the file's byte order.
TIMESTAMP_DTYPE = np.dtype([("fractions", "<u8"), ("seconds", "<i8")])
# On disk the two halves are stored in the machine order of the writer.
_TIMESTAMP_DTYPE_BE = np.dtype([("seconds", ">i8"), ("fractions", ">u8")])

TAG = b"TDSm"
LEAD_IN_SIZE = 28
VERSIONS = (4712, 4713)

TOC_META = 1 << 1
TOC_NEW_OBJ_LIST = 1 << 2
TOC_RAW = 1 << 3
TOC_INTERLEAVED = 1 << 5
TOC_BIG_ENDIAN = 1 << 6
TOC_DAQMX = 1 << 7

# Raw-data index headers that are not an index length.
NO_RAW = 0xFFFFFFFF
SAME_AS_PREVIOUS = 0x00000000
# NI documents these as byte sequences ("69 12 00 00"); files show the first
# spelling when read as a u32 in the segment's byte order, so accept both. The
# digital-line value DAQmx actually writes is 0x126A; the spec's 0x1369 is kept.
DAQMX_FORMAT_CHANGING = frozenset({0x00001269, 0x69120000})
DAQMX_DIGITAL_LINE = frozenset({0x0000126A, 0x6A120000, 0x00001369, 0x69130000})
# Any other header is the index's length in bytes. It is not used to skip the
# index: some writers declare 20 for a 28-byte string index.

NEXT_UNKNOWN = 0xFFFFFFFFFFFFFFFF  # the writer never finished this segment

TYPE_VOID = 0x00
TYPE_STRING = 0x20
TYPE_BOOL = 0x21
TYPE_TIMESTAMP = 0x44
TYPE_DAQMX = 0xFFFFFFFF

# tdsDataType code -> numpy type code, byte order applied per segment.
_FIXED_WIDTH = {
    0x01: "i1",
    0x02: "i2",
    0x03: "i4",
    0x04: "i8",
    0x05: "u1",
    0x06: "u2",
    0x07: "u4",
    0x08: "u8",
    0x09: "f4",
    0x0A: "f8",
    0x19: "f4",
    0x1A: "f8",  # the *WithUnit pair are plain floats
    TYPE_BOOL: "u1",
    0x08000C: "c8",
    0x10000D: "c16",
}
# LabVIEW's EXT: the x87 80-bit format as 10 bytes, no padding (seen in a
# LabVIEW 2025 recording); read into float64.
TYPE_EXTENDED = frozenset({0x0B, 0x1B})
_EXTENDED_DTYPE = np.dtype("V10")
_UNIMPLEMENTED_TYPES = {
    0x4F: "fixed point",
}
# The DAQmx scaler's data type is its own enumeration, not tdsDataType.
_DAQMX_TYPES = {0: "u1", 1: "i1", 2: "u2", 3: "i2", 4: "u4", 5: "i4", 6: "u8", 7: "i8", 8: "f4", 9: "f8"}

_STRUCT_FORMATS = {
    "i1": "b",
    "i2": "h",
    "i4": "i",
    "i8": "q",
    "u1": "B",
    "u2": "H",
    "u4": "I",
    "u8": "Q",
    "f4": "f",
    "f8": "d",
    "c8": "ff",
    "c16": "dd",
}

READ_SPAN_BYTES = 64 * 1024 * 1024  # cap on one strided read across many chunks


def _type_name(code: int) -> str:
    if code in _UNIMPLEMENTED_TYPES:
        return _UNIMPLEMENTED_TYPES[code]
    return f"type {code:#x}"


def _decode_extended(raw: npt.NDArray[Any], big_endian: bool) -> npt.NDArray[np.float64]:
    """80-bit x87 extended values (10 bytes each) as float64."""
    b: npt.NDArray[np.uint8] = np.frombuffer(raw.tobytes(), dtype=np.uint8).reshape(-1, 10)
    if big_endian:
        b = b[:, ::-1]
    mantissa = np.ascontiguousarray(b[:, :8]).view("<u8").reshape(-1)
    sign_exponent = np.ascontiguousarray(b[:, 8:10]).view("<u2").reshape(-1).astype(np.int64)
    sign = np.where(sign_exponent & 0x8000, -1.0, 1.0)
    exponent = sign_exponent & 0x7FFF
    # The integer bit is explicit, so the value is mantissa * 2^(exponent - 16383 - 63);
    # a zero exponent means a denormal with the exponent of 1.
    scale = np.where(exponent == 0, 1, exponent) - 16383 - 63
    with np.errstate(over="ignore"):
        value = np.ldexp(mantissa.astype(np.float64), scale)
    special = exponent == 0x7FFF
    if special.any():
        value = np.where(special, np.where((mantissa << np.uint64(1)) == 0, np.inf, np.nan), value)
    result: npt.NDArray[np.float64] = sign * value
    return result


# --------------------------------------------------------------------------- metadata decoding


class _Cursor:
    """Bounds-checked decoder over one segment's metadata bytes."""

    def __init__(self, buf: bytes, big_endian: bool, base_offset: int) -> None:
        self._buf = buf
        self._pos = 0
        self._e = ">" if big_endian else "<"
        self._big = big_endian
        self._base = base_offset

    @property
    def pos(self) -> int:
        return self._pos

    def _take(self, n: int) -> bytes:
        end = self._pos + n
        if end > len(self._buf):
            raise TdmsError(f"metadata truncated at offset {self._base + self._pos}")
        chunk = self._buf[self._pos : end]
        self._pos = end
        return chunk

    def _unpack(self, fmt: str) -> tuple[Any, ...]:
        return struct.unpack(self._e + fmt, self._take(struct.calcsize(fmt)))

    def u8(self) -> int:
        return self._take(1)[0]

    def u32(self) -> int:
        return int(self._unpack("I")[0])

    def u64(self) -> int:
        return int(self._unpack("Q")[0])

    def string(self) -> str:
        length = self.u32()
        raw = self._take(length)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise TdmsError(f"string is not UTF-8 at offset {self._base + self._pos - length}: {e.reason}") from e

    def value(self, type_code: int) -> object:  # noqa: PLR0911 - one return per property type
        """One property value as a Python object."""
        if type_code == TYPE_STRING:
            return self.string()
        if type_code == TYPE_BOOL:
            return bool(self._take(1)[0])
        if type_code == TYPE_TIMESTAMP:
            if self._big:
                seconds, fractions = self._unpack("qQ")
            else:
                fractions, seconds = self._unpack("Qq")
            return NiTimestamp(seconds, fractions)
        if type_code == TYPE_VOID:
            return None
        if type_code in TYPE_EXTENDED:
            return float(_decode_extended(np.frombuffer(self._take(10), dtype=_EXTENDED_DTYPE), self._big)[0])
        numpy_code = _FIXED_WIDTH.get(type_code)
        if numpy_code is None:
            raise TdmsError(f"property of {_type_name(type_code)} is not supported")
        parts = self._unpack(_STRUCT_FORMATS[numpy_code])
        if numpy_code.startswith("c"):
            return complex(parts[0], parts[1])
        return parts[0]


def _split_path(path: str) -> tuple[str | None, str | None]:
    """(group, channel) from an object path: ``/``, ``/'G'`` or ``/'G'/'C'``.

    Names are single-quoted with a quote inside a name doubled.
    """
    if path == "/":
        return None, None
    parts: list[str] = []
    i, n = 0, len(path)
    while i < n:
        if path[i] != "/" or i + 1 >= n or path[i + 1] != "'":
            raise TdmsError(f"malformed object path {path!r}")
        i += 2
        name: list[str] = []
        while True:
            if i >= n:
                raise TdmsError(f"malformed object path {path!r}")
            c = path[i]
            if c == "'":
                if i + 1 < n and path[i + 1] == "'":
                    name.append("'")
                    i += 2
                    continue
                i += 1
                break
            name.append(c)
            i += 1
        parts.append("".join(name))
    if not parts:
        raise TdmsError(f"malformed object path {path!r}")
    if len(parts) > 2:
        raise TdmsError(f"object path {path!r} has more than two levels")
    return parts[0], (parts[1] if len(parts) == 2 else None)


# --------------------------------------------------------------------------- data model


@dataclass(frozen=True)
class _Scaler:
    """One raw DAQmx value a channel reads out of a raw buffer's sample."""

    scale_id: int  # what the NI_Scale graph calls this input
    buffer_index: int
    byte_offset: int  # for a digital line: bit offset into the sample
    numpy_code: str | None  # None when the DAQmx type is unknown
    digital: bool


@dataclass(frozen=True)
class _RawIndex:
    """How one channel's values are typed and counted within a chunk."""

    type_code: int
    n_values: int
    string_bytes: int = 0  # strings only: offsets table plus payload, per chunk
    scalers: tuple[_Scaler, ...] = ()  # DAQmx only
    widths: tuple[int, ...] = ()  # DAQmx only: bytes per sample of each raw buffer

    @property
    def is_daqmx(self) -> bool:
        return self.type_code == TYPE_DAQMX

    @property
    def is_string(self) -> bool:
        return self.type_code == TYPE_STRING

    @property
    def is_timestamp(self) -> bool:
        return self.type_code == TYPE_TIMESTAMP

    def disk_dtype(self, big_endian: bool) -> np.dtype[Any] | None:
        """The on-disk element type of a non-DAQmx channel; None for strings."""
        if self.is_string:
            return None
        if self.is_timestamp:
            return _TIMESTAMP_DTYPE_BE if big_endian else TIMESTAMP_DTYPE
        if self.type_code in TYPE_EXTENDED:
            return _EXTENDED_DTYPE
        return np.dtype((">" if big_endian else "<") + _FIXED_WIDTH[self.type_code])

    def native_dtype(self) -> np.dtype[Any] | None:  # noqa: PLR0911 - one return per type family
        """The element type a caller sees before scaling; None when scaling is the only way to a value."""
        if self.is_string:
            return np.dtype(object)
        if self.is_timestamp:
            return TIMESTAMP_DTYPE
        if self.type_code == TYPE_BOOL:
            return np.dtype(bool)
        if self.type_code in TYPE_EXTENDED:
            return np.dtype(np.float64)
        if self.is_daqmx:
            if len(self.scalers) != 1 or self.scalers[0].numpy_code is None:
                return None
            return np.dtype(self.scalers[0].numpy_code)
        return np.dtype(_FIXED_WIDTH[self.type_code])


@dataclass(frozen=True)
class _Part:
    """One raw value stream of a run: where it starts in chunk 0 and how it is read."""

    scale_id: int
    chunk_start: int  # file offset of the first value in chunk 0
    value_stride: int  # bytes from one value to the next
    disk_dtype: np.dtype[Any]
    bit: int  # for a digital line, which bit of the byte; -1 otherwise


@dataclass(frozen=True)
class _ChunkRun:
    """Where one channel's values live in one segment: ``n_chunks`` repeats of a chunk."""

    first_value: int  # index of this run's first value within the channel
    n_chunks: int
    values_per_chunk: int
    chunk_stride: int  # bytes from one chunk to the next (the whole chunk's size)
    parts: tuple[_Part, ...]  # empty for strings
    string_start: int  # strings: file offset of the offsets table in chunk 0
    string_bytes: int
    big_endian: bool
    data_end: int  # nothing at or beyond this offset belongs to the segment

    @property
    def n_values(self) -> int:
        return self.n_chunks * self.values_per_chunk


class _Object:
    """Everything the metadata says about one path, accumulated over segments."""

    __slots__ = ("path", "group", "name", "properties", "index", "runs", "n_values", "unsupported")

    def __init__(self, path: str, group: str | None, name: str | None) -> None:
        self.path = path
        self.group = group
        self.name = name
        self.properties: dict[str, object] = {}
        self.index: _RawIndex | None = None
        self.runs: list[_ChunkRun] = []
        self.n_values = 0
        self.unsupported: str | None = None  # why this channel's values cannot be produced


# --------------------------------------------------------------------------- parsing


def _read_standard_index(cur: _Cursor, obj: _Object) -> _RawIndex:
    type_code = cur.u32()
    dimension = cur.u32()
    n_values = cur.u64()
    if type_code in (TYPE_DAQMX, TYPE_VOID) or (
        type_code not in _FIXED_WIDTH
        and type_code not in (TYPE_STRING, TYPE_TIMESTAMP)
        and type_code not in TYPE_EXTENDED
    ):
        raise TdmsError(f"{obj.path}: channel data of {_type_name(type_code)} is not supported")
    if dimension != 1:
        raise TdmsError(f"{obj.path}: array dimension {dimension} is not supported")
    string_bytes = 0
    if type_code == TYPE_STRING:
        string_bytes = cur.u64()
        if string_bytes < 4 * n_values:
            raise TdmsError(f"{obj.path}: string block of {string_bytes} bytes cannot hold {n_values} offsets")
    if obj.index is not None and obj.index.type_code != type_code:
        raise TdmsError(f"{obj.path}: data type changed between segments")
    return _RawIndex(type_code=type_code, n_values=n_values, string_bytes=string_bytes)


def _read_daqmx_index(cur: _Cursor, obj: _Object, digital: bool) -> _RawIndex:
    """A DAQmx raw data index.

    Format-changing and digital-line indices share one layout except that the
    digital-line scaler's sample-format field is one byte, and its offset is a
    bit offset into the port's sample rather than a byte offset.
    """
    cur.u32()  # data type, always TYPE_DAQMX here
    dimension = cur.u32()
    chunk_size = cur.u64()
    n_scalers = cur.u32()
    raw_scalers = []
    for _ in range(n_scalers):
        daqmx_type, buffer_index, offset = cur.u32(), cur.u32(), cur.u32()
        cur.u8() if digital else cur.u32()  # sample format bitmap
        raw_scalers.append((daqmx_type, buffer_index, offset, cur.u32()))
    n_widths = cur.u32()
    widths = tuple(cur.u32() for _ in range(n_widths))
    if dimension != 1:
        raise TdmsError(f"{obj.path}: array dimension {dimension} is not supported")
    if obj.index is not None and not obj.index.is_daqmx:
        raise TdmsError(f"{obj.path}: data type changed between segments")
    scalers = []
    for daqmx_type, buffer_index, offset, scale_id in raw_scalers:
        if buffer_index >= len(widths):
            raise TdmsError(f"{obj.path}: DAQmx scaler refers to raw buffer {buffer_index} of {len(widths)}")
        code = "u1" if digital else _DAQMX_TYPES.get(daqmx_type)
        if code is None:
            obj.unsupported = f"DAQmx scaler data type {daqmx_type} is not supported"
        else:
            first_byte = offset // 8 if digital else offset
            if first_byte + np.dtype(code).itemsize > widths[buffer_index]:
                raise TdmsError(
                    f"{obj.path}: DAQmx scaler at byte {first_byte} overruns a {widths[buffer_index]}-byte sample"
                )
        scalers.append(_Scaler(scale_id, buffer_index, offset, code, digital))
    if len({s.scale_id for s in scalers}) != len(scalers):
        raise TdmsError(f"{obj.path}: DAQmx scalers share a scale id")
    # A channel is one dtype and one scale graph over every segment, so its
    # value streams must keep their ids, types and kinds; where they sit within
    # a sample is per segment and free to move.
    if obj.index is not None and _scaler_layout(obj.index.scalers) != _scaler_layout(tuple(scalers)):
        obj.unsupported = "DAQmx scaler layout changed between segments"
    return _RawIndex(type_code=TYPE_DAQMX, n_values=chunk_size, scalers=tuple(scalers), widths=widths)


def _scaler_layout(scalers: tuple[_Scaler, ...]) -> list[tuple[int, str, bool]]:
    return sorted((s.scale_id, s.numpy_code or "?", s.digital) for s in scalers)


# Per object: (chunk-relative offset of its string block, or parts relative to the chunk start).
_Layout = list[tuple["_Object", int, tuple[_Part, ...]]]


def _chunk_layout(raw_list: list[_Object], toc: int, big_endian: bool) -> tuple[int, _Layout]:  # noqa: PLR0912, PLR0915
    """(chunk size in bytes, where each object's values sit within a chunk).

    Three layouts (DAQmx, interleaved, contiguous) share one function so their
    differences sit side by side; that is the branch count.
    """
    e = ">" if big_endian else "<"
    entries: _Layout = []
    if toc & TOC_DAQMX:
        lead = raw_list[0].index
        assert lead is not None
        for obj in raw_list:
            idx = obj.index
            assert idx is not None
            if not idx.is_daqmx:
                raise TdmsError(f"{obj.path}: plain channel in a DAQmx segment")
            if idx.widths != lead.widths or idx.n_values != lead.n_values:
                raise TdmsError(f"{obj.path}: DAQmx channels in one segment disagree on the raw buffer layout")
        bases = []
        running = 0
        for width in lead.widths:  # raw buffers follow one another within a chunk
            bases.append(running)
            running += width * lead.n_values
        for obj in raw_list:
            idx = obj.index
            assert idx is not None
            parts = []
            for scaler in idx.scalers:
                if scaler.numpy_code is None:
                    continue
                first_byte = scaler.byte_offset // 8 if scaler.digital else scaler.byte_offset
                bit = scaler.byte_offset % 8 if scaler.digital else -1
                stride = idx.widths[scaler.buffer_index]
                parts.append(
                    _Part(
                        scaler.scale_id,
                        bases[scaler.buffer_index] + first_byte,
                        stride,
                        np.dtype(e + scaler.numpy_code),
                        bit,
                    )
                )
            entries.append((obj, 0, tuple(parts)))
        return running, entries

    if toc & TOC_INTERLEAVED:
        lead = raw_list[0].index
        assert lead is not None
        offsets = []
        running = 0
        for obj in raw_list:
            idx = obj.index
            assert idx is not None
            if idx.is_daqmx:
                raise TdmsError(f"{obj.path}: DAQmx channel in a segment without the DAQmx flag")
            if idx.is_string:
                raise TdmsError(f"{obj.path}: interleaved string data is not supported")
            if idx.n_values != lead.n_values:
                raise TdmsError(f"{obj.path}: interleaved channels with different sample counts")
            offsets.append(running)
            running += idx.disk_dtype(big_endian).itemsize  # type: ignore[union-attr]
        for obj, offset in zip(raw_list, offsets):
            entries.append((obj, 0, (_Part(0, offset, running, obj.index.disk_dtype(big_endian), -1),)))  # type: ignore[union-attr, arg-type]
        return running * lead.n_values, entries

    running = 0
    for obj in raw_list:
        idx = obj.index
        assert idx is not None
        if idx.is_daqmx:
            raise TdmsError(f"{obj.path}: DAQmx channel in a segment without the DAQmx flag")
        if idx.is_string:
            entries.append((obj, running, ()))
            running += idx.string_bytes
        else:
            dtype = idx.disk_dtype(big_endian)
            assert dtype is not None
            entries.append((obj, 0, (_Part(0, running, dtype.itemsize, dtype, -1),)))
            running += dtype.itemsize * idx.n_values
    return running, entries


def _merge_carried(
    previous: list[tuple["_Object", bool]], listed: list[tuple["_Object", bool]]
) -> list[tuple["_Object", bool]]:
    """A segment's object list merged into the carried one (no new-object-list flag).

    The carried list keeps its order; objects new to it are appended, and a
    listed object updates its own entry in place. NO_RAW means "no raw data in
    this segment", so it switches the channel's data off without giving up its
    position: a later index or same-as-previous header switches it back on in
    its original slot.
    """
    result = list(previous)
    position = {id(obj): k for k, (obj, _) in enumerate(result)}
    for obj, has_raw in listed:
        k = position.get(id(obj))
        if k is None:
            position[id(obj)] = len(result)
            result.append((obj, has_raw))
        else:
            result[k] = (obj, has_raw)
    return result


class _Parser:
    def __init__(self, fh: BinaryIO, size: int) -> None:
        self._fh = fh
        self._size = size
        self.objects: dict[str, _Object] = {}
        self.groups: dict[str, dict[str, _Object]] = {}  # group -> channel name -> object
        self.root = _Object("/", None, None)

    def _object(self, path: str) -> _Object:
        obj = self.objects.get(path)
        if obj is not None:
            return obj
        group, channel = _split_path(path)
        if group is None:
            return self.root
        channels = self.groups.setdefault(group, {})
        if channel is None:
            obj = _Object(path, group, None)
        else:
            obj = _Object(path, group, channel)
            channels[channel] = obj
        self.objects[path] = obj
        return obj

    def parse(self) -> None:
        pos = 0
        roster: list[tuple[_Object, bool]] = []  # carried object list: (object, has data this segment)
        while pos < self._size:
            if self._size - pos < LEAD_IN_SIZE:
                if pos == 0:
                    raise TdmsError("not a TDMS file: shorter than one segment lead-in")
                logger.warning("ignoring %d trailing bytes after the last segment", self._size - pos)
                break
            self._fh.seek(pos)
            lead_in = self._fh.read(LEAD_IN_SIZE)
            if lead_in[:4] != TAG:
                if pos == 0:
                    raise TdmsError("not a TDMS file: missing the TDSm lead-in tag")
                raise TdmsError(f"bad segment tag {lead_in[:4]!r} at offset {pos}")
            toc = struct.unpack("<I", lead_in[4:8])[0]
            big_endian = bool(toc & TOC_BIG_ENDIAN)
            version, next_offset, raw_offset = struct.unpack((">" if big_endian else "<") + "IQQ", lead_in[8:])
            if version not in VERSIONS:
                logger.warning("segment at offset %d declares unknown TDMS version %d", pos, version)

            meta_start = pos + LEAD_IN_SIZE
            truncated = next_offset == NEXT_UNKNOWN or meta_start + next_offset > self._size
            segment_end = self._size if truncated else meta_start + next_offset
            raw_start = meta_start + raw_offset
            if raw_start > segment_end:
                if truncated:
                    raise TdmsError(f"file ends inside the metadata of the segment at offset {pos}")
                raise TdmsError(f"segment at offset {pos} puts its raw data past its own end")

            if toc & TOC_META:
                self._fh.seek(meta_start)
                cursor = _Cursor(self._fh.read(raw_start - meta_start), big_endian, meta_start)
                roster = self._read_metadata(cursor, toc, roster)

            raw_list = [o for o, has_raw in roster if has_raw]
            if toc & TOC_RAW and raw_list:
                self._place_raw_data(raw_list, toc, big_endian, raw_start, segment_end, truncated)

            if truncated:
                break
            pos = segment_end

    def _read_metadata(
        self, cursor: _Cursor, toc: int, previous: list[tuple[_Object, bool]]
    ) -> list[tuple[_Object, bool]]:
        listed: list[tuple[_Object, bool]] = []
        seen: dict[int, bool] = {}
        for _ in range(cursor.u32()):
            obj = self._object(cursor.string())
            header = cursor.u32()
            has_raw = True
            if header == NO_RAW:
                has_raw = False
            elif header == SAME_AS_PREVIOUS:
                if obj.index is None:
                    raise TdmsError(f"{obj.path}: raw data index refers to an earlier segment that has none")
            elif header in DAQMX_FORMAT_CHANGING:
                obj.index = _read_daqmx_index(cursor, obj, digital=False)
            elif header in DAQMX_DIGITAL_LINE:
                obj.index = _read_daqmx_index(cursor, obj, digital=True)
            else:
                obj.index = _read_standard_index(cursor, obj)
            # Listed twice with raw data, the object would take two blocks of
            # every chunk and shift every later channel's values; there is no
            # right reading of that, so it is malformed rather than guessed at.
            if id(obj) in seen and (seen[id(obj)] or has_raw):
                raise TdmsError(f"{obj.path}: listed more than once in one segment")
            seen[id(obj)] = seen.get(id(obj), False) or has_raw
            for _ in range(cursor.u32()):
                name = cursor.string()
                obj.properties[name] = cursor.value(cursor.u32())
            listed.append((obj, has_raw))

        if toc & TOC_NEW_OBJ_LIST:
            return listed
        return _merge_carried(previous, listed)

    def _place_raw_data(  # noqa: PLR0917 - the segment's extents, straight from the lead-in
        self, raw_list: list[_Object], toc: int, big_endian: bool, raw_start: int, segment_end: int, truncated: bool
    ) -> None:
        chunk_stride, entries = _chunk_layout(raw_list, toc, big_endian)
        if chunk_stride == 0:
            return
        n_chunks, remainder = divmod(segment_end - raw_start, chunk_stride)
        if remainder:
            # A partial chunk still counts: its channels keep a common length and
            # values hands back what is actually there.
            n_chunks += 1
            if not truncated:
                logger.warning("segment at offset %d holds a partial chunk of %d bytes", raw_start, remainder)
        if n_chunks == 0:
            return
        for obj, string_offset, parts in entries:
            idx = obj.index
            assert idx is not None
            if idx.n_values == 0:
                continue
            obj.runs.append(
                _ChunkRun(
                    first_value=obj.n_values,
                    n_chunks=n_chunks,
                    values_per_chunk=idx.n_values,
                    chunk_stride=chunk_stride,
                    parts=tuple(
                        _Part(p.scale_id, raw_start + p.chunk_start, p.value_stride, p.disk_dtype, p.bit) for p in parts
                    ),
                    string_start=raw_start + string_offset,
                    string_bytes=idx.string_bytes,
                    big_endian=big_endian,
                    data_end=segment_end,
                )
            )
            obj.n_values += n_chunks * idx.n_values


# --------------------------------------------------------------------------- reading values


def _read_strided(  # noqa: PLR0917 - a strided view needs all of these
    fh: BinaryIO, start: int, shape: tuple[int, int], strides: tuple[int, int], dtype: np.dtype[Any], data_end: int
) -> npt.NDArray[Any]:
    """Values at ``start + r*strides[0] + c*strides[1]`` for a rows x cols grid, flattened.

    Short when the segment ends first: then only whole values that are present
    are returned, row by row.
    """
    rows, cols = shape
    itemsize = dtype.itemsize
    nbytes = (rows - 1) * strides[0] + (cols - 1) * strides[1] + itemsize
    fh.seek(start)
    buf = fh.read(max(0, min(nbytes, data_end - start)))
    if len(buf) == nbytes:
        flat: npt.NDArray[Any] = np.ndarray(shape, dtype, buffer=buf, strides=strides).reshape(-1)
        return flat
    if rows == 1:
        n = 0 if len(buf) < itemsize else (len(buf) - itemsize) // strides[1] + 1
        if n == 0:
            return np.empty(0, dtype)
        row: npt.NDArray[Any] = np.ndarray((n,), dtype, buffer=buf, strides=(strides[1],)).copy()
        return row
    parts = []
    for r in range(rows):
        part = _read_strided(fh, start + r * strides[0], (1, cols), (0, strides[1]), dtype, data_end)
        parts.append(part)
        if len(part) < cols:
            break
    return np.concatenate(parts)


def _read_part(fh: BinaryIO, run: _ChunkRun, part: _Part, a: int, b: int) -> npt.NDArray[Any]:
    """Values [a, b) of one raw stream of a run, still in on-disk dtype."""
    dtype = part.disk_dtype
    vpc = run.values_per_chunk
    k0, i0 = divmod(a, vpc)
    k1, i1 = divmod(b - 1, vpc)

    def piece(k: int, i: int, n: int) -> npt.NDArray[Any]:
        return _read_strided(
            fh,
            part.chunk_start + k * run.chunk_stride + i * part.value_stride,
            (1, n),
            (0, part.value_stride),
            dtype,
            run.data_end,
        )

    if k0 == k1:
        return piece(k0, i0, i1 + 1 - i0)
    pieces = [piece(k0, i0, vpc - i0)]
    if len(pieces[0]) < vpc - i0:
        return pieces[0]
    k = k0 + 1
    batch = max(1, READ_SPAN_BYTES // run.chunk_stride)
    while k < k1:
        m = min(batch, k1 - k)
        piece_ = _read_strided(
            fh,
            part.chunk_start + k * run.chunk_stride,
            (m, vpc),
            (run.chunk_stride, part.value_stride),
            dtype,
            run.data_end,
        )
        pieces.append(piece_)
        if len(piece_) < m * vpc:
            return np.concatenate(pieces)
        k += m
    pieces.append(piece(k1, 0, i1 + 1))
    return np.concatenate(pieces)


def _read_string_chunk(fh: BinaryIO, run: _ChunkRun, k: int, a: int, b: int) -> list[str]:
    """Strings [a, b) of chunk k."""
    vpc = run.values_per_chunk
    chunk = run.string_start + k * run.chunk_stride
    if chunk + run.string_bytes > run.data_end:
        return []  # a truncated string chunk has no trustworthy offsets
    e = ">" if run.big_endian else "<"
    first = max(a - 1, 0)
    fh.seek(chunk + 4 * first)
    ends = np.frombuffer(fh.read(4 * (b - first)), dtype=e + "u4").astype(np.int64)
    if a > 0:
        begin, ends = int(ends[0]), ends[1:]
    else:
        begin = 0
    payload_bytes = run.string_bytes - 4 * vpc
    if len(ends) != b - a or ends[-1] > payload_bytes or np.any(np.diff(ends) < 0) or (ends[0] < begin):
        raise TdmsError(f"string offsets are inconsistent at offset {chunk}")
    fh.seek(chunk + 4 * vpc + begin)
    data = fh.read(int(ends[-1]) - begin)
    out = []
    prev = begin
    try:
        for end in ends.tolist():
            out.append(data[prev - begin : end - begin].decode("utf-8"))
            prev = end
    except UnicodeDecodeError as exc:
        raise TdmsError(f"string data is not UTF-8 at offset {chunk}: {exc.reason}") from exc
    return out


def _read_string_run(fh: BinaryIO, run: _ChunkRun, a: int, b: int) -> npt.NDArray[np.object_]:
    vpc = run.values_per_chunk
    values: list[str] = []
    k0, i0 = divmod(a, vpc)
    k1, i1 = divmod(b - 1, vpc)
    for k in range(k0, k1 + 1):
        lo = i0 if k == k0 else 0
        hi = i1 + 1 if k == k1 else vpc
        got = _read_string_chunk(fh, run, k, lo, hi)
        values.extend(got)
        if len(got) < hi - lo:
            break
    return np.array(values, dtype=object)


# --------------------------------------------------------------------------- public objects


class Channel:
    """One channel: its metadata now, its values on request.

    ``dtype`` is what ``values`` returns. It is None for a channel that never
    carried data, and also when ``unsupported`` is set: then the channel's
    values cannot be produced (a scale type or raw type this reader lacks) and
    the caller should skip it rather than the file.
    """

    def __init__(self, fh: BinaryIO, obj: _Object) -> None:
        """Wrap a parsed object; decides dtype and scaling once, reads nothing."""
        self._fh = fh
        self._obj = obj
        self._starts = [run.first_value for run in obj.runs]
        self.name: str = obj.name  # type: ignore[assignment]
        self.path = obj.path
        self.properties = obj.properties
        self.unsupported: str | None = obj.unsupported
        self._scaling: Scaling | None = None
        self.dtype: np.dtype[Any] | None = None
        idx = obj.index
        if idx is None or self.unsupported is not None:
            return
        self.dtype = idx.native_dtype()
        if idx.is_string or idx.is_timestamp:
            return
        try:
            self._scaling = parse_scaling(obj.properties, obj.path)
        except UnsupportedScaling as e:
            self._fail(str(e))
            return
        raw_ids = {scaler.scale_id for scaler in idx.scalers} if idx.is_daqmx else {0}
        if self._scaling is not None:
            missing = self._scaling.raw_ids - raw_ids
            if missing:
                self._fail(f"{obj.path}: NI scaling reads raw scaler {sorted(missing)} that the channel does not have")
            elif self.dtype is None or self.dtype.kind in "iuf":
                self.dtype = np.dtype(np.float64)
        elif self.dtype is None:
            self._fail(f"{obj.path}: {len(idx.scalers)} raw scalers and no NI scaling to combine them")

    def _fail(self, reason: str) -> None:
        self.unsupported = reason
        self.dtype = None
        self._scaling = None

    def __len__(self) -> int:
        """The channel's value count over every segment, partial trailing chunk included."""
        return self._obj.n_values

    def __repr__(self) -> str:
        """Path, dtype and length, for logs and test failures."""
        return f"Channel({self.path!r}, dtype={self.dtype}, len={len(self)})"

    def values(self, offset: int, count: int) -> npt.NDArray[Any]:
        """Values [offset, offset+count), clamped to the channel; short if the file is."""
        start = max(0, offset)
        end = min(len(self), start + max(0, count))
        empty = np.empty(0, self.dtype if self.dtype is not None else object)
        if end <= start or self.dtype is None:
            return empty
        pieces = []
        i = bisect.bisect_right(self._starts, start) - 1
        while start < end and i < len(self._obj.runs):
            run = self._obj.runs[i]
            a = start - run.first_value
            b = min(end, run.first_value + run.n_values) - run.first_value
            piece = _read_string_run(self._fh, run, a, b) if not run.parts else self._read_values(run, a, b)
            pieces.append(piece)
            if len(piece) < b - a:
                break  # the file ended early; nothing after this gap is trustworthy
            start = run.first_value + b
            i += 1
        if not pieces:
            return empty
        return pieces[0] if len(pieces) == 1 else np.concatenate(pieces)

    def _read_values(self, run: _ChunkRun, a: int, b: int) -> npt.NDArray[Any]:
        """Values [a, b) of a numeric run: every raw stream read, then combined."""
        idx = self._obj.index
        assert idx is not None
        raw = {
            part.scale_id: _native(_read_part(self._fh, run, part, a, b), part, run.big_endian) for part in run.parts
        }
        n = min(len(values) for values in raw.values())
        if any(len(values) != n for values in raw.values()):
            raw = {k: v[:n] for k, v in raw.items()}
        if idx.is_timestamp or idx.type_code == TYPE_BOOL:
            values = raw[0]
            return values != 0 if idx.type_code == TYPE_BOOL else values
        if self._scaling is not None:
            return self._scaling.apply(raw)
        (values,) = raw.values()
        return values


def _native(raw: npt.NDArray[Any], part: _Part, big_endian: bool) -> npt.NDArray[Any]:
    """Native byte order, canonical timestamp layout, and the selected bit of a digital line."""
    if raw.dtype == _EXTENDED_DTYPE:
        return _decode_extended(raw, big_endian)
    if raw.dtype.names is not None:
        if not big_endian:
            return np.ascontiguousarray(raw, dtype=TIMESTAMP_DTYPE)
        out = np.empty(len(raw), TIMESTAMP_DTYPE)
        out["seconds"] = raw["seconds"]
        out["fractions"] = raw["fractions"]
        return out
    values = raw.astype(raw.dtype.newbyteorder("="), copy=False)
    if part.bit >= 0:
        return (values >> np.uint8(part.bit)) & np.uint8(1)
    return values


class Group:
    """A TDMS group: its properties and its channels in file order."""

    def __init__(self, name: str, properties: dict[str, object], channels: list[Channel]) -> None:
        """Hold the channels built for this group; nothing is read."""
        self.name = name
        self.properties = properties
        self._channels = channels
        self._by_name = {channel.name: channel for channel in channels}

    def channels(self) -> list[Channel]:
        """The channels in the order the file first mentions them."""
        return list(self._channels)

    def __getitem__(self, name: str) -> Channel:
        """The channel of that name; KeyError if the group has none."""
        return self._by_name[name]

    def __repr__(self) -> str:
        """Name and channel count, for logs and test failures."""
        return f"Group({self.name!r}, {len(self._channels)} channels)"


class TdmsReader:
    """A TDMS file with its metadata parsed and its handle open for reads."""

    def __init__(self, fh: BinaryIO, groups: list[Group], properties: dict[str, object]) -> None:
        """Own the open handle the channels read from; use ``open`` rather than this."""
        self._fh = fh
        self._groups = groups
        self._by_name = {group.name: group for group in groups}
        self.properties = properties

    @classmethod
    def open(cls, path: str | Path) -> "TdmsReader":
        fh = open(path, "rb")
        try:
            fh.seek(0, 2)
            parser = _Parser(fh, fh.tell())
            parser.parse()
            groups = []
            for group_name, channels in parser.groups.items():
                group_obj = parser.objects.get(f"/'{group_name.replace(chr(39), chr(39) * 2)}'")
                groups.append(
                    Group(
                        group_name,
                        group_obj.properties if group_obj is not None else {},
                        [Channel(fh, obj) for obj in channels.values()],
                    )
                )
            return cls(fh, groups, parser.root.properties)
        except BaseException:
            fh.close()
            raise

    def groups(self) -> list[Group]:
        """The groups in the order the file first mentions them."""
        return list(self._groups)

    def __getitem__(self, name: str) -> Group:
        """The group of that name; KeyError if the file has none."""
        return self._by_name[name]

    def close(self) -> None:
        """Release the file handle; channels cannot be read afterwards."""
        self._fh.close()

    def __enter__(self) -> "TdmsReader":
        """Support ``with TdmsReader.open(path) as f``."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the handle when the block ends."""
        self.close()

    def __iter__(self) -> Iterator[Group]:
        """Iterate over the groups in file order."""
        return iter(self._groups)
