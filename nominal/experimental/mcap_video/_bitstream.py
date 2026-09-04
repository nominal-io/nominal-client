"""Getting the encoded frame out of one MCAP message, and its NAL units out of the frame.

An MCAP records a video topic as a stream of `CompressedVideo` messages in one of two wire
envelopes: protobuf, which the Foxglove SDK and most custom recorders write, or CDR, which ROS 2's
rosbag2 always writes. Both carry the same H.264 bytes; only the wrapping differs.

Both envelopes are parsed properly here rather than guessed at. Guessing works until it doesn't: a
"take the longest length-delimited field" heuristic reads a protobuf message correctly and reads a
CDR one as noise, and the failure surfaces as a wrong codec string rather than as an error.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

# Field numbers from the Foxglove CompressedVideo schema, shared by its protobuf and ROS definitions.
_PROTOBUF_FIELD_DATA = 3
_PROTOBUF_FIELD_FORMAT = 4

_WIRE_TYPE_VARINT = 0
_WIRE_TYPE_64BIT = 1
_WIRE_TYPE_LENGTH_DELIMITED = 2
_WIRE_TYPE_32BIT = 5


class FrameDecodeError(ValueError):
    """Raised when a message cannot be read as a CompressedVideo of the stated encoding."""


@dataclass(frozen=True)
class VideoFrame:
    """One encoded frame, and the codec the recorder said it is."""

    data: bytes
    # The schema's `format` field, e.g. "h264". Absent when the recorder left it unset, which is
    # legal and common enough that it cannot be treated as an error.
    format: str | None


def _read_varint(buffer: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(buffer):
            raise FrameDecodeError("truncated varint")
        byte = buffer[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift > 63:
            raise FrameDecodeError("varint too long")


def extract_protobuf_frame(message: bytes) -> VideoFrame:
    """Read `data` and `format` out of a protobuf-encoded CompressedVideo.

    Walks the fields by tag rather than depending on generated bindings, which would drag a protobuf
    schema dependency into a reader that needs exactly two fields.
    """
    data: bytes | None = None
    video_format: str | None = None
    offset = 0
    while offset < len(message):
        key, offset = _read_varint(message, offset)
        field_number, wire_type = key >> 3, key & 7
        if wire_type == _WIRE_TYPE_LENGTH_DELIMITED:
            length, offset = _read_varint(message, offset)
            end = offset + length
            if end > len(message):
                raise FrameDecodeError("length-delimited field runs past the end of the message")
            if field_number == _PROTOBUF_FIELD_DATA:
                data = message[offset:end]
            elif field_number == _PROTOBUF_FIELD_FORMAT:
                video_format = message[offset:end].decode("utf-8", errors="replace")
            offset = end
        elif wire_type == _WIRE_TYPE_VARINT:
            _, offset = _read_varint(message, offset)
        elif wire_type == _WIRE_TYPE_32BIT:
            offset += 4
        elif wire_type == _WIRE_TYPE_64BIT:
            offset += 8
        else:
            raise FrameDecodeError(f"unsupported protobuf wire type {wire_type}")

    if data is None:
        raise FrameDecodeError("no data field in the CompressedVideo message")
    return VideoFrame(data=data, format=video_format)


class _CdrReader:
    """Sequential reader for a CDR-encoded message body, honouring CDR's alignment rules.

    Every primitive in CDR is aligned to its own width, measured from the start of the body rather
    than the start of the buffer -- which is why the 4-byte encapsulation header is excluded from the
    offset used for padding.
    """

    __slots__ = ("_data", "_pos", "_prefix")

    def __init__(self, data: bytes, body_start: int, little_endian: bool) -> None:
        self._data = data
        self._pos = body_start
        self._prefix = "<" if little_endian else ">"

    def _align(self, size: int) -> None:
        # Alignment is relative to the body, which starts after the 4-byte encapsulation header.
        padding = (size - ((self._pos - 4) % size)) % size
        self._pos += padding

    def uint32(self) -> int:
        self._align(4)
        if self._pos + 4 > len(self._data):
            raise FrameDecodeError("truncated CDR uint32")
        (value,) = struct.unpack_from(f"{self._prefix}I", self._data, self._pos)
        self._pos += 4
        return int(value)

    def skip(self, count: int) -> None:
        self._pos += count

    def byte_sequence(self) -> bytes:
        length = self.uint32()
        if self._pos + length > len(self._data):
            raise FrameDecodeError("truncated CDR sequence")
        value = self._data[self._pos : self._pos + length]
        self._pos += length
        return value

    def string(self) -> str:
        # A CDR string's length includes its NUL terminator.
        raw = self.byte_sequence()
        return raw.rstrip(b"\x00").decode("utf-8", errors="replace")


def extract_cdr_frame(message: bytes) -> VideoFrame:
    """Read `data` and `format` out of a CDR-encoded CompressedVideo.

    Field order is fixed by the ROS message definition -- timestamp, frame_id, data, format -- so the
    body is walked in that order rather than searched.
    """
    if len(message) < 4:
        raise FrameDecodeError("message is too short to carry a CDR encapsulation header")
    # Byte 1 of the encapsulation header selects endianness; 0 is big-endian, 1 little-endian.
    little_endian = bool(message[1] & 1)
    reader = _CdrReader(message, body_start=4, little_endian=little_endian)

    reader.uint32()  # timestamp.sec  (int32, same width)
    reader.uint32()  # timestamp.nanosec
    reader.string()  # frame_id
    data = reader.byte_sequence()
    try:
        video_format: str | None = reader.string()
    except FrameDecodeError:
        # `format` is the last field; a recorder that truncated it still gives us usable frame bytes.
        video_format = None
    return VideoFrame(data=data, format=video_format)


def split_annex_b(payload: bytes) -> list[bytes]:
    """Split an Annex B stream into NAL units.

    A unit runs from just after its own start code to just before the next one begins, which is the
    detail worth being careful about: start codes are three or four bytes, so ending a unit a fixed
    distance before the next start leaves a stray zero on every unit that happens to be followed by
    the four-byte form. Trailing zeros are stripped for the same reason.
    """
    starts: list[tuple[int, int]] = []  # (payload begins, start code begins)
    index = 0
    length = len(payload)
    while index + 2 < length:
        if payload[index] == 0 and payload[index + 1] == 0:
            if payload[index + 2] == 1:
                starts.append((index + 3, index))
                index += 3
                continue
            if index + 3 < length and payload[index + 2] == 0 and payload[index + 3] == 1:
                starts.append((index + 4, index))
                index += 4
                continue
        index += 1

    units = []
    for position, (begin, _) in enumerate(starts):
        end = starts[position + 1][1] if position + 1 < len(starts) else length
        unit = payload[begin:end].rstrip(b"\x00")
        if unit:
            units.append(unit)
    return units


def split_length_prefixed(payload: bytes) -> list[bytes]:
    """Split a length-prefixed (AVCC-style) stream into NAL units.

    Assumes the four-byte length field that every recorder we have seen writes. A prefix that does
    not describe the buffer stops the walk rather than raising, so a misidentified stream simply
    yields nothing and the caller falls back.
    """
    units = []
    index = 0
    while index + 4 <= len(payload):
        size = int.from_bytes(payload[index : index + 4], "big")
        if size == 0 or index + 4 + size > len(payload):
            break
        units.append(payload[index + 4 : index + 4 + size])
        index += 4 + size
    return units
