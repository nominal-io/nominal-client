"""Builders for synthetic MCAPs and H.264 parameter sets.

The scanner's job is to decode bitstream syntax, so it is tested against streams built to a known
specification rather than against a recorded sample: a fixture whose dimensions are asserted to be
1920x1080 proves nothing unless something independent said they should be.
"""

from __future__ import annotations

import struct
from io import BytesIO
from typing import Iterable, Sequence

from mcap.writer import Writer

NANOS_PER_SECOND = 1_000_000_000

PROTOBUF_VIDEO_SCHEMA = "foxglove.CompressedVideo"
ROS_VIDEO_SCHEMA = "foxglove_msgs/msg/CompressedVideo"


class BitWriter:
    """Writes the fixed-width and Exp-Golomb fields H.264 parameter sets are made of."""

    def __init__(self) -> None:
        self._bits: list[int] = []

    def u(self, count: int, value: int) -> None:
        for shift in range(count - 1, -1, -1):
            self._bits.append((value >> shift) & 1)

    def ue(self, value: int) -> None:
        code = value + 1
        length = code.bit_length()
        self.u(length - 1, 0)
        self.u(length, code)

    def se(self, value: int) -> None:
        self.ue(2 * value - 1 if value > 0 else -2 * value)

    def rbsp_trailing(self) -> None:
        self._bits.append(1)
        while len(self._bits) % 8:
            self._bits.append(0)

    def bytes(self) -> bytes:
        bits = list(self._bits)
        while len(bits) % 8:
            bits.append(0)
        out = bytearray()
        for index in range(0, len(bits), 8):
            byte = 0
            for bit in bits[index : index + 8]:
                byte = (byte << 1) | bit
            out.append(byte)
        return bytes(out)


def build_sps(
    *,
    width_mbs: int = 8,
    height_map_units: int = 4,
    profile_idc: int = 77,
    level_idc: int = 40,
    constraint_flags: int = 0,
    frame_mbs_only: int = 1,
    chroma_format_idc: int = 1,
    crop: tuple[int, int, int, int] = (0, 0, 0, 0),
    scaling_matrix: bool = False,
) -> bytes:
    """Build one SPS NAL unit, including its header byte.

    Defaults describe a 128x64 4:2:0 main-profile stream, the smallest thing that exercises every
    field the parser reads.
    """
    writer = BitWriter()
    writer.ue(0)  # seq_parameter_set_id
    if profile_idc in (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135):
        writer.ue(chroma_format_idc)
        if chroma_format_idc == 3:
            writer.u(1, 0)  # separate_colour_plane_flag
        writer.ue(0)  # bit_depth_luma_minus8
        writer.ue(0)  # bit_depth_chroma_minus8
        writer.u(1, 0)  # qpprime_y_zero_transform_bypass_flag
        writer.u(1, 1 if scaling_matrix else 0)
        if scaling_matrix:
            count = 8 if chroma_format_idc != 3 else 12
            for index in range(count):
                # Only the first list is present, and it is emitted in full, so the parser has to
                # walk its coefficients rather than skip a fixed width.
                present = 1 if index == 0 else 0
                writer.u(1, present)
                if present:
                    for _ in range(16):
                        writer.se(0)
    writer.ue(0)  # log2_max_frame_num_minus4
    writer.ue(2)  # pic_order_cnt_type: 2 needs no further fields
    writer.ue(1)  # max_num_ref_frames
    writer.u(1, 0)  # gaps_in_frame_num_value_allowed_flag
    writer.ue(width_mbs - 1)  # pic_width_in_mbs_minus1
    writer.ue(height_map_units - 1)  # pic_height_in_map_units_minus1
    writer.u(1, frame_mbs_only)
    if not frame_mbs_only:
        writer.u(1, 0)  # mb_adaptive_frame_field_flag
    writer.u(1, 1)  # direct_8x8_inference_flag
    cropping = any(crop)
    writer.u(1, 1 if cropping else 0)
    if cropping:
        for value in crop:
            writer.ue(value)
    writer.u(1, 0)  # vui_parameters_present_flag
    writer.rbsp_trailing()

    payload = bytes([profile_idc, constraint_flags, level_idc]) + writer.bytes()
    return bytes([0x67]) + _add_emulation_prevention(payload)


def _add_emulation_prevention(data: bytes) -> bytes:
    """Insert the 0x03 bytes an encoder would, so the parser has to strip them back out."""
    out = bytearray()
    zeros = 0
    for byte in data:
        if zeros >= 2 and byte <= 3:
            out.append(3)
            zeros = 0
        out.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return bytes(out)


def annex_b(*nal_units: bytes, four_byte: bool = False) -> bytes:
    """Concatenate NAL units with start codes."""
    prefix = b"\x00\x00\x00\x01" if four_byte else b"\x00\x00\x01"
    return b"".join(prefix + unit for unit in nal_units)


def length_prefixed(*nal_units: bytes) -> bytes:
    return b"".join(len(unit).to_bytes(4, "big") + unit for unit in nal_units)


def protobuf_compressed_video(frame: bytes, video_format: str | None = "h264") -> bytes:
    """Encode a CompressedVideo protobuf message carrying `frame` in field 3."""
    out = bytearray()
    out.append((3 << 3) | 2)
    out.extend(_varint(len(frame)))
    out.extend(frame)
    if video_format is not None:
        encoded = video_format.encode()
        out.append((4 << 3) | 2)
        out.extend(_varint(len(encoded)))
        out.extend(encoded)
    return bytes(out)


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def cdr_compressed_video(frame: bytes, video_format: str | None = "h264") -> bytes:
    """Encode a CompressedVideo as little-endian CDR, matching the ROS 2 field order."""
    body = bytearray()

    def align(size: int) -> None:
        while len(body) % size:
            body.append(0)

    body.extend(struct.pack("<i", 0))  # timestamp.sec
    body.extend(struct.pack("<I", 0))  # timestamp.nanosec
    frame_id = b"cam\x00"
    align(4)
    body.extend(struct.pack("<I", len(frame_id)))
    body.extend(frame_id)
    align(4)
    body.extend(struct.pack("<I", len(frame)))
    body.extend(frame)
    if video_format is not None:
        encoded = video_format.encode() + b"\x00"
        align(4)
        body.extend(struct.pack("<I", len(encoded)))
        body.extend(encoded)
    return b"\x00\x01\x00\x00" + bytes(body)


def write_mcap(
    topics: Sequence[tuple[str, str, Iterable[tuple[int, bytes]]]],
    *,
    chunk_size: int = 1024,
    use_chunking: bool = True,
    schema_name: str = PROTOBUF_VIDEO_SCHEMA,
    telemetry_topics: Sequence[str] = (),
) -> bytes:
    """Write an MCAP in memory.

    Args:
        topics: (topic, message_encoding, messages) where messages are (log_time_nanos, payload).
        chunk_size: small by default so a short fixture still spans several chunks, which is what
            the per-channel timing assertions need.
        use_chunking: False produces a file with no chunk index, i.e. one that cannot be registered.
        schema_name: schema to register the video topics under, so a fixture can present itself as
            either the protobuf or the ROS spelling of CompressedVideo.
        telemetry_topics: non-video topics to include, each given one message.
    """
    buffer = BytesIO()
    writer = Writer(buffer, chunk_size=chunk_size, use_chunking=use_chunking)
    writer.start()
    video_schema = writer.register_schema(name=schema_name, encoding="protobuf", data=b"")
    for topic, encoding, messages in topics:
        channel_id = writer.register_channel(topic=topic, message_encoding=encoding, schema_id=video_schema)
        for log_time, payload in messages:
            writer.add_message(channel_id, log_time=log_time, data=payload, publish_time=log_time)
    if telemetry_topics:
        other_schema = writer.register_schema(name="std_msgs/msg/String", encoding="protobuf", data=b"")
        for topic in telemetry_topics:
            channel_id = writer.register_channel(topic=topic, message_encoding="protobuf", schema_id=other_schema)
            writer.add_message(channel_id, log_time=NANOS_PER_SECOND, data=b"\x00", publish_time=NANOS_PER_SECOND)
    writer.finish()
    return buffer.getvalue()


def video_messages(count: int, frame: bytes, *, start_ns: int, step_ns: int) -> list[tuple[int, bytes]]:
    """`count` messages carrying the same frame, evenly spaced."""
    return [(start_ns + index * step_ns, frame) for index in range(count)]
