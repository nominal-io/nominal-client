"""H.264 SPS parsing, for the two fields an MCAP summary cannot supply.

`codec_string`, `width` and `height` are not recorded anywhere in an MCAP. They are properties of
the encoded stream, carried in the sequence parameter set, so the only way to learn them is to read
one and decode its fields. Everything else a registration needs comes from the summary section.

This is deliberately a parser rather than a lookup: `avc1.PPCCLL` is three bytes of the SPS in hex,
but the dimensions are Exp-Golomb coded behind a variable-length prefix, so they cannot be read at a
fixed offset.
"""

from __future__ import annotations

from dataclasses import dataclass

# nal_unit_type values from ITU-T H.264 Table 7-1.
_NAL_TYPE_SPS = 7

# Profiles that carry chroma_format_idc and the scaling matrices in the SPS (H.264 7.3.2.1.1).
_PROFILES_WITH_CHROMA_INFO = frozenset({100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135})


class SpsParseError(ValueError):
    """Raised when a buffer that should hold an SPS cannot be decoded as one."""


@dataclass(frozen=True)
class SpsInfo:
    """What an SPS tells us that the MCAP summary does not."""

    codec_string: str
    width: int
    height: int


class _BitReader:
    """Bit reader over an RBSP, with the Exp-Golomb codes the SPS syntax uses.

    Every read is bounds-checked. A truncated or misidentified SPS is an ordinary thing to encounter
    -- it is why we search several frames rather than trusting the first -- so running off the end
    raises `SpsParseError` and lets the caller move on, rather than raising `IndexError` from some
    depth that says nothing about the cause.
    """

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = _strip_emulation_prevention(data)
        self._pos = 0

    def bit(self) -> int:
        index = self._pos >> 3
        if index >= len(self._data):
            raise SpsParseError("ran past the end of the SPS")
        value = (self._data[index] >> (7 - (self._pos & 7))) & 1
        self._pos += 1
        return value

    def bits(self, count: int) -> int:
        value = 0
        for _ in range(count):
            value = (value << 1) | self.bit()
        return value

    def ue(self) -> int:
        """Unsigned Exp-Golomb (H.264 9.1)."""
        leading = 0
        while self.bit() == 0:
            leading += 1
            if leading > 32:
                raise SpsParseError("malformed Exp-Golomb code")
        return (1 << leading) - 1 + (self.bits(leading) if leading else 0)

    def se(self) -> int:
        """Signed Exp-Golomb (H.264 9.1.1)."""
        k = self.ue()
        return (k + 1) // 2 if k % 2 else -(k // 2)


def _strip_emulation_prevention(data: bytes) -> bytes:
    """Remove emulation prevention bytes, turning an EBSP back into an RBSP.

    An encoder inserts 0x03 after any 0x00 0x00 that would otherwise look like a start code. Leaving
    them in shifts every subsequent bit and silently yields wrong dimensions.
    """
    out = bytearray()
    zeros = 0
    for byte in data:
        if zeros == 2 and byte == 3:
            zeros = 0
            continue
        out.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return bytes(out)


def _skip_scaling_list(reader: _BitReader, size: int) -> None:
    """Consume one scaling list (H.264 7.3.2.1.1.1).

    Nothing downstream needs the coefficients, but they sit between the profile fields and the
    dimensions, so they have to be walked rather than skipped by a fixed offset.
    """
    last_scale = 8
    next_scale = 8
    for _ in range(size):
        if next_scale != 0:
            delta_scale = reader.se()
            next_scale = (last_scale + delta_scale + 256) % 256
        last_scale = next_scale if next_scale != 0 else last_scale


def parse_sps(nal_units: list[bytes]) -> SpsInfo | None:
    """Decode the first SPS among `nal_units`, or None if there is none.

    Returns None when no NAL unit is an SPS -- an ordinary outcome when scanning frames. Raises
    `SpsParseError` when a unit claims to be an SPS but does not decode as one, because that is a
    corrupt stream rather than a search that has not finished.
    """
    for nal_unit in nal_units:
        if len(nal_unit) < 4 or (nal_unit[0] & 0x1F) != _NAL_TYPE_SPS:
            continue

        # avc1.PPCCLL is profile_idc, the constraint flags byte and level_idc as hex -- literally the
        # three bytes after the NAL header, which is why the codec string needs no bit parsing.
        profile_idc, constraint_flags, level_idc = nal_unit[1], nal_unit[2], nal_unit[3]
        codec_string = f"avc1.{profile_idc:02x}{constraint_flags:02x}{level_idc:02x}"
        width, height = _decode_dimensions(nal_unit[4:], profile_idc)
        return SpsInfo(codec_string=codec_string, width=width, height=height)
    return None


def _read_chroma_info(reader: _BitReader, profile_idc: int) -> tuple[int, int]:
    """Consume the chroma and scaling-matrix fields the high profiles carry (H.264 7.3.2.1.1).

    Returns `(chroma_format_idc, separate_colour_plane_flag)`, defaulting to 4:2:0 for the profiles
    that do not signal them. The scaling lists are walked rather than skipped because they are
    variable length and sit between here and the picture dimensions.
    """
    if profile_idc not in _PROFILES_WITH_CHROMA_INFO:
        return 1, 0

    chroma_format_idc = reader.ue()
    separate_colour_plane_flag = reader.bit() if chroma_format_idc == 3 else 0
    reader.ue()  # bit_depth_luma_minus8
    reader.ue()  # bit_depth_chroma_minus8
    reader.bit()  # qpprime_y_zero_transform_bypass_flag
    if reader.bit():  # seq_scaling_matrix_present_flag
        for index in range(8 if chroma_format_idc != 3 else 12):
            if reader.bit():  # seq_scaling_list_present_flag[index]
                _skip_scaling_list(reader, 16 if index < 6 else 64)
    return chroma_format_idc, separate_colour_plane_flag


def _decode_dimensions(rbsp: bytes, profile_idc: int) -> tuple[int, int]:
    """Decode width and height from the SPS payload following profile/level (H.264 7.3.2.1.1)."""
    reader = _BitReader(rbsp)
    reader.ue()  # seq_parameter_set_id

    chroma_format_idc, separate_colour_plane_flag = _read_chroma_info(reader, profile_idc)

    reader.ue()  # log2_max_frame_num_minus4
    pic_order_cnt_type = reader.ue()
    if pic_order_cnt_type == 0:
        reader.ue()  # log2_max_pic_order_cnt_lsb_minus4
    elif pic_order_cnt_type == 1:
        reader.bit()  # delta_pic_order_always_zero_flag
        reader.se()  # offset_for_non_ref_pic
        reader.se()  # offset_for_top_to_bottom_field
        for _ in range(reader.ue()):  # num_ref_frames_in_pic_order_cnt_cycle
            reader.se()

    reader.ue()  # max_num_ref_frames
    reader.bit()  # gaps_in_frame_num_value_allowed_flag
    width_in_mbs = reader.ue() + 1
    height_in_map_units = reader.ue() + 1
    frame_mbs_only_flag = reader.bit()
    if not frame_mbs_only_flag:
        reader.bit()  # mb_adaptive_frame_field_flag
    reader.bit()  # direct_8x8_inference_flag

    crop_left = crop_right = crop_top = crop_bottom = 0
    if reader.bit():  # frame_cropping_flag
        crop_left, crop_right, crop_top, crop_bottom = reader.ue(), reader.ue(), reader.ue(), reader.ue()

    # Crop offsets are expressed in chroma samples, so the units depend on the subsampling
    # (H.264 7.4.2.1.1). Monochrome is its own case and not a 4:2:0 lookalike -- IR cameras record
    # it, and treating it as 4:2:0 would halve the reported crop and give the wrong picture size.
    chroma_array_type = 0 if separate_colour_plane_flag else chroma_format_idc
    if chroma_array_type == 0:
        crop_unit_x, crop_unit_y = 1, 2 - frame_mbs_only_flag
    else:
        sub_width_c = 2 if chroma_array_type in (1, 2) else 1
        sub_height_c = 2 if chroma_array_type == 1 else 1
        crop_unit_x, crop_unit_y = sub_width_c, sub_height_c * (2 - frame_mbs_only_flag)

    height_in_mbs = (2 - frame_mbs_only_flag) * height_in_map_units
    width = width_in_mbs * 16 - crop_unit_x * (crop_left + crop_right)
    height = height_in_mbs * 16 - crop_unit_y * (crop_top + crop_bottom)
    if width <= 0 or height <= 0:
        raise SpsParseError(f"decoded a nonsensical picture size {width}x{height}")
    return width, height
