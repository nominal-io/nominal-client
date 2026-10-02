"""The TDMS reader.

Three kinds of test. The spec-byte tests build segments by hand from NI's format
description to cover what no writer produces on request: damaged files,
incremental metadata, partial chunks, both byte orders. The DAQmx tests require
the values DAQmx itself returned while recording each file under fixtures/ni to
come back out of the file. The frozen-recording tests hold every NI recording to
the channel contents captured when those checks were first passed, against
NI's own software.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import numpy as np
import pytest

from nominal.tdms import _reader as reader
from nominal.tdms._reader import (
    NEXT_UNKNOWN,
    NO_RAW,
    SAME_AS_PREVIOUS,
    TIMESTAMP_DTYPE,
    TOC_BIG_ENDIAN,
    TOC_DAQMX,
    TOC_INTERLEAVED,
    TOC_META,
    TOC_NEW_OBJ_LIST,
    TOC_RAW,
    TYPE_BOOL,
    TYPE_STRING,
    TYPE_TIMESTAMP,
    NiTimestamp,
    TdmsError,
    TdmsReader,
)

FIXTURES = Path(__file__).parent / "fixtures"

I16, I32, U8, F8 = 0x02, 0x03, 0x05, 0x0A
STANDARD = TOC_META | TOC_NEW_OBJ_LIST | TOC_RAW


# --------------------------------------------------------------------------- byte builders


def u32(v: int, big: bool = False) -> bytes:
    return struct.pack(">I" if big else "<I", v)


def u64(v: int, big: bool = False) -> bytes:
    return struct.pack(">Q" if big else "<Q", v)


def tstring(s: str, big: bool = False) -> bytes:
    raw = s.encode("utf-8")
    return u32(len(raw), big) + raw


def timestamp_bytes(seconds: int, fractions: int, big: bool = False) -> bytes:
    return struct.pack(">qQ", seconds, fractions) if big else struct.pack("<Qq", fractions, seconds)


def index(type_code: int, n: int, big: bool = False, string_bytes: int | None = None) -> bytes:
    body = u32(type_code, big) + u32(1, big) + u64(n, big)
    if string_bytes is not None:
        body += u64(string_bytes, big)
    return u32(4 + len(body), big) + body


def daqmx_index(chunk: int, scalers: list[tuple], widths: list[int], big: bool = False) -> bytes:
    """A format-changing-scaler index; scalers are (daqmx type, buffer, byte offset[, scale id])."""
    out = u32(0x1269, big) + u32(0xFFFFFFFF, big) + u32(1, big) + u64(chunk, big) + u32(len(scalers), big)
    for daqmx_type, buffer_index, byte_offset, *rest in scalers:
        scale_id = rest[0] if rest else 0
        out += u32(daqmx_type, big) + u32(buffer_index, big) + u32(byte_offset, big) + u32(0, big) + u32(scale_id, big)
    out += u32(len(widths), big) + b"".join(u32(w, big) for w in widths)
    return out


def prop(name: str, type_code: int, value: bytes, big: bool = False) -> bytes:
    return tstring(name, big) + u32(type_code, big) + value


def obj(path: str, raw_index: bytes, props: list[bytes] = (), big: bool = False) -> bytes:
    return tstring(path, big) + raw_index + u32(len(props), big) + b"".join(props)


def segment(  # noqa: PLR0917
    objects: list[bytes] | None,
    raw: bytes = b"",
    toc: int = STANDARD,
    big: bool = False,
    next_offset: int | None = None,
    version: int = 4713,
) -> bytes:
    """A whole segment. ``objects=None`` means no metadata block at all."""
    if big:
        toc |= TOC_BIG_ENDIAN
    meta = b"" if objects is None else u32(len(objects), big) + b"".join(objects)
    if objects is None:
        toc &= ~TOC_META
    if next_offset is None:
        next_offset = len(meta) + len(raw)
    return b"TDSm" + u32(toc) + u32(version, big) + u64(next_offset, big) + u64(len(meta), big) + meta + raw


def values(fmt: str, *vals) -> bytes:
    return struct.pack(fmt, *vals)


def write(tmp_path: Path, *segments: bytes, name: str = "t.tdms") -> Path:
    path = tmp_path / name
    path.write_bytes(b"".join(segments))
    return path


def full(channel: reader.Channel) -> np.ndarray:
    return channel.values(0, len(channel))


# --------------------------------------------------------------------------- spec-level behaviour


class TestSpecExample:
    def test_the_worked_example_from_the_format_description(self, tmp_path):
        path = write(
            tmp_path,
            segment(
                [
                    obj(
                        "/'Group'",
                        u32(NO_RAW),
                        [prop("prop", TYPE_STRING, tstring("value")), prop("num", I32, values("<i", 10))],
                    ),
                    obj("/'Group'/'Channel1'", index(I32, 2)),
                ],
                values("<ii", 1, 2),
            ),
        )
        with TdmsReader.open(path) as f:
            (group,) = f.groups()
            assert group.name == "Group"
            assert group.properties == {"prop": "value", "num": 10}
            (channel,) = group.channels()
            assert channel.name == "Channel1"
            assert channel.dtype == np.dtype("int32")
            assert len(channel) == 2
            assert full(channel).tolist() == [1, 2]
            assert f["Group"]["Channel1"] is channel

    def test_version_4712_files_are_read_too(self, tmp_path):
        path = write(tmp_path, segment([obj("/'G'/'C'", index(U8, 3))], bytes([7, 8, 9]), version=4712))
        with TdmsReader.open(path) as f:
            assert full(f["G"]["C"]).tolist() == [7, 8, 9]

    def test_property_values_of_every_supported_type(self, tmp_path):
        props = [
            prop("i8", 0x01, values("<b", -5)),
            prop("i16", 0x02, values("<h", -300)),
            prop("i32", 0x03, values("<i", -70000)),
            prop("i64", 0x04, values("<q", -(1 << 40))),
            prop("u8", 0x05, values("<B", 250)),
            prop("u16", 0x06, values("<H", 65000)),
            prop("u32", 0x07, values("<I", 4_000_000_000)),
            prop("u64", 0x08, values("<Q", 1 << 63)),
            prop("f4", 0x09, values("<f", 1.5)),
            prop("f8", 0x0A, values("<d", 2.25)),
            prop("f8u", 0x1A, values("<d", 3.5)),
            prop("bool", TYPE_BOOL, b"\x01"),
            prop("str", TYPE_STRING, tstring("héllo 'quoted'")),
            prop("ts", TYPE_TIMESTAMP, timestamp_bytes(3_000_000_000, 1 << 63)),
            prop("cplx", 0x10000D, values("<dd", 1.0, -2.0)),
            prop("void", 0x00, b""),
        ]
        path = write(tmp_path, segment([obj("/'G'/'C'", index(U8, 1), props)], b"\x00"))
        with TdmsReader.open(path) as f:
            p = f["G"]["C"].properties
        assert p["i8"] == -5 and p["i16"] == -300 and p["i32"] == -70000 and p["i64"] == -(1 << 40)
        assert p["u8"] == 250 and p["u16"] == 65000 and p["u32"] == 4_000_000_000 and p["u64"] == 1 << 63
        assert p["f4"] == 1.5 and p["f8"] == 2.25 and p["f8u"] == 3.5
        assert p["bool"] is True
        assert p["str"] == "héllo 'quoted'"
        assert p["ts"] == NiTimestamp(3_000_000_000, 1 << 63)
        assert p["cplx"] == complex(1.0, -2.0)
        assert p["void"] is None
        assert all(type(p[k]) is int for k in ("i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"))

    def test_paths_unescape_doubled_quotes_and_keep_slashes(self, tmp_path):
        path = write(
            tmp_path,
            segment(
                [
                    obj("/'Bay/1'/'it''s ''quoted''/odd'", index(U8, 1)),  # channel before its group object
                    obj("/'Bay/1'", u32(NO_RAW), [prop("k", TYPE_STRING, tstring("v"))]),
                    obj("/", u32(NO_RAW), [prop("root", I32, values("<i", 1))]),
                ],
                b"\x05",
            ),
        )
        with TdmsReader.open(path) as f:
            (group,) = f.groups()
            assert group.name == "Bay/1"
            assert group.properties == {"k": "v"}
            assert [c.name for c in group.channels()] == ["it's 'quoted'/odd"]
            assert f.properties == {"root": 1}

    @pytest.mark.parametrize("path", ["", "Group", "/Group", "/'Group", "/'A'/'B'/'C'", "/'A'x"])
    def test_bad_object_paths_are_rejected(self, tmp_path, path):
        with pytest.raises(TdmsError, match="object path"):
            TdmsReader.open(write(tmp_path, segment([obj(path, u32(NO_RAW))])))


EXT_ONE = bytes(7) + bytes([0x80, 0xFF, 0x3F])  # 1.0 as x87 80-bit, little-endian
EXT_MINUS_2_5 = bytes(7) + bytes([0xA0, 0x00, 0xC0])


class TestExtendedFloats:
    def test_channel_and_property_values_decode_to_float64(self, tmp_path):
        props = [prop("p", 0x0B, EXT_MINUS_2_5)]
        path = write(tmp_path, segment([obj("/'G'/'X'", index(0x0B, 2), props)], EXT_ONE + EXT_MINUS_2_5))
        with TdmsReader.open(path) as f:
            x = f["G"]["X"]
            assert x.dtype == np.dtype("float64")
            assert full(x).tolist() == [1.0, -2.5]
            assert x.properties == {"p": -2.5}

    def test_big_endian_extended_floats(self, tmp_path):
        path = write(
            tmp_path,
            segment(
                [obj("/'G'/'X'", index(0x1B, 2, big=True), big=True)], EXT_ONE[::-1] + EXT_MINUS_2_5[::-1], big=True
            ),
        )
        with TdmsReader.open(path) as f:
            assert full(f["G"]["X"]).tolist() == [1.0, -2.5]

    def test_special_values(self, tmp_path):
        inf = bytes(7) + bytes([0x80, 0xFF, 0x7F])
        nan = bytes([0, 0, 0, 0, 0, 0, 0, 0xC0, 0xFF, 0x7F])
        denormal = bytes([1, 0, 0, 0, 0, 0, 0, 0, 0, 0])  # 2^-16445
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'X'", index(0x0B, 3))], inf + nan + denormal))) as f:
            values_ = full(f["G"]["X"])
            assert values_[0] == np.inf and np.isnan(values_[1]) and values_[2] == 0.0  # below float64's range

    @pytest.mark.skipif(not (FIXTURES / "ni" / "lv_ext.tdms").exists(), reason="no LabVIEW EXT recording")
    def test_the_labview_recording(self):
        with TdmsReader.open(FIXTURES / "ni" / "lv_ext.tdms") as f:
            (group,) = f.groups()
            assert full(group["EXT"]).tolist() == [1.0, -2.5, 1e100, 3.141592653589793, 0.1, -0.0]
            assert group.properties == {"p_ext": 1e-300}


class TestByteOrder:
    def test_big_endian_data_properties_and_timestamps(self, tmp_path):
        ts_prop = prop("start", TYPE_TIMESTAMP, timestamp_bytes(3_600_000_000, 12345, big=True), big=True)
        path = write(
            tmp_path,
            segment(
                [
                    obj(
                        "/'G'/'F'",
                        index(F8, 2, big=True),
                        [ts_prop, prop("n", I32, values(">i", -7), big=True)],
                        big=True,
                    ),
                    obj("/'G'/'T'", index(TYPE_TIMESTAMP, 2, big=True), big=True),
                ],
                values(">dd", 1.5, -2.5) + timestamp_bytes(10, 20, big=True) + timestamp_bytes(30, 40, big=True),
                big=True,
            ),
        )
        with TdmsReader.open(path) as f:
            fchan, tchan = f["G"]["F"], f["G"]["T"]
            assert fchan.properties == {"start": NiTimestamp(3_600_000_000, 12345), "n": -7}
            data = full(fchan)
            assert data.dtype == np.dtype("float64") and data.dtype.isnative
            assert data.tolist() == [1.5, -2.5]
            stamps = full(tchan)
            assert stamps.dtype == TIMESTAMP_DTYPE
            assert stamps["seconds"].tolist() == [10, 30]
            assert stamps["fractions"].tolist() == [20, 40]

    def test_little_endian_timestamps_store_fractions_first(self, tmp_path):
        path = write(tmp_path, segment([obj("/'G'/'T'", index(TYPE_TIMESTAMP, 1))], timestamp_bytes(10, 20)))
        with TdmsReader.open(path) as f:
            stamps = full(f["G"]["T"])
        assert stamps["seconds"].tolist() == [10] and stamps["fractions"].tolist() == [20]


class TestSegments:
    def test_incremental_metadata_keeps_and_extends_the_carried_list(self, tmp_path):
        seg1 = segment([obj("/'G'/'A'", index(U8, 2)), obj("/'G'/'B'", index(U8, 2))], bytes([1, 2, 11, 12]))
        # No new-object-list flag: A gets a property and, per NO_RAW, no raw
        # data in this segment (its slot switches off); C is appended, B is
        # untouched, per the format description's definition of 0xFFFFFFFF.
        seg2 = segment(
            [obj("/'G'/'A'", u32(NO_RAW), [prop("note", TYPE_STRING, tstring("x"))]), obj("/'G'/'C'", index(U8, 2))],
            bytes([13, 14, 21, 22]),
            toc=TOC_META | TOC_RAW,
        )
        seg3 = segment(None, bytes([15, 16, 23, 24]), toc=TOC_RAW)  # no metadata at all
        # Still no new-object-list flag: A rejoins in its original slot, ahead of B.
        seg4 = segment([obj("/'G'/'A'", u32(SAME_AS_PREVIOUS))], bytes([5, 6, 17, 18, 25, 26]), toc=TOC_META | TOC_RAW)
        seg5 = segment([obj("/'G'/'B'", u32(SAME_AS_PREVIOUS))], bytes([19, 20]))  # new list: B only
        with TdmsReader.open(write(tmp_path, seg1, seg2, seg3, seg4, seg5)) as f:
            a, b, c = (f["G"][n] for n in "ABC")
            assert a.properties == {"note": "x"}
            assert full(a).tolist() == [1, 2, 5, 6]
            assert full(b).tolist() == [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
            assert full(c).tolist() == [21, 22, 23, 24, 25, 26]

    def test_same_as_previous_without_an_earlier_index_is_an_error(self, tmp_path):
        with pytest.raises(TdmsError, match="earlier segment"):
            TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", u32(SAME_AS_PREVIOUS))])))

    def test_an_object_listed_twice_with_raw_data_is_rejected(self, tmp_path):
        # A doubled entry would take two blocks of every chunk and shift every
        # later channel's values; there is no right reading of it.
        seg = segment(
            [obj("/'G'/'A'", index(I16, 2)), obj("/'G'/'A'", index(I16, 2)), obj("/'G'/'B'", index(I16, 2))],
            values("<hhhhhh", 1, 2, 3, 4, 11, 12),
        )
        with pytest.raises(TdmsError, match="listed more than once"):
            TdmsReader.open(write(tmp_path, seg))

    def test_a_raw_block_can_repeat_the_chunk_layout(self, tmp_path):
        chunk = values("<hh", 1, 2) + values("<d", 0.5)
        path = write(tmp_path, segment([obj("/'G'/'S'", index(I16, 2)), obj("/'G'/'D'", index(F8, 1))], chunk * 3))
        with TdmsReader.open(path) as f:
            s, d = f["G"]["S"], f["G"]["D"]
            assert len(s) == 6 and len(d) == 3
            everything = full(s)
            assert everything.tolist() == [1, 2, 1, 2, 1, 2]
            assert full(d).tolist() == [0.5, 0.5, 0.5]
            for a, n in [(1, 4), (2, 2), (3, 10), (5, 1), (0, 6)]:
                assert s.values(a, n).tolist() == everything[a : a + n].tolist()

    def test_a_partial_trailing_chunk_counts_in_len_but_reads_short(self, tmp_path):
        chunk = values("<hh", 1, 2) + values("<hh", 3, 4)  # A then B, 2 values each
        raw = chunk * 2 + values("<hhh", 1, 2, 3)  # third chunk lost its last value
        path = write(
            tmp_path,
            segment([obj("/'G'/'A'", index(I16, 2)), obj("/'G'/'B'", index(I16, 2))], raw, next_offset=NEXT_UNKNOWN),
        )
        with TdmsReader.open(path) as f:
            a, b = f["G"]["A"], f["G"]["B"]
            assert len(a) == len(b) == 6
            assert full(a).tolist() == [1, 2, 1, 2, 1, 2]
            assert full(b).tolist() == [3, 4, 3, 4, 3]
            assert b.values(5, 1).tolist() == []
            assert b.values(4, 2).tolist() == [3]

    def test_a_file_cut_short_is_read_to_its_actual_end(self, tmp_path):
        whole = segment([obj("/'G'/'A'", index(I32, 4))], values("<iiii", 1, 2, 3, 4))
        with TdmsReader.open(write(tmp_path, whole[:-6])) as f:  # says 4 values, holds 2 and a half
            a = f["G"]["A"]
            assert len(a) == 4
            assert full(a).tolist() == [1, 2]

    def test_a_segment_after_a_truncated_one_is_never_reached(self, tmp_path):
        first = segment([obj("/'G'/'A'", index(U8, 2))], bytes([1, 2]), next_offset=NEXT_UNKNOWN)
        second = segment([obj("/'G'/'A'", index(U8, 2))], bytes([3, 4]))
        with TdmsReader.open(write(tmp_path, first, second)) as f:
            a = f["G"]["A"]
            # Everything to the end of the file is the first segment's raw block,
            # the second segment's bytes included, in whole and partial chunks.
            raw_bytes = 2 + len(second)
            assert len(a) == 2 * ((raw_bytes + 1) // 2)
            assert full(a)[:2].tolist() == [1, 2]

    def test_raw_flag_with_no_bytes_or_no_values_yields_nothing(self, tmp_path):
        empty_block = segment([obj("/'G'/'A'", index(U8, 2))], b"")
        zero_values = segment([obj("/'G'/'A'", index(U8, 0))], bytes([9, 9]))
        with TdmsReader.open(write(tmp_path, empty_block)) as f:
            assert len(f["G"]["A"]) == 0 and full(f["G"]["A"]).tolist() == []
        with TdmsReader.open(write(tmp_path, zero_values)) as f:
            assert len(f["G"]["A"]) == 0

    def test_reads_past_the_end_are_empty_with_the_channel_dtype(self, tmp_path):
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", index(F8, 1))], values("<d", 1.0)))) as f:
            a = f["G"]["A"]
            assert a.values(5, 3).dtype == np.dtype("float64") and len(a.values(5, 3)) == 0
            assert a.values(0, 100).tolist() == [1.0]
            assert a.values(-3, 2).tolist() == [1.0]

    def test_a_channel_with_properties_but_no_data(self, tmp_path):
        with TdmsReader.open(
            write(tmp_path, segment([obj("/'G'/'A'", u32(NO_RAW), [prop("x", I32, values("<i", 1))])]))
        ) as f:
            a = f["G"]["A"]
            assert a.dtype is None and len(a) == 0 and a.properties == {"x": 1}
            assert len(a.values(0, 10)) == 0


class TestInterleaved:
    def test_channels_of_different_widths_are_deinterleaved(self, tmp_path):
        row = lambda i, x: values("<h", i) + values("<d", x)  # noqa: E731
        raw = b"".join(row(i, i / 2) for i in range(3)) * 2  # two chunks of three rows
        path = write(
            tmp_path,
            segment(
                [obj("/'G'/'S'", index(I16, 3)), obj("/'G'/'D'", index(F8, 3))], raw, toc=STANDARD | TOC_INTERLEAVED
            ),
        )
        with TdmsReader.open(path) as f:
            assert full(f["G"]["S"]).tolist() == [0, 1, 2, 0, 1, 2]
            assert full(f["G"]["D"]).tolist() == [0, 0.5, 1, 0, 0.5, 1]
            assert f["G"]["D"].values(2, 3).tolist() == [1, 0, 0.5]

    def test_unequal_counts_or_strings_are_rejected(self, tmp_path):
        with pytest.raises(TdmsError, match="different sample counts"):
            TdmsReader.open(
                write(
                    tmp_path,
                    segment(
                        [obj("/'G'/'A'", index(U8, 2)), obj("/'G'/'B'", index(U8, 3))],
                        bytes(5),
                        toc=STANDARD | TOC_INTERLEAVED,
                    ),
                )
            )
        with pytest.raises(TdmsError, match="interleaved string"):
            TdmsReader.open(
                write(
                    tmp_path,
                    segment(
                        [obj("/'G'/'A'", index(TYPE_STRING, 1, string_bytes=5))],
                        bytes(5),
                        toc=STANDARD | TOC_INTERLEAVED,
                    ),
                )
            )


class TestStrings:
    @staticmethod
    def string_chunk(items: list[str]) -> bytes:
        payload = b"".join(s.encode("utf-8") for s in items)
        ends, total = [], 0
        for s in items:
            total += len(s.encode("utf-8"))
            ends.append(total)
        return b"".join(u32(e) for e in ends) + payload

    def test_strings_across_chunks_and_windows(self, tmp_path):
        items = ["a", "", "°C", "dddd"]
        chunk = self.string_chunk(items)
        path = write(tmp_path, segment([obj("/'G'/'S'", index(TYPE_STRING, 4, string_bytes=len(chunk)))], chunk * 2))
        with TdmsReader.open(path) as f:
            s = f["G"]["S"]
            assert s.dtype == np.dtype(object) and len(s) == 8
            assert full(s).tolist() == items * 2
            assert s.values(1, 2).tolist() == ["", "°C"]
            assert s.values(3, 3).tolist() == ["dddd", "a", ""]

    def test_a_string_block_too_small_for_its_offsets_is_rejected(self, tmp_path):
        with pytest.raises(TdmsError, match="cannot hold"):
            TdmsReader.open(
                write(tmp_path, segment([obj("/'G'/'S'", index(TYPE_STRING, 3, string_bytes=8))], bytes(8)))
            )

    def test_inconsistent_offsets_are_rejected_on_read(self, tmp_path):
        bad = u32(3) + u32(1) + b"abc"  # ends go backwards
        path = write(tmp_path, segment([obj("/'G'/'S'", index(TYPE_STRING, 2, string_bytes=len(bad)))], bad))
        with TdmsReader.open(path) as f:
            with pytest.raises(TdmsError, match="offsets"):
                full(f["G"]["S"])

    def test_a_truncated_string_chunk_yields_nothing(self, tmp_path):
        chunk = self.string_chunk(["ab", "cd"])
        path = write(
            tmp_path,
            segment(
                [obj("/'G'/'S'", index(TYPE_STRING, 2, string_bytes=len(chunk)))],
                chunk + chunk[:-1],
                next_offset=NEXT_UNKNOWN,
            ),
        )
        with TdmsReader.open(path) as f:
            assert len(f["G"]["S"]) == 4
            assert full(f["G"]["S"]).tolist() == ["ab", "cd"]


def linear(i: int, slope: float, intercept: float, source: int = 0) -> list[bytes]:
    return [
        prop(f"NI_Scale[{i}]_Scale_Type", TYPE_STRING, tstring("Linear")),
        prop(f"NI_Scale[{i}]_Linear_Slope", F8, values("<d", slope)),
        prop(f"NI_Scale[{i}]_Linear_Y_Intercept", F8, values("<d", intercept)),
        prop(f"NI_Scale[{i}]_Linear_Input_Source", I32, values("<i", source)),
    ]


def scale_type(i: int, kind: str) -> bytes:
    return prop(f"NI_Scale[{i}]_Scale_Type", TYPE_STRING, tstring(kind))


def f8_array(prefix: str, numbers: list[float]) -> list[bytes]:
    return [prop(f"{prefix}_Size", I32, values("<i", len(numbers)))] + [
        prop(f"{prefix}[{k}]", F8, values("<d", x)) for k, x in enumerate(numbers)
    ]


def unscaled(n_scales: int) -> list[bytes]:
    return [
        prop("NI_Scaling_Status", TYPE_STRING, tstring("unscaled")),
        prop("NI_Number_Of_Scales", I32, values("<i", n_scales)),
    ]


UNSCALED = unscaled(2)
DAQMX = TOC_META | TOC_NEW_OBJ_LIST | TOC_RAW | TOC_INTERLEAVED | TOC_DAQMX


def digital_index(chunk: int, bit: int, width: int = 1, big: bool = False) -> bytes:
    """A digital-line index: like the format-changing one, but a one-byte sample format field."""
    out = u32(0x126A, big) + u32(0xFFFFFFFF, big) + u32(1, big) + u64(chunk, big) + u32(1, big)
    out += u32(0, big) + u32(0, big) + u32(bit, big) + b"\x00" + u32(0, big)
    return out + u32(1, big) + u32(width, big)


class TestDaqmx:
    def test_format_changing_scalers_share_one_raw_buffer(self, tmp_path):
        rows = b"".join(values("<hh", i, -i) for i in range(4))  # 4 samples of two int16 channels, 4-byte stride
        path = write(
            tmp_path,
            segment(
                [
                    obj("/'G'/'A'", daqmx_index(4, [(3, 0, 0)], [4]), UNSCALED + linear(1, 0.5, 1.0)),
                    obj("/'G'/'B'", daqmx_index(4, [(3, 0, 2)], [4])),
                ],
                rows * 2,
                toc=DAQMX,
            ),
        )
        with TdmsReader.open(path) as f:
            a, b = f["G"]["A"], f["G"]["B"]
            assert a.dtype == np.dtype("float64") and b.dtype == np.dtype("int16")
            assert len(a) == len(b) == 8
            assert full(a).tolist() == [1.0, 1.5, 2.0, 2.5] * 2
            assert full(b).tolist() == [0, -1, -2, -3] * 2
            assert a.values(3, 2).tolist() == [2.5, 1.0]

    def test_two_raw_buffers_of_different_width(self, tmp_path):
        chunk = values("<hh", 1, 2) + values("<ii", 3, 4)  # buffer 0: int16 x2 samples; buffer 1: int32 x2 samples
        path = write(
            tmp_path,
            segment(
                [
                    obj("/'G'/'A'", daqmx_index(2, [(3, 0, 0)], [2, 4])),
                    obj("/'G'/'B'", daqmx_index(2, [(5, 1, 0)], [2, 4])),
                ],
                chunk,
                toc=DAQMX,
            ),
        )
        with TdmsReader.open(path) as f:
            assert full(f["G"]["A"]).tolist() == [1, 2]
            assert full(f["G"]["B"]).tolist() == [3, 4]

    def test_digital_lines_are_bits_of_the_port_sample(self, tmp_path):
        port = bytes([0b0000_0101, 0b0000_0010, 0b1000_0001])  # three samples of an 8-bit port
        path = write(
            tmp_path,
            segment(
                [
                    obj("/'G'/'line0'", digital_index(3, 0)),
                    obj("/'G'/'line2'", digital_index(3, 2)),
                    obj("/'G'/'line7'", digital_index(3, 7)),
                ],
                port,
                toc=DAQMX,
            ),
        )
        with TdmsReader.open(path) as f:
            assert f["G"]["line0"].dtype == np.dtype("uint8")
            assert full(f["G"]["line0"]).tolist() == [1, 0, 1]
            assert full(f["G"]["line2"]).tolist() == [1, 0, 0]
            assert full(f["G"]["line7"]).tolist() == [0, 0, 1]

    def test_two_raw_scalers_combined_by_a_subtract_stage(self, tmp_path):
        # Signal (id 0) and cold junction (id 2) as int32 in one 8-byte sample,
        # each scaled linearly, then subtracted: the shape of a thermocouple chain.
        rows = values("<ii", 1000, 100) + values("<ii", 2000, 100)
        props = (
            unscaled(5)
            + linear(1, 0.01, 0.0, source=0)
            + linear(3, 0.01, 0.0, source=2)
            + [
                scale_type(4, "Subtract"),
                prop("NI_Scale[4]_Subtract_Left_Operand_Input_Source", I32, values("<i", 1)),
                prop("NI_Scale[4]_Subtract_Right_Operand_Input_Source", I32, values("<i", 3)),
            ]
        )
        path = write(
            tmp_path,
            segment([obj("/'G'/'T'", daqmx_index(2, [(5, 0, 0, 0), (5, 0, 4, 2)], [8]), props)], rows, toc=DAQMX),
        )
        with TdmsReader.open(path) as f:
            t = f["G"]["T"]
            assert t.unsupported is None and t.dtype == np.dtype("float64")
            assert full(t).tolist() == [-9.0, -19.0]  # DAQmx's Subtract is right minus left

    def test_a_channel_whose_values_cannot_be_produced_is_marked_not_failed(self, tmp_path):
        two_raw = daqmx_index(2, [(3, 0, 0, 0), (3, 0, 2, 2)], [4])  # two scalers, nothing to combine them
        unknown = daqmx_index(2, [(42, 0, 0)], [4])
        path = write(
            tmp_path,
            segment(
                [obj("/'G'/'A'", two_raw), obj("/'G'/'B'", unknown), obj("/'G'/'C'", daqmx_index(2, [(3, 0, 2)], [4]))],
                bytes(8),
                toc=DAQMX,
            ),
        )
        with TdmsReader.open(path) as f:
            assert "no NI scaling to combine" in f["G"]["A"].unsupported
            assert "scaler data type 42" in f["G"]["B"].unsupported
            assert f["G"]["A"].dtype is None and len(f["G"]["A"].values(0, 2)) == 0
            assert full(f["G"]["C"]).tolist() == [0, 0]  # its neighbours still read

    @pytest.mark.parametrize(
        "bad_index, message",
        [
            (daqmx_index(2, [(3, 1, 0)], [2]), "raw buffer 1 of 1"),
            (daqmx_index(2, [(5, 0, 0)], [2]), "overruns"),
            (digital_index(2, 9, width=1), "overruns"),
            # Two scalers with one scale id would silently collapse to one raw stream.
            (daqmx_index(2, [(3, 0, 0, 0), (3, 0, 2, 0)], [4]), "share a scale id"),
        ],
    )
    def test_indices_that_break_the_chunk_layout_are_rejected(self, tmp_path, bad_index, message):
        with pytest.raises(TdmsError, match=message):
            TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", bad_index)], bytes(8), toc=DAQMX)))

    def test_channels_disagreeing_on_the_buffer_layout_are_rejected(self, tmp_path):
        seg = segment(
            [obj("/'G'/'A'", daqmx_index(2, [(3, 0, 0)], [2])), obj("/'G'/'B'", daqmx_index(2, [(3, 0, 0)], [4]))],
            bytes(8),
            toc=DAQMX,
        )
        with pytest.raises(TdmsError, match="disagree"):
            TdmsReader.open(write(tmp_path, seg))

    def test_a_scaler_set_change_between_segments_excludes_the_channel(self, tmp_path):
        # Segment 2 gives A a second scaler. Were A read anyway, a scale graph
        # over both ids would KeyError on segment 1's runs; B still reads.
        seg1 = segment(
            [obj("/'G'/'A'", daqmx_index(2, [(3, 0, 0)], [4])), obj("/'G'/'B'", daqmx_index(2, [(3, 0, 2)], [4]))],
            bytes(8),
            toc=DAQMX,
        )
        seg2 = segment(
            [
                obj("/'G'/'A'", daqmx_index(2, [(3, 0, 0, 0), (3, 0, 2, 2)], [4])),
                obj("/'G'/'B'", u32(SAME_AS_PREVIOUS)),
            ],
            bytes(8),
            toc=DAQMX,
        )
        with TdmsReader.open(write(tmp_path, seg1, seg2)) as f:
            assert "layout changed between segments" in f["G"]["A"].unsupported
            assert f["G"]["A"].dtype is None and len(full(f["G"]["A"])) == 0
            assert full(f["G"]["B"]).tolist() == [0, 0, 0, 0]

    def test_a_scaler_type_change_between_segments_excludes_the_channel(self, tmp_path):
        # int16 in segment 1, int32 in segment 2: one channel cannot be both.
        seg1 = segment([obj("/'G'/'A'", daqmx_index(2, [(3, 0, 0)], [4]))], bytes(8), toc=DAQMX)
        seg2 = segment([obj("/'G'/'A'", daqmx_index(2, [(5, 0, 0)], [4]))], bytes(8), toc=DAQMX)
        with TdmsReader.open(write(tmp_path, seg1, seg2)) as f:
            assert "layout changed between segments" in f["G"]["A"].unsupported


class TestScaling:
    def test_already_scaled_channels_keep_their_raw_type(self, tmp_path):
        props = [prop("NI_Scaling_Status", TYPE_STRING, tstring("scaled"))] + linear(1, 2.0, 0.0)
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", index(I16, 1), props)], values("<h", 5)))) as f:
            assert f["G"]["A"].dtype == np.dtype("int16") and full(f["G"]["A"]).tolist() == [5]

    def test_stages_chain_through_their_input_sources(self, tmp_path):
        props = unscaled(3) + linear(1, 2.0, 1.0) + linear(2, 10.0, -3.0, source=1)  # (x*2+1)*10-3
        with TdmsReader.open(
            write(tmp_path, segment([obj("/'G'/'A'", index(I16, 2), props)], values("<hh", 0, 1)))
        ) as f:
            assert full(f["G"]["A"]).tolist() == [7.0, 27.0]

    def test_polynomial_stage(self, tmp_path):
        props = (
            unscaled(3)
            + linear(1, 0.5, 0.0)
            + [scale_type(2, "Polynomial")]
            + f8_array("NI_Scale[2]_Polynomial_Coefficients", [1.0, 0.0, 2.0])
            + [prop("NI_Scale[2]_Polynomial_Input_Source", I32, values("<i", 1))]
        )  # 1 + 2*(x/2)^2
        with TdmsReader.open(
            write(tmp_path, segment([obj("/'G'/'A'", index(I16, 3), props)], values("<hhh", 0, 2, 4)))
        ) as f:
            assert full(f["G"]["A"]).tolist() == [1.0, 3.0, 9.0]

    def test_table_stage_interpolates_and_clamps(self, tmp_path):
        # Input runs along Scaled_Values, output along Pre_Scaled_Values (see scaling._table).
        props = (
            unscaled(2)
            + [scale_type(1, "Table")]
            + f8_array("NI_Scale[1]_Table_Scaled_Values", [0.0, 10.0, 20.0])
            + f8_array("NI_Scale[1]_Table_Pre_Scaled_Values", [0.0, 100.0, 300.0])
            + [prop("NI_Scale[1]_Table_Input_Source", I32, values("<i", 0))]
        )
        with TdmsReader.open(
            write(tmp_path, segment([obj("/'G'/'A'", index(I16, 5), props)], values("<hhhhh", -5, 0, 5, 15, 25)))
        ) as f:
            assert full(f["G"]["A"]).tolist() == [0.0, 0.0, 50.0, 200.0, 300.0]

    def test_thermocouple_stage_follows_the_nist_inverse_functions(self, tmp_path):
        # NIST ITS-90 type K table: 4.096 mV at 100 degC, -5.891 mV at -200 degC.
        props = (
            unscaled(3)
            + linear(1, 1.0, 0.0)
            + [
                scale_type(2, "Thermocouple"),
                prop("NI_Scale[2]_Thermocouple_Thermocouple_Type", I32, values("<i", 10073)),
                prop("NI_Scale[2]_Thermocouple_Scaling_Direction", I32, values("<i", 0)),
                prop("NI_Scale[2]_Thermocouple_Input_Source", I32, values("<i", 1)),
            ]
        )
        microvolts = values("<ii", 4096, -5891)
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'T'", index(I32, 2), props)], microvolts))) as f:
            t = full(f["G"]["T"])
            # The table's emf is rounded to 1 uV, which is 0.07 degC at -200 degC.
            assert abs(t[0] - 100.0) < 0.05 and abs(t[1] - (-200.0)) < 0.1

    def test_rtd_stage_inverts_callendar_van_dusen(self, tmp_path):
        # Pt100 (A = 3.9083e-3, B = -5.775e-7, C = -4.183e-12): 138.5055 ohm at 100 degC,
        # 60.2558 ohm at -100 degC; 1 mA excitation, so the stage sees those in volts / 1000.
        f8 = lambda k, x: prop(f"NI_Scale[1]_RTD_{k}", F8, values("<d", x))  # noqa: E731
        props = unscaled(2) + [
            scale_type(1, "RTD"),
            f8("Current_Excitation", 0.001),
            f8("R0_Nominal_Resistance", 100.0),
            f8("A", 0.0039083),
            f8("B", -5.775e-07),
            f8("C", -4.183e-12),
            f8("Lead_Wire_Resistance", 0.0),
            prop("NI_Scale[1]_RTD_Resistance_Configuration", I32, values("<i", 4)),
            prop("NI_Scale[1]_RTD_Input_Source", I32, values("<i", 0)),
        ]
        volts = values("<dd", 0.1385055, 0.0602558)
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'R'", index(F8, 2), props)], volts))) as f:
            t = full(f["G"]["R"])
            assert abs(t[0] - 100.0) < 1e-3 and abs(t[1] - (-100.0)) < 1e-3

    def test_strain_stage_full_bridge(self, tmp_path):
        # Full bridge I: strain = -Vr / GF with Vr = (V - V0) / Vex.
        f8 = lambda k, x: prop(f"NI_Scale[1]_Strain_{k}", F8, values("<d", x))  # noqa: E731
        props = unscaled(2) + [
            scale_type(1, "Strain"),
            prop("NI_Scale[1]_Strain_Configuration", I32, values("<i", 10183)),
            f8("Poisson_Ratio", 0.3),
            f8("Gage_Resistance", 350.0),
            f8("Lead_Wire_Resistance", 0.0),
            f8("Initial_Bridge_Voltage", 0.001),
            f8("Gage_Factor", 2.0),
            f8("Bridge_Shunt_Calibration_Gain_Adjustment", 1.0),
            f8("Voltage_Excitation", 2.5),
            prop("NI_Scale[1]_Strain_Input_Source", I32, values("<i", 0)),
        ]
        with TdmsReader.open(
            write(tmp_path, segment([obj("/'G'/'S'", index(F8, 1), props)], values("<d", 0.006)))
        ) as f:
            assert full(f["G"]["S"]).tolist() == pytest.approx([-(0.005 / 2.5) / 2.0])

    def test_a_chain_of_thousands_of_stages_evaluates(self, tmp_path):
        # Deeper than Python's recursion limit: parsing and applying the graph
        # must both be iterative, or RecursionError escapes as neither a
        # TdmsError nor an exclusion.
        n = 3000
        props = unscaled(n)
        for i in range(1, n):
            props += linear(i, 1.0, 1.0, source=i - 1)  # each stage adds 1
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", index(I16, 1), props)], values("<h", 0)))) as f:
            assert full(f["G"]["A"]).tolist() == [float(n - 1)]

    def test_a_stage_cycle_excludes_the_channel(self, tmp_path):
        props = unscaled(3) + linear(1, 1.0, 0.0, source=2) + linear(2, 1.0, 0.0, source=1)
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", index(I16, 1), props)], values("<h", 0)))) as f:
            assert "cycle" in f["G"]["A"].unsupported

    def test_an_unknown_scale_type_excludes_only_that_channel(self, tmp_path):
        props = UNSCALED + [scale_type(1, "Reciprocal")]
        path = write(
            tmp_path,
            segment([obj("/'G'/'A'", index(I16, 1), props), obj("/'G'/'B'", index(I16, 1))], values("<hh", 0, 7)),
        )
        with TdmsReader.open(path) as f:
            assert "'Reciprocal' is not supported" in f["G"]["A"].unsupported
            assert full(f["G"]["B"]).tolist() == [7]


class TestMalformed:
    def test_garbage_is_not_a_tdms_file(self, tmp_path):
        with pytest.raises(TdmsError, match="not a TDMS file"):
            TdmsReader.open(write(tmp_path, b"this is not a TDMS file, just bytes" * 4))

    @pytest.mark.parametrize(
        "content, message",
        [
            (b"TDSm" + bytes(10), "not a TDMS file"),
            (segment([obj("/'G'/'A'", index(U8, 1))], b"\x00") + b"XXXX" + bytes(24), "bad segment tag"),
            (b"TDSm" + u32(STANDARD) + u32(4713) + u64(4) + u64(8) + bytes(4), "past its own end"),
            (segment([obj("/'G'/'A'", index(U8, 1))])[:-6], "ends inside the metadata"),
            (b"TDSm" + u32(STANDARD) + u32(4713) + u64(8) + u64(8) + u32(1) + u32(100), "metadata truncated"),
            (segment([obj("/'G'/'A'", index(0x4F, 1))]), "fixed point"),
            (segment([obj("/'G'/'A'", index(0xFFFFFFFF, 1))]), "not supported"),
            (segment([obj("/'G'/'A'", u32(24) + u32(U8) + u32(2) + u64(1))]), "dimension 2"),
            (
                segment([obj("/'G'/'A'", index(U8, 1))], b"\x00") + segment([obj("/'G'/'A'", index(I16, 1))], bytes(2)),
                "type changed",
            ),
            (segment([obj("/'G'/'A'", u32(NO_RAW), [prop("p", 0x4F, bytes(16))])]), "fixed point"),
            (
                segment([obj("/'G'/'A'", u32(NO_RAW), [tstring("p") + u32(TYPE_STRING) + u32(2) + b"\xff\xfe"])]),
                "not UTF-8",
            ),
        ],
    )
    def test_damaged_or_unsupported_input_is_a_tdms_error(self, tmp_path, content, message):
        with pytest.raises(TdmsError, match=message):
            TdmsReader.open(write(tmp_path, content))

    def test_trailing_bytes_after_the_last_segment_are_ignored(self, tmp_path, caplog):
        with TdmsReader.open(write(tmp_path, segment([obj("/'G'/'A'", index(U8, 1))], b"\x07") + b"junk")) as f:
            assert full(f["G"]["A"]).tolist() == [7]
        assert "trailing bytes" in caplog.text


# --------------------------------------------------------------------------- agreement with DAQmx itself

DAQMX_FIRST_SAMPLES = FIXTURES / "ni" / "daqmx_first_samples.json"


@pytest.mark.skipif(not DAQMX_FIRST_SAMPLES.exists(), reason="no DAQmx recordings")
class TestAgreementWithDaqmx:
    """The values DAQmx handed the recording script must come back out of the file."""

    def test_first_samples_match_what_daqmx_read(self):
        expected = {k: v for k, v in json.loads(DAQMX_FIRST_SAMPLES.read_text()).items() if not k.startswith("_")}
        for name, samples in expected.items():
            with TdmsReader.open(FIXTURES / "ni" / name) as f:
                channel = f.groups()[0].channels()[0]  # DAQmx logs one group per channel for digital lines
                assert channel.unsupported is None, (name, channel.unsupported)
                ours = channel.values(0, len(samples))
            # The script prints DAQmx's doubles rounded to 6 decimals; the relative
            # term covers summation-order differences on the huge out-of-range
            # values types B, R and S extrapolate to.
            assert np.allclose(ours, samples, rtol=1e-8, atol=5.1e-7), (name, ours.tolist(), samples)


# --------------------------------------------------------------------------- frozen NI recordings

EXPECTED_CHANNELS = FIXTURES / "ni" / "expected_channels.json"


@pytest.mark.skipif(not EXPECTED_CHANNELS.exists(), reason="no NI recordings")
class TestFrozenRecordings:
    """Every LabVIEW and DAQmx recording reads as it did when it was verified against NI's software."""

    @pytest.fixture
    def expected(self) -> dict:
        return {k: v for k, v in json.loads(EXPECTED_CHANNELS.read_text()).items() if not k.startswith("_")}

    def test_every_recording_is_listed(self, expected):
        assert {p.name for p in (FIXTURES / "ni").glob("*.tdms")} == set(expected)

    def test_channels_match_their_frozen_contents(self, expected):
        for name, channels in expected.items():
            with TdmsReader.open(FIXTURES / "ni" / name) as f:
                seen = {}
                for group in f.groups():
                    for channel in group.channels():
                        seen[channel.path] = channel
                assert set(seen) == set(channels), name
                for path, want in channels.items():
                    channel = seen[path]
                    if "unsupported" in want:
                        assert channel.unsupported == want["unsupported"], path
                        continue
                    assert (
                        channel.unsupported is None
                        and str(channel.dtype) == want["dtype"]
                        and len(channel) == want["len"]
                    ), path
                    got = channel.values(0, len(want["first"]))
                    if channel.dtype == TIMESTAMP_DTYPE:
                        assert [[int(s), int(fr)] for fr, s in got.tolist()] == want["first"], path
                    elif channel.dtype.kind == "f":
                        want_f = np.array([np.nan if v is None else v for v in want["first"]])
                        assert np.allclose(
                            got,
                            want_f,
                            rtol=1e-12,
                            atol=1e-12 * (np.nanmax(np.abs(want_f)) if want_f.size else 1.0),
                            equal_nan=True,
                        ), path
                    else:
                        assert got.tolist() == want["first"], path
