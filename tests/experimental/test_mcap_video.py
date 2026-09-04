"""Tests for deriving direct-MCAP registrations.

Every H.264 fixture is built from a specification rather than recorded, so an assertion about a
decoded picture size is checked against what the bitstream was constructed to say.
"""

from __future__ import annotations

import pytest

pytest.importorskip("mcap", exc_type=ImportError)

from nominal.experimental.mcap_video import (  # noqa: E402
    McapBitstreamFormat,
    McapMessageEncoding,
    McapVideoScanError,
    scan_mcap_video,
)
from nominal.experimental.mcap_video._bitstream import (  # noqa: E402
    FrameDecodeError,
    extract_cdr_frame,
    extract_protobuf_frame,
    split_annex_b,
    split_length_prefixed,
)
from nominal.experimental.mcap_video._sps import SpsParseError, parse_sps  # noqa: E402
from tests.experimental._mcap_video_fixtures import (  # noqa: E402
    NANOS_PER_SECOND,
    ROS_VIDEO_SCHEMA,
    annex_b,
    build_sps,
    cdr_compressed_video,
    length_prefixed,
    protobuf_compressed_video,
    video_messages,
    write_mcap,
)


def _sps_from(payload: bytes):
    return parse_sps(split_annex_b(payload))


class TestSpsDimensions:
    def test_decodes_the_size_the_sps_was_built_with(self) -> None:
        info = _sps_from(annex_b(build_sps(width_mbs=8, height_map_units=4)))
        assert info is not None
        assert (info.width, info.height) == (128, 64)
        assert info.codec_string == "avc1.4d0028"

    def test_codec_string_is_profile_constraints_level(self) -> None:
        info = _sps_from(annex_b(build_sps(profile_idc=0x42, constraint_flags=0xC0, level_idc=0x1F)))
        assert info is not None
        assert info.codec_string == "avc1.42c01f"

    def test_cropping_uses_chroma_units_for_420(self) -> None:
        # 4:2:0 crops in 2-sample units horizontally, so cropping 1 unit removes 2 pixels.
        info = _sps_from(annex_b(build_sps(width_mbs=8, height_map_units=4, crop=(0, 1, 0, 0))))
        assert info is not None
        assert (info.width, info.height) == (126, 64)

    def test_monochrome_crops_in_whole_samples(self) -> None:
        """Regression: monochrome is not 4:2:0 with the colour turned off.

        The IR cameras record monochrome. Treating chroma_array_type 0 as 4:2:0 halves the crop and
        reports a picture wider than the stream actually is.
        """
        info = _sps_from(
            annex_b(build_sps(profile_idc=100, chroma_format_idc=0, width_mbs=8, height_map_units=4, crop=(0, 1, 0, 0)))
        )
        assert info is not None
        assert (info.width, info.height) == (127, 64)

    def test_walks_a_present_scaling_list(self) -> None:
        """Regression: an SPS carrying a scaling matrix used to raise NameError.

        The coefficients sit between the profile fields and the dimensions, so they must be walked
        or every field after them is read at the wrong bit offset.
        """
        info = _sps_from(annex_b(build_sps(profile_idc=100, scaling_matrix=True, width_mbs=8, height_map_units=4)))
        assert info is not None
        assert (info.width, info.height) == (128, 64)

    def test_interlaced_doubles_the_frame_height(self) -> None:
        info = _sps_from(annex_b(build_sps(width_mbs=8, height_map_units=4, frame_mbs_only=0)))
        assert info is not None
        assert (info.width, info.height) == (128, 128)

    def test_truncated_sps_raises_a_parse_error(self) -> None:
        """Regression: reading past the end used to raise IndexError from inside the bit reader."""
        truncated = build_sps()[:5]
        with pytest.raises(SpsParseError):
            _sps_from(annex_b(truncated))

    def test_no_sps_present_returns_none(self) -> None:
        # 0x41 is a non-IDR slice, not a parameter set.
        assert _sps_from(annex_b(b"\x41\x00\x11\x22")) is None


class TestNalSplitting:
    def test_four_byte_start_codes_leave_no_trailing_zero(self) -> None:
        """Regression: ending a unit a fixed 3 bytes before the next start kept a stray zero."""
        first, second = b"\x67\x01\x02\x03", b"\x68\x04\x05\x06"
        assert split_annex_b(annex_b(first, second, four_byte=True)) == [first, second]

    def test_three_byte_start_codes(self) -> None:
        first, second = b"\x67\x01\x02\x03", b"\x68\x04\x05\x06"
        assert split_annex_b(annex_b(first, second)) == [first, second]

    def test_length_prefixed_round_trip(self) -> None:
        units = [b"\x67\xaa\xbb", b"\x68\xcc"]
        assert split_length_prefixed(length_prefixed(*units)) == units

    def test_length_prefixed_stops_on_a_bogus_prefix(self) -> None:
        assert split_length_prefixed(b"\xff\xff\xff\xff\x01\x02") == []


class TestFrameExtraction:
    def test_protobuf_carries_data_and_format(self) -> None:
        frame = extract_protobuf_frame(protobuf_compressed_video(b"\x00\x01\x02", "h264"))
        assert frame.data == b"\x00\x01\x02"
        assert frame.format == "h264"

    def test_protobuf_without_data_is_an_error(self) -> None:
        with pytest.raises(FrameDecodeError):
            extract_protobuf_frame(protobuf_compressed_video(b"", None)[:0] or b"\x20\x01")

    def test_cdr_carries_data_and_format(self) -> None:
        frame = extract_cdr_frame(cdr_compressed_video(b"\xde\xad\xbe\xef", "h264"))
        assert frame.data == b"\xde\xad\xbe\xef"
        assert frame.format == "h264"

    def test_cdr_finds_the_frame_a_protobuf_reader_would_miss(self) -> None:
        """The two envelopes are genuinely different; a protobuf walk over CDR is not a near miss."""
        sps = build_sps()
        message = cdr_compressed_video(annex_b(sps), "h264")
        assert extract_cdr_frame(message).data == annex_b(sps)


def _protobuf_video_mcap(**kwargs: object) -> bytes:
    frame = annex_b(build_sps(width_mbs=8, height_map_units=4))
    payload = protobuf_compressed_video(frame)
    return write_mcap(
        [("/cam/front", "protobuf", video_messages(20, payload, start_ns=NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND))],
        **kwargs,  # type: ignore[arg-type]
    )


class TestScan:
    def test_derives_a_channel_per_video_topic(self) -> None:
        scan = scan_mcap_video_bytes(_protobuf_video_mcap())
        assert len(scan.channels) == 1
        channel = scan.channels[0]
        assert channel.topic == "/cam/front"
        assert channel.channel == "cam_front"
        assert (channel.width, channel.height) == (128, 64)
        assert channel.message_encoding is McapMessageEncoding.PROTOBUF
        assert channel.bitstream_format is McapBitstreamFormat.ANNEX_B
        assert channel.chunks

    def test_separates_telemetry_from_video(self) -> None:
        frame = protobuf_compressed_video(annex_b(build_sps()))
        data = write_mcap(
            [("/cam", "protobuf", video_messages(10, frame, start_ns=NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND))],
            telemetry_topics=["/imu", "/gps"],
        )
        scan = scan_mcap_video_bytes(data)
        assert scan.video_topics == ("/cam",)
        assert scan.telemetry_topics == ("/gps", "/imu")

    def test_cdr_recordings_scan(self) -> None:
        frame = cdr_compressed_video(annex_b(build_sps()))
        data = write_mcap(
            [("/cam", "cdr", video_messages(10, frame, start_ns=NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND))],
            schema_name=ROS_VIDEO_SCHEMA,
        )
        scan = scan_mcap_video_bytes(data)
        assert scan.channels[0].message_encoding is McapMessageEncoding.CDR
        assert scan.channels[0].width == 128

    def test_each_channel_is_timed_by_its_own_chunks(self) -> None:
        """Regression: start, end and frame rate used to come from file-level statistics.

        A camera that runs for part of a recording would then be given the whole file's span, so its
        start time preceded its first frame and its frame rate was diluted by the gap.
        """
        frame = protobuf_compressed_video(annex_b(build_sps()))
        early = video_messages(10, frame, start_ns=1 * NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND // 10)
        late = video_messages(10, frame, start_ns=60 * NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND // 10)
        data = write_mcap([("/early", "protobuf", early), ("/late", "protobuf", late)])

        scan = scan_mcap_video_bytes(data)
        by_topic = {channel.topic: channel for channel in scan.channels}
        assert by_topic["/early"].end < by_topic["/late"].start
        # Both ran for about a second at ~10fps. Taken from the file's 60s span they would read ~0.16.
        for channel in scan.channels:
            assert 5.0 < channel.frame_rate < 20.0

    def test_a_file_with_no_summary_is_refused(self) -> None:
        with pytest.raises(McapVideoScanError, match="summary"):
            scan_mcap_video_bytes(_protobuf_video_mcap(use_chunking=False))

    def test_a_recording_with_no_video_is_refused(self) -> None:
        data = write_mcap([], telemetry_topics=["/imu"])
        with pytest.raises(McapVideoScanError, match="no compressed-video topics"):
            scan_mcap_video_bytes(data)

    def test_h265_is_refused_rather_than_labelled_h264(self) -> None:
        """The API accepts H.265; this scanner does not derive it, and says so."""
        frame = protobuf_compressed_video(annex_b(build_sps()), "h265")
        data = write_mcap(
            [("/cam", "protobuf", video_messages(5, frame, start_ns=NANOS_PER_SECOND, step_ns=NANOS_PER_SECOND))]
        )
        with pytest.raises(McapVideoScanError, match="H.265"):
            scan_mcap_video_bytes(data)


class TestScanEditing:
    def _scan(self):
        return scan_mcap_video_bytes(_protobuf_video_mcap())

    def test_with_tags_applies_to_every_channel(self) -> None:
        tagged = self._scan().with_tags({"vehicle": "nazgul"})
        assert all(dict(c.tags) == {"vehicle": "nazgul"} for c in tagged.channels)

    def test_rename_channels_is_keyed_by_topic(self) -> None:
        renamed = self._scan().rename_channels({"/cam/front": "front_eo"})
        assert renamed.channels[0].channel == "front_eo"
        assert renamed.channels[0].topic == "/cam/front"

    def test_rename_rejects_an_unknown_topic(self) -> None:
        with pytest.raises(KeyError, match="no such topic"):
            self._scan().rename_channels({"/nope": "x"})

    def test_validate_accepts_a_scan_from_a_real_file(self) -> None:
        self._scan().validate()

    def test_validate_rejects_too_many_chunk_ranges(self) -> None:
        from dataclasses import replace

        from nominal.experimental.mcap_video._types import MAX_CHUNK_RANGES_PER_REQUEST, McapChunkRange

        scan = self._scan()
        overfull = replace(
            scan.channels[0],
            chunks=tuple(McapChunkRange(start=0, end=1) for _ in range(MAX_CHUNK_RANGES_PER_REQUEST + 1)),
        )
        with pytest.raises(ValueError, match="chunk ranges exceeds"):
            replace(scan, channels=(overfull,)).validate()


def scan_mcap_video_bytes(data: bytes):
    """Scan an in-memory MCAP, which the scanner accepts as any seekable binary stream."""
    from io import BytesIO

    return scan_mcap_video(BytesIO(data))
