"""Tests for normalize_video's audio-timeline repair.

These tests exercise real ffmpeg (skipped when it isn't installed). Fixtures are synthesized:
a short test-pattern video whose audio timestamp table (the MP4 ``stts`` box) is rewritten in
place to carry degenerate per-packet durations — the failure class seen from recorders that
splice audio, where packets are stamped fractions of a frame apart. Such timelines survive a
plain re-encode (the encoder stamps output packets from decoded-frame timestamps) and later
fail strict, timestamp-preserving remuxes with non-monotonic DTS errors.
"""

from __future__ import annotations

import pathlib
import shutil
import struct
import subprocess

import pytest

pytest.importorskip("ffmpeg", exc_type=ImportError)
import ffmpeg  # noqa: E402

from nominal.experimental.video_processing.video_conversion import (  # noqa: E402
    DEFAULT_AUDIO_FILTER,
    normalize_video,
)

pytestmark = pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="requires ffmpeg and ffprobe on PATH",
)

AAC_SAMPLES_PER_FRAME = 1024


@pytest.fixture
def base_video(tmp_path: pathlib.Path) -> pathlib.Path:
    """A short, well-formed H264+AAC video generated from ffmpeg test sources."""
    path = tmp_path / "base.mp4"
    video = ffmpeg.input("testsrc2=size=128x72:rate=30", f="lavfi")
    audio = ffmpeg.input("sine=frequency=440:sample_rate=48000", f="lavfi")
    ffmpeg.output(
        video,
        audio,
        str(path),
        t=3,
        vcodec="libx264",
        pix_fmt="yuv420p",
        acodec="aac",
    ).run(quiet=True)
    return path


@pytest.fixture
def degenerate_audio_video(base_video: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """The base video with its audio timestamp table corrupted to sub-frame packet spacing."""
    path = tmp_path / "degenerate.mp4"
    path.write_bytes(_with_degenerate_audio_stts(base_video.read_bytes()))
    # The corruption must be visible to a demuxer, or the tests below prove nothing.
    assert min(_audio_packet_durations(path)) < AAC_SAMPLES_PER_FRAME
    return path


_MP4_CONTAINER_BOXES = {b"moov", b"trak", b"mdia", b"minf", b"stbl"}


def _find_audio_stts(data: bytes, start: int, end: int, path: tuple[int, ...]) -> tuple[int, tuple[int, ...]] | None:
    """Locate the stts box whose first sample delta is one AAC frame, with its ancestor offsets.

    Walks the MP4 box tree (``size | fourcc | payload``); returns the stts box offset and the
    offsets of the container boxes enclosing it, outermost first.
    """
    offset = start
    while offset + 8 <= end:
        (size,) = struct.unpack(">I", data[offset : offset + 4])
        fourcc = data[offset + 4 : offset + 8]
        if size < 8:
            return None
        if fourcc in _MP4_CONTAINER_BOXES:
            if (found := _find_audio_stts(data, offset + 8, offset + size, (*path, offset))) is not None:
                return found
        elif fourcc == b"stts":
            entry_count, _, delta = struct.unpack(">III", data[offset + 12 : offset + 24])
            if entry_count >= 1 and delta == AAC_SAMPLES_PER_FRAME:
                return offset, path
        offset += size
    return None


def _with_degenerate_audio_stts(data: bytes) -> bytes:
    """Splice a run of 1-sample packet durations into the audio track's timestamp table.

    An stts box maps packets to durations as ``(sample_count, sample_delta)`` runs. The generated
    fixture's audio track has a single run of frame-sized deltas; this rewrites it so that
    mid-stream, a run of packets claims 1-sample (~20.8µs) durations while each still decodes to a
    full AAC frame (21.3ms) — the overlapping-timeline shape observed from recorders that splice
    audio.
    A partial-resync run follows, then the normal cadence resumes. Total packet count is
    preserved; the file's chunk offsets stay valid because the fixture's moov trails the mdat, so
    growing it shifts nothing the offsets point at.
    """
    found = _find_audio_stts(data, 0, len(data), ())
    assert found is not None, "fixture has no audio stts leading with an AAC frame-sized delta"
    stts_at, ancestors = found
    entry_count, first_run_count = struct.unpack(">II", data[stts_at + 12 : stts_at + 20])
    trailing = [struct.unpack(">II", data[stts_at + 20 + 8 * i : stts_at + 28 + 8 * i]) for i in range(entry_count - 1)]
    glitch_at = first_run_count // 2
    assert first_run_count >= glitch_at + 10, "fixture audio too short to hold the glitch run"
    entries = [
        (glitch_at, AAC_SAMPLES_PER_FRAME),
        (8, 1),  # the degenerate splice: sub-frame packet spacing
        (1, 2 * AAC_SAMPLES_PER_FRAME),  # partial resync after the splice
        (first_run_count - glitch_at - 9, AAC_SAMPLES_PER_FRAME),
        *trailing,
    ]
    payload = struct.pack(">II", 0, len(entries)) + b"".join(struct.pack(">II", n, d) for n, d in entries)
    new_stts = struct.pack(">I", len(payload) + 8) + b"stts" + payload

    (old_size,) = struct.unpack(">I", data[stts_at : stts_at + 4])
    grown_by = len(new_stts) - old_size
    patched = bytearray(data[:stts_at] + new_stts + data[stts_at + old_size :])
    for ancestor_at in ancestors:
        (ancestor_size,) = struct.unpack(">I", patched[ancestor_at : ancestor_at + 4])
        patched[ancestor_at : ancestor_at + 4] = struct.pack(">I", ancestor_size + grown_by)
    return bytes(patched)


def _audio_packet_durations(path: pathlib.Path) -> list[int]:
    packets = ffmpeg.probe(str(path), select_streams="a:0", show_entries="packet=duration", v="error")["packets"]
    return [int(p["duration"]) for p in packets]


def _audio_packet_dts(path: pathlib.Path) -> list[int]:
    packets = ffmpeg.probe(str(path), select_streams="a:0", show_entries="packet=dts", v="error")["packets"]
    return [int(p["dts"]) for p in packets]


def _max_audio_grid_deviation(path: pathlib.Path) -> int:
    """Worst distance (in 1/48000 ticks) of any audio packet from a continuous frame grid.

    Every AAC packet decodes to one frame of samples, so on a healthy timeline packet ``i`` is
    stamped ``i`` frames after the first packet. The deviation from that grid measures how much
    claimed time diverges from decoded samples — the defect this module's audio filter repairs.
    """
    dts = _audio_packet_dts(path)
    return max(abs(d - (dts[0] + i * AAC_SAMPLES_PER_FRAME)) for i, d in enumerate(dts))


def _video_packet_pts(path: pathlib.Path) -> list[int]:
    packets = ffmpeg.probe(str(path), select_streams="v:0", show_entries="packet=pts", v="error")["packets"]
    return [int(p["pts"]) for p in packets]


def _normalize_without_audio_filter(input_path: pathlib.Path, output_path: pathlib.Path) -> None:
    """normalize_video's ffmpeg invocation minus the audio filter — the pre-fix behavior."""
    ffmpeg.input(str(input_path)).output(
        str(output_path),
        acodec="aac",
        vcodec="h264",
        pix_fmt="yuv420p",
        force_key_frames="expr:gte(t,n_forced*2)",
    ).run(quiet=True)


def _strict_remux_to_mpegts(input_path: pathlib.Path, output_path: pathlib.Path) -> bool:
    """Stream-copy to MPEG-TS preserving timestamps exactly, failing on any ffmpeg error.

    This mirrors how ingest segments video: -copyts with negative-timestamp avoidance disabled
    disables ffmpeg's automatic DTS fixup, so a non-monotonic audio timeline is a hard error
    instead of a silently-corrected warning.
    """
    result = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-xerror",
            "-y",
            "-copyts",
            "-i",
            str(input_path),
            "-c",
            "copy",
            "-mpegts_copyts",
            "1",
            "-avoid_negative_ts",
            "disabled",
            str(output_path),
        ],  # fmt: skip
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


# The fixture's splice displaces the audio timeline by ~7 AAC frames (9 packets decode 9 frames
# of samples while the claimed timeline advances ~2). A repaired timeline must come back well
# under that; an inherited one cannot.
GLITCH_DISPLACEMENT_TICKS = 7 * AAC_SAMPLES_PER_FRAME
REPAIRED_DEVIATION_BOUND_TICKS = 4 * AAC_SAMPLES_PER_FRAME


def test_repairs_degenerate_audio_timeline(degenerate_audio_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    assert _max_audio_grid_deviation(degenerate_audio_video) >= GLITCH_DISPLACEMENT_TICKS

    output = tmp_path / "normalized.mp4"
    normalize_video(degenerate_audio_video, output)

    dts = _audio_packet_dts(output)
    assert dts, "normalized output lost its audio track"
    assert all(later > earlier for earlier, later in zip(dts, dts[1:]))
    assert _max_audio_grid_deviation(output) < REPAIRED_DEVIATION_BOUND_TICKS

    assert _strict_remux_to_mpegts(output, tmp_path / "normalized.ts")


def test_plain_reencode_does_not_repair_audio(degenerate_audio_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """The audio filter is load-bearing: without it, re-encoding inherits the broken timeline."""
    output = tmp_path / "reencoded_no_filter.mp4"
    _normalize_without_audio_filter(degenerate_audio_video, output)

    durations = _audio_packet_durations(output)
    assert 1 in durations, "sub-frame packet durations should survive a plain re-encode"
    assert _max_audio_grid_deviation(output) >= REPAIRED_DEVIATION_BOUND_TICKS


def test_audio_filter_does_not_affect_video_timestamps(base_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    with_filter = tmp_path / "with_filter.mp4"
    without_filter = tmp_path / "without_filter.mp4"
    normalize_video(base_video, with_filter)
    _normalize_without_audio_filter(base_video, without_filter)

    assert _video_packet_pts(with_filter) == _video_packet_pts(without_filter)


def test_video_only_input_normalizes(base_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    video_only = tmp_path / "video_only.mp4"
    ffmpeg.input(str(base_video)).output(str(video_only), an=None, c="copy").run(quiet=True)

    output = tmp_path / "video_only_normalized.mp4"
    normalize_video(video_only, output)
    assert _video_packet_pts(output), "normalized output has no video stream"


def test_default_audio_filter_is_async_resample() -> None:
    """Guards the repair mechanism: async resampling is what rebuilds the audio timeline."""
    assert "aresample" in DEFAULT_AUDIO_FILTER
    assert "async=" in DEFAULT_AUDIO_FILTER
