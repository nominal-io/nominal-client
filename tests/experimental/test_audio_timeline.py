"""Tests for audio-timeline diagnosis and the repair decision it drives in normalize_video.

Fixtures are synthesized with real ffmpeg — no external media. Two defect shapes are built:

* **holes** — the clock advances past the sound, as a lossy capture produces when packets are
  dropped in transit.
* **overlap** — packets stamped a fraction of a frame apart while each still decodes a full
  frame, as a recorder produces when it flushes a late burst.

The two call for opposite repairs and routinely occur in the same file, so each is measured on
its own.
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

from nominal.experimental.video_processing import (
    AUDIO_REPAIR_FILTER,
    AudioDefect,
    AudioTimelineError,
    audio_repair_filter,
    diagnose_audio,
    normalize_video,
    survives_strict_segmentation,
    timeline_from_packets,
)

requires_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="requires ffmpeg and ffprobe on PATH",
)

# One sample at 96kHz is less than one tick of the 90kHz MPEG-TS clock, so sub-frame packet
# spacing at this rate reliably collides during segmentation.
_COLLIDING_RATE = 96000


def _build(
    path: pathlib.Path,
    *,
    seconds: int,
    audio_rate: int,
    audio_seconds: int,
    audio_filter: str | None,
    audio_codec: str = "aac",
    channels: int = 2,
) -> None:
    command = [
        "ffmpeg", "-y", "-v", "error",
        "-f", "lavfi", "-i", f"testsrc2=size=128x72:rate=15:duration={seconds}",
        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate={audio_rate}:duration={audio_seconds}",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
    ]  # fmt: skip
    if audio_filter is not None:
        command += ["-af", audio_filter]
    command += ["-c:a", audio_codec, "-ac", str(channels), str(path)]
    subprocess.run(command, check=True, capture_output=True)


@pytest.fixture
def healthy_video(tmp_path: pathlib.Path) -> pathlib.Path:
    """A well-formed video whose audio clock matches its audio content."""
    path = tmp_path / "healthy.mp4"
    _build(path, seconds=6, audio_rate=48000, audio_seconds=6, audio_filter=None)
    return path


@pytest.fixture
def overlap_video(tmp_path: pathlib.Path) -> pathlib.Path:
    """A video whose audio packets are stamped one sample apart, so ingest rejects it."""
    path = tmp_path / "overlap.mp4"
    _build(
        path,
        seconds=8,
        audio_rate=_COLLIDING_RATE,
        audio_seconds=8,
        audio_filter=r"asetpts=N-1023*max(0\,min((N-204800)/1024\,50))",
    )
    return path


@pytest.fixture
def gaping_video(tmp_path: pathlib.Path) -> pathlib.Path:
    """A video whose audio carries a 60-second hole alongside sub-frame packet spacing."""
    path = tmp_path / "gaping.mp4"
    _build(
        path,
        seconds=90,
        audio_rate=_COLLIDING_RATE,
        audio_seconds=30,
        audio_filter=r"asetpts=N-1023*max(0\,min((N-204800)/1024\,50))+gte(N/96000\,6)*60*96000",
    )
    return path


def _video_timestamps(path: pathlib.Path) -> str:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "packet=pts",
            "-of",
            "csv=p=0",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


@requires_ffmpeg
def test_healthy_audio_reports_no_defect(healthy_video: pathlib.Path) -> None:
    """A coherent audio track is diagnosed as coherent and selects no filter."""
    diagnosis = diagnose_audio(healthy_video)

    assert diagnosis.defect is AudioDefect.NONE
    assert diagnosis.segments_cleanly
    assert audio_repair_filter(healthy_video) is None


@requires_ffmpeg
def test_video_without_audio_is_recognized(healthy_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """A file carrying no audio track is reported with its own classification."""
    silent = tmp_path / "silent.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", str(healthy_video), "-an", "-c", "copy", str(silent)],
        check=True,
        capture_output=True,
    )

    assert diagnose_audio(silent).defect is AudioDefect.NO_AUDIO_TRACK


@requires_ffmpeg
def test_sub_frame_packet_spacing_is_measured_as_overlap(overlap_video: pathlib.Path) -> None:
    """Packets stamped closer together than the sound they carry are reported as overlap."""
    diagnosis = diagnose_audio(overlap_video)
    timeline = diagnosis.timeline

    assert timeline is not None
    assert diagnosis.defect is AudioDefect.OVERLAP
    assert timeline.overlap_seconds > 0.25
    assert timeline.hole_seconds == pytest.approx(0.0, abs=0.25)


@requires_ffmpeg
def test_missing_audio_is_measured_as_holes(gaping_video: pathlib.Path) -> None:
    """A dropout is reported as a hole whose measured size matches the audio that went missing."""
    timeline = diagnose_audio(gaping_video).timeline

    assert timeline is not None
    assert timeline.largest_hole_seconds == pytest.approx(60.0, abs=1.0)
    # The same file also carries overlap, and both are reported.
    assert timeline.overlap_seconds > 0.0


# Containers differ in how they store audio timestamps -- MP4 in sample units, Matroska in
# milliseconds, MPEG-TS at 90kHz -- and the measurement has to be right in all of them.
_SHAPES = [
    pytest.param("mp4", "aac", 48000, 2, id="aac-stereo-48k-mp4"),
    pytest.param("mkv", "aac", 48000, 2, id="aac-stereo-48k-mkv"),
    pytest.param("ts", "aac", 48000, 2, id="aac-stereo-48k-ts"),
    pytest.param("mov", "aac", 44100, 1, id="aac-mono-44k-mov"),
    pytest.param("mp4", "aac", 96000, 2, id="aac-stereo-96k-mp4"),
    pytest.param("mkv", "libmp3lame", 44100, 2, id="mp3-stereo-44k-mkv"),
]


@requires_ffmpeg
@pytest.mark.parametrize(("container", "codec", "rate", "channels"), _SHAPES)
def test_ingestible_audio_is_left_byte_identical(
    tmp_path: pathlib.Path, container: str, codec: str, rate: int, channels: int
) -> None:
    """Audio that already segments keeps the conversion it would get with inspection disabled."""
    source = tmp_path / f"source.{container}"
    _build(
        source, seconds=6, audio_rate=rate, audio_seconds=6, audio_filter=None,
        audio_codec=codec, channels=channels,
    )  # fmt: skip
    suffix = ".mkv" if container == "mkv" else ".mp4"
    repaired = tmp_path / f"repaired{suffix}"
    untouched = tmp_path / f"untouched{suffix}"

    normalize_video(source, repaired, repair_audio=True)
    normalize_video(source, untouched, repair_audio=False)

    assert diagnose_audio(source).defect is AudioDefect.NONE
    assert audio_repair_filter(source) is None


@requires_ffmpeg
def test_codecs_without_a_fixed_packet_size_are_left_alone(tmp_path: pathlib.Path) -> None:
    """PCM has no fixed samples-per-packet, so its timeline is reported unmeasurable, not guessed."""
    source = tmp_path / "pcm.mov"
    _build(
        source, seconds=4, audio_rate=48000, audio_seconds=4, audio_filter=None,
        audio_codec="pcm_s16le",
    )  # fmt: skip

    assert diagnose_audio(source).defect is AudioDefect.UNMEASURABLE
    assert audio_repair_filter(source) is None


@requires_ffmpeg
def test_rejected_audio_is_repaired_into_an_ingestible_file(
    overlap_video: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A file that fails to segment is rebuilt into one that segments."""
    assert not survives_strict_segmentation(overlap_video)

    output = tmp_path / "normalized.mp4"
    normalize_video(overlap_video, output)

    assert survives_strict_segmentation(output)


@requires_ffmpeg
def test_repair_does_not_move_video_timestamps(overlap_video: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """Repairing audio leaves every video timestamp exactly where it was."""
    repaired = tmp_path / "repaired.mp4"
    unrepaired = tmp_path / "unrepaired.mp4"

    normalize_video(overlap_video, repaired, repair_audio=True)
    normalize_video(overlap_video, unrepaired, repair_audio=False)

    assert _video_timestamps(repaired) == _video_timestamps(unrepaired)


@requires_ffmpeg
def test_oversized_hole_is_left_alone_rather_than_filled(gaping_video: pathlib.Path) -> None:
    """A gap too large to be a real dropout selects no filter, so the conversion is unchanged."""
    assert audio_repair_filter(gaping_video, max_audio_hole_seconds=10) is None


@requires_ffmpeg
def test_a_file_that_still_cannot_segment_fails_before_upload(
    gaping_video: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """When the repair is declined and the result still cannot segment, that is raised to the caller."""
    with pytest.raises(AudioTimelineError, match="still cannot be segmented"):
        normalize_video(gaping_video, tmp_path / "out.mp4", max_audio_hole_seconds=10)


@requires_ffmpeg
def test_conversion_is_not_verified_when_audio_inspection_is_disabled(
    gaping_video: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Opting out of audio handling opts out of the post-conversion check along with it."""
    output = tmp_path / "unverified.mp4"

    normalize_video(gaping_video, output, repair_audio=False)

    assert output.exists()


@requires_ffmpeg
def test_oversized_hole_is_filled_when_the_caller_raises_the_limit(
    gaping_video: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The hole limit is a guardrail, not a refusal: raising it permits the repair."""
    output = tmp_path / "out.mp4"

    assert audio_repair_filter(gaping_video, max_audio_hole_seconds=120) == AUDIO_REPAIR_FILTER

    normalize_video(gaping_video, output, max_audio_hole_seconds=120)

    assert survives_strict_segmentation(output)


@requires_ffmpeg
def test_audio_outlasting_video_is_left_alone(tmp_path: pathlib.Path) -> None:
    """Streams that disagree about the recording's length are reported, not altered or rejected."""
    path = tmp_path / "long_audio.mp4"
    _build(path, seconds=4, audio_rate=48000, audio_seconds=10, audio_filter=None)

    assert audio_repair_filter(path) is None
    normalize_video(path, tmp_path / "out.mp4")  # the conversion completes


_RATE = 48000
_FRAME = 1024
_PACKET_SECONDS = _FRAME / _RATE


def test_evenly_spaced_packets_measure_as_coherent() -> None:
    """Packets spaced exactly one frame apart carry neither holes nor overlap."""
    timeline = timeline_from_packets([i * _PACKET_SECONDS for i in range(100)], _PACKET_SECONDS, _FRAME, _RATE)

    assert timeline is not None
    assert timeline.defect is AudioDefect.NONE
    assert timeline.hole_seconds == 0.0
    assert timeline.overlap_seconds == 0.0


def test_dropped_packets_measure_as_a_hole() -> None:
    """Packets missing from the middle are reported as one hole of exactly the elapsed duration."""
    missing = 30  # packets; comfortably above the tolerance that separates noise from a defect
    starts = [n * _PACKET_SECONDS for n in (0, 1, 2 + missing, 3 + missing)]

    timeline = timeline_from_packets(starts, _PACKET_SECONDS, _FRAME, _RATE)

    assert timeline is not None
    assert timeline.defect is AudioDefect.HOLES
    assert timeline.hole_seconds == pytest.approx(missing * _PACKET_SECONDS)
    assert timeline.hole_count == 1
    assert timeline.overlap_seconds == 0.0


def test_holes_and_overlap_are_reported_separately_not_netted() -> None:
    """A file holding both holes and overlap reports each of them at its measured size."""
    span = 30  # packets of each defect, so neither is dismissed as rounding noise
    # A dropout, then a burst of packets stamped almost on top of one another as a buffer flushes.
    starts = [0.0, (1 + span) * _PACKET_SECONDS]
    starts += [starts[-1] + i / 48000 for i in range(1, span + 1)]

    timeline = timeline_from_packets(starts, _PACKET_SECONDS, _FRAME, _RATE)

    assert timeline is not None
    assert timeline.hole_seconds == pytest.approx(span * _PACKET_SECONDS)
    assert timeline.overlap_seconds == pytest.approx(span * _PACKET_SECONDS, rel=0.01)
    assert timeline.defect is AudioDefect.MIXED


def test_millisecond_timestamp_rounding_is_not_reported_as_damage() -> None:
    """Timestamps stored at whole-millisecond resolution measure as a coherent timeline."""
    # 400 packets stamped at whole-millisecond resolution, as a Matroska muxer would write them.
    starts = [round(n * _PACKET_SECONDS, 3) for n in range(400)]

    timeline = timeline_from_packets(starts, _PACKET_SECONDS, _FRAME, _RATE)

    assert timeline is not None
    assert timeline.defect is AudioDefect.NONE
    assert timeline.hole_seconds == 0.0
    assert timeline.overlap_seconds == 0.0


def test_measuring_needs_at_least_one_packet() -> None:
    """Measuring requires at least one packet, and yields no timeline without one."""
    assert timeline_from_packets([], _PACKET_SECONDS, _FRAME, _RATE) is None
