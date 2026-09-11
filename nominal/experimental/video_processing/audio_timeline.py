"""Diagnose an audio track's timeline, and decide whether repairing it is safe.

A container stores audio as packets plus a table of packet *durations*; a packet's start time is
the running sum of the durations before it. The sound itself is a fixed number of samples per
packet, decided by the codec. Nothing forces the two to agree, so a file can carry a clock that
advances faster than its audio (*holes*) or slower (*overlap*) while every timestamp still
increases. Both shapes occur, frequently in the same file, and they need opposite repairs — so
they are measured separately and never netted against each other.

Checks are ordered by cost:

* :func:`probe_audio_stream` reads the container header (~0.1s on a 7GB file).
* :func:`survives_strict_segmentation` stream-copies to MPEG-TS preserving timestamps exactly,
  decoding nothing (~1s on a 7GB file). This is the authoritative answer to "will this file be
  rejected?" — it asks ffmpeg rather than modelling its timestamp arithmetic, which is easy to
  get wrong and cheap to simply measure.
* :func:`measure_audio_timeline` walks every audio packet (~8s on a 7GB file) to separate holes
  from overlap and locate the worst one.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import json
import logging
import os
import subprocess
from typing import Sequence

from nominal.core._types import PathLike

logger = logging.getLogger(__name__)

DEFAULT_MAX_AUDIO_HOLE_SECONDS = 10.0
"""Largest single run of missing audio that will be filled with silence.

Beyond this a timestamp is far likelier to be corrupt than to describe a real dropout, and filling
it would synthesize that much silence from one bad number. Larger gaps are reported and the audio
is left alone.
"""

MAX_AUDIO_OVERRUN_SECONDS = 1.0
"""How far audio may outlast video before it is reported as a stream-length disagreement.

Audio ending early is ordinary — a track can simply stop. Audio running past the end of the video
means the two streams disagree about the length of the recording, which no audio-only repair can
reconcile.
"""

DEFAULT_TOLERANCE_SECONDS = 0.25
"""Slack allowed before a measurement counts as a defect, absorbing encoder priming and rounding."""

AUDIO_REPAIR_FILTER = "aresample=async=1"
"""Rebuilds audio onto the timeline the container already declares.

``async=1`` selects filling and trimming only: silence is inserted where the clock says sound is
missing, and samples with no time to play in are dropped. Values above 1 additionally permit
*stretching* — resampling audio to chase a drifting clock, which alters the timing of every sample
after it. We never want that: the declared timeline is authoritative, so content is adjusted to fit
it rather than the other way around. Every value at or above 1 repaired the failures we measured
identically, so this takes the least invasive one.
"""

_CONSTANT_FRAME_CODECS = frozenset({"aac", "mp3", "ac3", "eac3", "opus", "vorbis"})
"""Codecs whose packets each decode to a fixed number of samples, making the sound countable."""

_SAMPLE_WINDOW_SECONDS = 2
"""How much audio to decode when learning a codec's samples-per-packet."""

_QUANTIZATION_DEADBAND_FRACTION = 0.25
"""Share of one packet's duration that a timestamp may deviate before it counts as a defect.

Absorbs the rounding a container applies when its time base is coarser than the audio's, which
would otherwise accumulate into minutes of phantom holes across a long recording.
"""


class AudioDefect(enum.Enum):
    """What, if anything, is wrong with an audio track's timeline."""

    NONE = "none"
    """Clock and sound agree; the track is coherent."""

    NO_AUDIO_TRACK = "no_audio_track"
    """The file carries no audio at all."""

    HOLES = "holes"
    """The clock covers more time than there is sound: audio is missing."""

    OVERLAP = "overlap"
    """There is more sound than time to play it in: packets are stamped too close together."""

    MIXED = "mixed"
    """Both holes and overlap are present, typically a lossy capture with jitter-buffer flushes."""

    UNMEASURABLE = "unmeasurable"
    """Audio is present but its packets have no fixed sample count, so it cannot be compared."""


class AudioTimelineError(Exception):
    """Raised when a normalized file still cannot be segmented, so uploading it would fail."""


@dataclasses.dataclass(frozen=True)
class AudioStreamInfo:
    """Header fields of an audio stream, parsed once so callers never re-coerce raw ffprobe text."""

    codec: str
    sample_rate: int
    channels: int
    duration_seconds: float | None


@dataclasses.dataclass(frozen=True)
class AudioTimeline:
    """A measured comparison between what a container claims about its audio and what the audio is.

    This type exists only when the measurement succeeded; a track that cannot be measured produces
    no timeline rather than one populated with zeroes that would read as "nothing wrong".
    """

    codec: str
    sample_rate: int
    channels: int
    packet_count: int
    clock_seconds: float
    """Span the container's timeline claims, first packet start to last packet end."""
    sound_seconds: float
    """Audio actually present, from packet count times the codec's samples per packet."""
    hole_seconds: float
    """Total time the clock covers that holds no sound."""
    overlap_seconds: float
    """Total sound that has no time on the clock to play in."""
    largest_hole_seconds: float
    hole_count: int
    overlap_count: int

    @property
    def defect(self) -> AudioDefect:
        """Classify the track, using the same tolerance the repair decision uses."""
        holes = self.hole_seconds > DEFAULT_TOLERANCE_SECONDS
        overlap = self.overlap_seconds > DEFAULT_TOLERANCE_SECONDS
        if holes and overlap:
            return AudioDefect.MIXED
        if holes:
            return AudioDefect.HOLES
        if overlap:
            return AudioDefect.OVERLAP
        return AudioDefect.NONE

    def describe(self) -> str:
        """One line naming what is wrong, for logs and operator-facing messages."""
        if self.defect is AudioDefect.NONE:
            return f"audio timeline is coherent ({self.packet_count} packets, {self.clock_seconds:.2f}s)"
        parts = []
        if self.hole_seconds > DEFAULT_TOLERANCE_SECONDS:
            gaps = "gap" if self.hole_count == 1 else "gaps"
            parts.append(
                f"{self.hole_seconds:.2f}s of missing audio across {self.hole_count} {gaps} "
                f"(largest {self.largest_hole_seconds:.2f}s)"
            )
        if self.overlap_seconds > DEFAULT_TOLERANCE_SECONDS:
            parts.append(f"{self.overlap_seconds:.2f}s of audio with no time to play in ({self.overlap_count} packets)")
        return "; ".join(parts)


@dataclasses.dataclass(frozen=True)
class AudioDiagnosis:
    """Everything measured about a file's audio, and whether ingest will accept it as-is."""

    defect: AudioDefect
    segments_cleanly: bool
    """Whether the file survives a strict, timestamp-preserving segmentation pass."""
    timeline: AudioTimeline | None
    """The measurement, or None when there is no audio track or the codec cannot be measured."""
    stream: AudioStreamInfo | None = None
    video_seconds: float | None = None

    @property
    def audio_seconds(self) -> float | None:
        """Duration the container records for the audio track, if any."""
        return self.stream.duration_seconds if self.stream is not None else None


def _run_ffprobe(args: Sequence[str]) -> dict[str, object]:
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-of", "json", *args],
        capture_output=True,
        text=True,
        check=True,
    )
    parsed: dict[str, object] = json.loads(result.stdout)
    return parsed


def _first_stream(response: dict[str, object]) -> dict[str, object] | None:
    streams = response.get("streams")
    if not isinstance(streams, list) or not streams:
        return None
    first = streams[0]
    return first if isinstance(first, dict) else None


def _as_int(value: object) -> int:
    """Coerce an ffprobe field to an int, treating missing or unparseable values as 0."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _as_float(value: object) -> float | None:
    """Coerce an ffprobe field to a float, or None when it is missing or unparseable."""
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def probe_audio_stream(video_path: PathLike) -> AudioStreamInfo | None:
    """Return the first audio stream's header fields, or None when the file has no audio.

    Reads only the container header, so this is effectively free regardless of file size.
    """
    stream = _first_stream(
        _run_ffprobe(
            [
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_name,sample_rate,channels,duration",
                str(video_path),
            ]
        )
    )
    if stream is None:
        return None
    return AudioStreamInfo(
        codec=str(stream.get("codec_name") or ""),
        sample_rate=_as_int(stream.get("sample_rate")),
        channels=_as_int(stream.get("channels")),
        duration_seconds=_as_float(stream.get("duration")),
    )


def video_duration_seconds(video_path: PathLike) -> float | None:
    """Duration of the first video stream, or None when the container does not record one."""
    stream = _first_stream(
        _run_ffprobe(["-select_streams", "v:0", "-show_entries", "stream=duration", str(video_path)])
    )
    return _as_float(stream.get("duration")) if stream is not None else None


def survives_strict_segmentation(video_path: PathLike) -> bool:
    """Whether the file's audio survives a strict, timestamp-preserving remux to MPEG-TS.

    Segmenting video for streaming playback requires exactly this to succeed: timestamps are
    preserved rather than rewritten, ffmpeg's automatic timestamp fixups are disabled, and the
    first problem is fatal. A file that fails here will fail to segment. Nothing is decoded and
    nothing is kept, so the cost is a single sequential read.
    """
    result = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-xerror",
            "-copyts",
            "-i",
            str(video_path),
            "-map",
            "0:a:0",
            "-c",
            "copy",
            "-mpegts_copyts",
            "1",
            "-avoid_negative_ts",
            "disabled",
            "-f",
            "mpegts",
            "-y",
            os.devnull,
        ],  # fmt: skip
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _samples_per_packet(video_path: PathLike, codec: str) -> int | None:
    """Decode a brief window to learn how many samples one packet of this codec carries.

    Returns None when the codec has no fixed packet size, or when the sampled window shows no
    clearly dominant size — an unmeasurable track is reported as such rather than guessed at.
    """
    if codec not in _CONSTANT_FRAME_CODECS:
        return None
    response = _run_ffprobe(
        [
            "-select_streams",
            "a:0",
            "-read_intervals",
            f"%+{_SAMPLE_WINDOW_SECONDS}",
            "-show_entries",
            "frame=nb_samples",
            str(video_path),
        ]
    )
    frames = response.get("frames")
    if not isinstance(frames, list) or not frames:
        return None
    # The last packet of a sampled window is routinely short, so take the dominant size rather
    # than requiring unanimity — but refuse to guess when no size clearly dominates.
    counts = collections.Counter(
        _as_int(frame.get("nb_samples")) for frame in frames if isinstance(frame, dict) and frame.get("nb_samples")
    )
    if not counts:
        return None
    dominant, dominant_count = counts.most_common(1)[0]
    if dominant_count * 2 < len(frames):
        return None
    return dominant or None


def _read_audio_packets(video_path: PathLike) -> tuple[list[float], float]:
    """Return every audio packet's start time and the final packet's duration, both in seconds.

    ffprobe is asked for ``dts_time`` rather than raw ``dts`` deliberately. A packet timestamp is
    expressed in the stream's own time base, which is 1/sample_rate only for MP4 and MOV --
    Matroska uses milliseconds and MPEG-TS uses 90kHz. Letting ffprobe apply the time base keeps
    this container-agnostic instead of silently mis-scaling every non-MP4 input.
    """
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "packet=dts_time,duration_time",
            "-of",
            "csv=p=0",
            str(video_path),
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    )
    starts: list[float] = []
    final_duration = 0.0
    for line in result.stdout.splitlines():
        fields = line.strip().rstrip(",").split(",")
        if len(fields) < 2:
            continue
        start = _as_float(fields[0])
        if start is None:
            continue
        starts.append(start)
        final_duration = _as_float(fields[1]) or 0.0
    return starts, final_duration


def timeline_from_packets(
    info: AudioStreamInfo,
    packet_start_seconds: Sequence[float],
    final_packet_duration_seconds: float,
    samples_per_packet: int,
) -> AudioTimeline | None:
    """Measure holes and overlap from packet start times. Pure -- no subprocesses, no file access.

    Each packet carries ``samples_per_packet`` of sound. Where the next packet starts later than
    that, the clock covers time holding no sound (a hole); where it starts earlier, sound exists
    that the clock gives no time to play (overlap).

    Deviations smaller than a fraction of a packet are ignored. Containers store timestamps at
    their own resolution -- Matroska rounds to the millisecond, so a 21.33ms packet is recorded as
    21ms -- and accumulating that rounding across a long file would otherwise manufacture minutes
    of phantom defect. Real dropouts lose whole packets and real flush runs compress by nearly a
    whole packet, so both stay far above this threshold.
    """
    if not packet_start_seconds or info.sample_rate <= 0 or samples_per_packet <= 0:
        return None

    packet_seconds = samples_per_packet / info.sample_rate
    deadband = packet_seconds * _QUANTIZATION_DEADBAND_FRACTION

    hole_seconds = overlap_seconds = largest_hole = 0.0
    hole_count = overlap_count = 0
    for earlier, later in zip(packet_start_seconds, packet_start_seconds[1:]):
        deviation = (later - earlier) - packet_seconds
        if deviation > deadband:
            hole_seconds += deviation
            hole_count += 1
            largest_hole = max(largest_hole, deviation)
        elif deviation < -deadband:
            overlap_seconds += -deviation
            overlap_count += 1

    return AudioTimeline(
        codec=info.codec,
        sample_rate=info.sample_rate,
        channels=info.channels,
        packet_count=len(packet_start_seconds),
        clock_seconds=packet_start_seconds[-1] + final_packet_duration_seconds - packet_start_seconds[0],
        sound_seconds=len(packet_start_seconds) * packet_seconds,
        hole_seconds=hole_seconds,
        overlap_seconds=overlap_seconds,
        largest_hole_seconds=largest_hole,
        hole_count=hole_count,
        overlap_count=overlap_count,
    )


def measure_audio_timeline(video_path: PathLike) -> AudioTimeline | None:
    """Walk every audio packet and measure holes and overlap separately.

    Returns None when the file has no audio track, or when its codec has no fixed packet size and
    the comparison therefore cannot be made.
    """
    info = probe_audio_stream(video_path)
    if info is None:
        return None
    samples_per_packet = _samples_per_packet(video_path, info.codec)
    if samples_per_packet is None:
        return None
    starts, final_duration = _read_audio_packets(video_path)
    return timeline_from_packets(info, starts, final_duration, samples_per_packet)


def diagnose_audio(video_path: PathLike) -> AudioDiagnosis:
    """Measure a file's audio timeline and whether ingest will accept it as-is."""
    info = probe_audio_stream(video_path)
    if info is None:
        return AudioDiagnosis(defect=AudioDefect.NO_AUDIO_TRACK, segments_cleanly=True, timeline=None)

    timeline = measure_audio_timeline(video_path)
    return AudioDiagnosis(
        defect=timeline.defect if timeline is not None else AudioDefect.UNMEASURABLE,
        segments_cleanly=survives_strict_segmentation(video_path),
        timeline=timeline,
        stream=info,
        video_seconds=video_duration_seconds(video_path),
    )


def audio_repair_filter(
    video_path: PathLike,
    max_audio_hole_seconds: float = DEFAULT_MAX_AUDIO_HOLE_SECONDS,
) -> str | None:
    """Return the ffmpeg audio filter this file needs, or None to leave its audio untouched.

    Never raises and never refuses to convert. When a repair would be unsafe or unnecessary this
    logs what it found and returns None, leaving the conversion exactly as it would have been
    without any audio inspection at all.
    """
    diagnosis = diagnose_audio(video_path)
    timeline = diagnosis.timeline

    if diagnosis.defect is AudioDefect.NO_AUDIO_TRACK:
        return None

    if diagnosis.defect is AudioDefect.UNMEASURABLE:
        codec = diagnosis.stream.codec if diagnosis.stream else "unknown"
        logger.info(
            "Audio of '%s' uses codec '%s', whose packets have no fixed sample count, so its "
            "timeline cannot be measured. Leaving the audio untouched.",
            video_path,
            codec,
        )
        return None

    # Audio outlasting video means the streams disagree about how long the recording was. No
    # audio-only repair reconciles that, so report it and convert exactly as before.
    if (
        diagnosis.audio_seconds is not None
        and diagnosis.video_seconds is not None
        and diagnosis.audio_seconds - diagnosis.video_seconds > MAX_AUDIO_OVERRUN_SECONDS
    ):
        logger.warning(
            "Audio of '%s' runs %.2fs against %.2fs of video; the streams disagree about the "
            "length of the recording. Leaving the audio untouched -- inspect the source.",
            video_path,
            diagnosis.audio_seconds,
            diagnosis.video_seconds,
        )
        return None

    # A file ingest already accepts is never rewritten on our own initiative: rebuilding its audio
    # could only move data that is currently fine.
    if diagnosis.segments_cleanly:
        if timeline is not None and timeline.defect is not AudioDefect.NONE:
            logger.warning(
                "Leaving audio of '%s' unchanged: it is accepted as-is, but %s. That audio is "
                "missing from the recording itself, which no conversion can recover.",
                video_path,
                timeline.describe(),
            )
        else:
            logger.debug("Audio timeline of '%s' is coherent; leaving audio untouched", video_path)
        return None

    if timeline is not None and timeline.largest_hole_seconds > max_audio_hole_seconds:
        logger.warning(
            "Leaving audio of '%s' unchanged: it has a %.2fs gap, beyond the %.2fs limit. A gap "
            "that long is likelier a corrupt timestamp than real missing audio, and filling it "
            "would synthesize that much silence. Raise max_audio_hole_seconds to fill it anyway.",
            video_path,
            timeline.largest_hole_seconds,
            max_audio_hole_seconds,
        )
        return None

    finding = timeline.describe() if timeline is not None else "it is rejected by strict segmentation"
    logger.warning(
        "Repairing audio of '%s' with ffmpeg filter '%s': %s. The filter fills gaps with silence "
        "and drops audio that has no time to play, so the timeline the file declares is preserved "
        "exactly and no video or audio timestamp moves.",
        video_path,
        AUDIO_REPAIR_FILTER,
        finding,
    )
    return AUDIO_REPAIR_FILTER
