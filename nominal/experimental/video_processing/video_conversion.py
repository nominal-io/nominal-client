from __future__ import annotations

import logging
import pathlib
import shlex
from collections.abc import Callable  # noqa: F401
from enum import Enum, auto
from fractions import Fraction
from typing import Any, Literal

import ffmpeg

from nominal.core._types import PathLike
from nominal.experimental.video_processing.resolution import (
    AnyResolutionType,
    scale_factor_from_resolution,
)

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    """Raised when a video cannot be cleanly normalized without violating frame/duration invariants."""


class TimingStrategy(Enum):
    """How the output's frame timing is produced."""

    CFR_HEAL = auto()  # re-grid at the true rate; duplicate-fill dropped frames; clamp duration
    PASSTHROUGH = auto()  # preserve the source's own frame timing exactly


FrameRateMode = Literal["auto", "cfr", "passthrough"]

# Tier-1: |avg-r|/r at or below this is treated as a clean constant-rate source (skip timestamp analysis).
_CLEAN_RATE_TOLERANCE = 0.005
# Tier-2: a PTS delta counts as "on-grid" if delta/(1/r) is within this of a positive integer.
_GRID_DELTA_TOLERANCE = 0.25
# Tier-2: at/above this fraction of on-grid deltas the source is a (possibly holey) constant-rate grid.
_GRID_MATCH_MIN_FRACTION = 0.98
# Verification: allowed output-vs-source duration drift, in seconds, before it is an error (CFR) or warning.
_DURATION_TOLERANCE_SECONDS = 0.25


def _parse_rate(rate: str | None) -> Fraction | None:
    """Parse an ffprobe rate string ('num/den' or 'num') into a positive Fraction, or None.

    Returns None for missing, zero ('0/0'), non-numeric, or non-positive values so callers can treat
    'no usable rate' uniformly.
    """
    if not rate:
        return None
    try:
        value = Fraction(rate)
    except (ValueError, ZeroDivisionError):
        return None
    return value if value > 0 else None


def _parse_timing_probe(
    probe: dict[str, Any], video_path: pathlib.Path
) -> tuple[Fraction | None, Fraction | None, float | None]:
    """Parse (r_frame_rate, avg_frame_rate, container_duration_seconds) from an ffprobe result dict.

    Pure (no I/O) so it can be unit-tested with canned probe dicts. Raises NormalizationError when the
    result has no video stream. ``video_path`` is used only for the error message.
    """
    streams = probe.get("streams", [])
    if not streams:
        raise NormalizationError(f"Cannot normalize '{video_path}': no video stream found")

    stream = streams[0]
    r = _parse_rate(stream.get("r_frame_rate"))
    avg = _parse_rate(stream.get("avg_frame_rate"))

    duration_raw = probe.get("format", {}).get("duration")
    try:
        duration: float | None = float(duration_raw)
    except (TypeError, ValueError):
        duration = None

    return r, avg, duration


def _parse_packet_pts(probe: dict[str, Any]) -> list[float]:
    """Extract packet presentation timestamps (seconds, file order) from an ffprobe result dict.

    Pure (no I/O) so it can be unit-tested with canned probe dicts. Skips unparseable entries; file
    order is preserved so the discontinuity check can see backward jumps.
    """
    times: list[float] = []
    for packet in probe.get("packets", []):
        try:
            times.append(float(packet.get("pts_time")))
        except (TypeError, ValueError):
            continue
    return times


def _classify_tier1(
    r: Fraction | None,
    avg: Fraction | None,
    tolerance: float = _CLEAN_RATE_TOLERANCE,
) -> TimingStrategy | None:
    """Cheap first-pass classification from r_frame_rate vs avg_frame_rate.

    Returns PASSTHROUGH when the rate is indeterminate (preserve timing), CFR_HEAL when avg≈r
    (clean constant-rate grid), or None when the gap is large in either direction -- which is
    ambiguous (dropped frames, true VFR, or misdetected r) and must be resolved by Tier 2.
    """
    if r is None:
        return TimingStrategy.PASSTHROUGH
    if avg is None:
        return None
    if abs(float(avg) - float(r)) <= tolerance * float(r):
        return TimingStrategy.CFR_HEAL
    return None


def _has_timestamp_discontinuity(pts: list[float], r: Fraction) -> bool:
    """Return True if the timestamps (in file order) contain a large backward jump.

    A PTS reset or 33-bit wrap shows up as a big backward step; small backward steps from normal
    B-frame reordering are ignored. CFR cannot be safely imposed across a discontinuity, so callers
    treat True as a hard error.
    """
    base = 1.0 / float(r)
    threshold = max(1.0, 10.0 * base)
    for prev, cur in zip(pts, pts[1:]):
        if cur - prev < -threshold:
            return True
    return False


def _deltas_match_grid(
    pts: list[float],
    r: Fraction,
    tolerance: float = _GRID_DELTA_TOLERANCE,
    min_fraction: float = _GRID_MATCH_MIN_FRACTION,
) -> bool:
    """Return True if the inter-frame deltas look like a constant-rate grid (possibly with holes).

    Sorts the timestamps into presentation order, then checks what fraction of consecutive deltas are
    positive-integer multiples of the base interval 1/r. A high fraction means a clean grid or a grid
    with dropped frames (heal with CFR); a low fraction means genuine variable frame rate (pass through).
    """
    ordered = sorted(pts)
    base = 1.0 / float(r)
    deltas = [cur - prev for prev, cur in zip(ordered, ordered[1:])]
    if not deltas:
        return False
    on_grid = 0
    for delta in deltas:
        if delta <= 0:
            continue
        multiple = delta / base
        if multiple >= 0.5 and abs(multiple - round(multiple)) <= tolerance:
            on_grid += 1
    return on_grid / len(deltas) >= min_fraction


def _select_strategy(
    frame_rate_mode: FrameRateMode,
    r: Fraction | None,
    avg: Fraction | None,
    sample_pts: Callable[[], list[float]],
    video_path: pathlib.Path,
) -> tuple[TimingStrategy, float | None]:
    """Pick the timing strategy and the max observed PTS (used for the CFR clamp guard).

    `sample_pts` is invoked lazily, only when Tier 1 is ambiguous, so clean and forced sources never pay
    the timestamp-probe cost. Raises NormalizationError on a timestamp discontinuity, or when CFR is
    forced on a source whose frame rate is indeterminate.
    """
    if frame_rate_mode == "passthrough":
        return TimingStrategy.PASSTHROUGH, None
    if frame_rate_mode == "cfr":
        if r is None:
            raise NormalizationError(f"Cannot force CFR on '{video_path}': frame rate is indeterminate")
        return TimingStrategy.CFR_HEAL, None

    tier1 = _classify_tier1(r, avg)
    if tier1 is not None:
        return tier1, None

    if r is None:  # _classify_tier1 only returns None when r is known; defense-in-depth
        raise NormalizationError(f"Cannot classify '{video_path}': frame rate is indeterminate")
    pts = sample_pts()
    if not pts:
        logger.warning("Could not sample timestamps for '%s'; preserving original timing", video_path)
        return TimingStrategy.PASSTHROUGH, None
    if _has_timestamp_discontinuity(pts, r):
        raise NormalizationError(
            f"Cannot normalize '{video_path}': timestamp discontinuity/reset detected; "
            "the source timeline is incoherent and cannot be cleanly normalized"
        )
    if _deltas_match_grid(pts, r):
        return TimingStrategy.CFR_HEAL, max(pts)
    logger.warning("Source '%s' is variable-frame-rate; preserving original timing", video_path)
    return TimingStrategy.PASSTHROUGH, max(pts)


def _timing_output_kwargs(
    strategy: TimingStrategy,
    r: Fraction | None,
    duration: float | None,
    max_observed_pts: float | None,
) -> dict[str, str]:
    """Build the timing-related ffmpeg output kwargs for the chosen strategy.

    PASSTHROUGH preserves the source's own frame timing. CFR_HEAL stamps a constant grid at the true
    rate (numerator timescale keeps per-frame timestamps exact for any rate) and clamps the duration so
    gap-fill can't overshoot. The clamp is skipped when the duration is unknown or is contradicted by a
    later sampled timestamp (which would mean -t truncates real frames).
    """
    if strategy is TimingStrategy.PASSTHROUGH:
        return {"fps_mode": "passthrough"}

    if r is None:
        raise NormalizationError("CFR-heal requires a known frame rate")

    kwargs = {
        "r": f"{r.numerator}/{r.denominator}",
        "fps_mode": "cfr",
        "video_track_timescale": str(r.numerator),
    }
    if duration is not None and duration > 0 and (max_observed_pts is None or duration >= max_observed_pts):
        kwargs["t"] = f"{duration:.6f}"
    return kwargs


def _clean_encode_kwargs(video_codec: VideoCodec) -> dict[str, str]:
    """Codec/colorspace kwargs applied to every source: selected codec, AAC audio, yuv420p, no B-frames.

    `-bf 0` disables B-frames for both the libx264/libx265 CPU encoders and the NVENC encoders.
    """
    return {
        "vcodec": video_codec,
        "acodec": DEFAULT_AUDIO_CODEC,
        "pix_fmt": DEFAULT_PIXEL_FORMAT,
        "bf": "0",
    }


def _build_output_kwargs(
    strategy: TimingStrategy,
    r: Fraction | None,
    source_duration: float | None,
    max_observed_pts: float | None,
    video_codec: VideoCodec,
    key_frame_interval: int | None,
    resolution: AnyResolutionType | None,
) -> dict[str, str | None]:
    """Assemble the full ffmpeg output kwargs: clean-encode + timing strategy + keyframes + optional scale.

    Pure (no I/O) so the exact ffmpeg arguments can be asserted directly in tests.
    """
    kwargs: dict[str, str | None] = {
        **_clean_encode_kwargs(video_codec),
        **_timing_output_kwargs(strategy, r, source_duration, max_observed_pts),
    }
    if key_frame_interval is None:
        kwargs["force_key_frames"] = "source"
    else:
        kwargs["force_key_frames"] = f"expr:gte(t,n_forced*{key_frame_interval})"
    if resolution is not None:
        kwargs["vf"] = scale_factor_from_resolution(resolution)
    return kwargs


def _verify_invariants(
    strategy: TimingStrategy,
    r: Fraction | None,
    source_duration: float | None,
    source_frames: int | None,
    output_frames: int,
    output_duration: float | None,
    video_path: pathlib.Path,
) -> None:
    """Enforce the normalization invariants on the produced output; raise NormalizationError if violated.

    CFR_HEAL: the output frame count must match the source's intended count (duration x rate) within one
    frame, and the duration must match the source within tolerance. PASSTHROUGH: the output must keep at
    least as many frames as the source decoded; duration shortfalls only warn (trailing source drops are
    a faithfully-reproduced source property).
    """
    if output_frames <= 0:
        raise NormalizationError(f"Normalization of '{video_path}' produced no frames")

    if strategy is TimingStrategy.CFR_HEAL:
        if r is not None and source_duration is not None and source_duration > 0:
            expected = round(source_duration * float(r))
            if abs(output_frames - expected) > 1:
                raise NormalizationError(
                    f"Normalization of '{video_path}' is invalid: expected ~{expected} frames "
                    f"({source_duration:.3f}s x {float(r):.5f} fps), got {output_frames}"
                )
            if output_duration is not None and abs(output_duration - source_duration) > _DURATION_TOLERANCE_SECONDS:
                raise NormalizationError(
                    f"Normalization of '{video_path}' is invalid: output duration {output_duration:.3f}s "
                    f"differs from source {source_duration:.3f}s by more than {_DURATION_TOLERANCE_SECONDS}s"
                )
        else:
            logger.warning("Could not verify CFR-heal invariants for '%s' (rate/duration unknown)", video_path)
    else:  # PASSTHROUGH
        if source_frames is not None and output_frames < source_frames:
            raise NormalizationError(
                f"Normalization of '{video_path}' dropped frames: source {source_frames}, output {output_frames}"
            )
        if (
            source_duration is not None
            and output_duration is not None
            and abs(output_duration - source_duration) > _DURATION_TOLERANCE_SECONDS
        ):
            logger.warning(
                "Passthrough output for '%s' duration %.3fs differs from source %.3fs (likely trailing source drops)",
                video_path,
                output_duration,
                source_duration,
            )


# The video codec options exposed to callers. These names are passed directly to ffmpeg as the
# -c:v value and ffmpeg resolves each to its default encoder for that codec:
#   - "h264":        H.264 (CPU, resolves to libx264). Portable, no GPU required, slower.
#   - "h264_nvenc":  GPU-accelerated H.264 via NVIDIA NVENC. Requires an NVIDIA GPU; much faster.
#   - "hevc":        H.265/HEVC (CPU, resolves to libx265). Better compression, slower, less universally played.
#   - "hevc_nvenc":  GPU-accelerated H.265/HEVC via NVIDIA NVENC. Requires an NVIDIA GPU; better
#                    compression than h264_nvenc at similar speed.
VideoCodec = Literal["h264", "h264_nvenc", "hevc", "hevc_nvenc"]

DEFAULT_VIDEO_CODEC: VideoCodec = "h264"
DEFAULT_AUDIO_CODEC = "aac"
DEFAULT_PIXEL_FORMAT = "yuv420p"
DEFAULT_KEY_FRAME_INTERVAL_SEC = 2


def normalize_video(
    input_path: PathLike,
    output_path: PathLike,
    key_frame_interval: int | None = DEFAULT_KEY_FRAME_INTERVAL_SEC,
    force: bool = True,
    resolution: AnyResolutionType | None = None,
    num_threads: int | None = None,
    video_codec: VideoCodec = DEFAULT_VIDEO_CODEC,
    frame_rate_mode: FrameRateMode = "auto",
) -> None:
    """Re-encode ("normalize") a video into a clean form best supported by Nominal.

    Always produces the selected codec, yuv420p, no B-frames, and keyframes at the requested interval.
    The frame timing is chosen adaptively so the output never drops real frames and its duration stays
    aligned to the source:

      * Clean constant-rate sources are re-stamped at their true rate (identity for frame count).
      * Constant-rate sources with dropped frames are healed: re-gridded at the true rate with the
        previous frame duplicated to fill each gap, and the duration clamped so the fill can't overshoot.
      * Variable-frame-rate (and indeterminate-rate) sources are passed through, preserving their exact
        timing and frame count.
      * Sources with a timestamp discontinuity/reset, no video stream, or that fail to probe/encode raise
        NormalizationError. A post-encode check re-verifies the invariants and raises rather than emit a
        file that lost frames or drifted in duration.

    ffmpeg (>=5.1, built for the selected codec) must be installed locally.

    Args:
        input_path: Path to the source video.
        output_path: Path to write the output (.mkv or .mp4).
        key_frame_interval: Seconds between forced keyframes; None keeps source keyframes.
        force: If True, overwrite an existing output path.
        resolution: If provided, rescale to this resolution.
        num_threads: If provided, the number of CPU cores ffmpeg may use.
        video_codec: Encoder, one of h264 / h264_nvenc / hevc / hevc_nvenc (default h264).
        frame_rate_mode: "auto" (default) classifies the source and picks a strategy; "cfr" forces
            constant-rate healing; "passthrough" forces timing preservation.
    """
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)
    if not input_path.exists():
        raise NormalizationError(f"Input path does not exist: {input_path}")
    if output_path.suffix.lower() not in (".mkv", ".mp4"):
        raise NormalizationError(f"Output must be .mkv or .mp4, got: {output_path}")
    if output_path.exists():
        if force:
            logger.info("Output file %s already exists! Deleting...", output_path)
            output_path.unlink()
        else:
            raise NormalizationError(f"Output path already exists: {output_path}")

    r, avg, source_duration = _probe_timing(input_path)
    strategy, max_observed_pts = _select_strategy(
        frame_rate_mode, r, avg, lambda: _sample_frame_pts(input_path), input_path
    )
    output_kwargs = _build_output_kwargs(
        strategy, r, source_duration, max_observed_pts, video_codec, key_frame_interval, resolution
    )

    input_kwargs: dict[str, str] = {}
    if num_threads is not None:
        input_kwargs["threads"] = str(num_threads)

    video_in = ffmpeg.input(str(input_path), **input_kwargs)
    video_out = video_in.output(str(output_path), **output_kwargs)
    logger.info("Running command: '%s'", shlex.join(video_out.compile()))
    try:
        video_out.run(capture_stderr=True)
    except ffmpeg.Error as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if exc.stderr else ""
        raise NormalizationError(f"ffmpeg failed to normalize '{input_path}'. {stderr.strip()}") from exc

    output_frames = frame_count(output_path)
    _, _, output_duration = _probe_timing(output_path)
    source_frames = frame_count(input_path) if strategy is TimingStrategy.PASSTHROUGH else None
    _verify_invariants(strategy, r, source_duration, source_frames, output_frames, output_duration, input_path)


def _probe_timing(video_path: pathlib.Path) -> tuple[Fraction | None, Fraction | None, float | None]:
    """Probe (r_frame_rate, avg_frame_rate, container_duration_seconds) for the first video stream.

    Raises NormalizationError if the file can't be probed or has no video stream.
    """
    try:
        probe: dict[str, Any] = ffmpeg.probe(
            video_path,
            v="error",
            select_streams="v:0",
            show_entries="stream=r_frame_rate,avg_frame_rate:format=duration",
        )
    except ffmpeg.Error as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if exc.stderr else ""
        raise NormalizationError(f"Cannot normalize '{video_path}': ffprobe failed. {stderr.strip()}") from exc
    return _parse_timing_probe(probe, video_path)


def _sample_frame_pts(video_path: pathlib.Path) -> list[float]:
    """Return the video stream's packet presentation timestamps (seconds), in file order, no decode.

    File order is preserved so the discontinuity check can see backward jumps; the grid check sorts
    internally. Returns [] if no parseable timestamps are present.
    """
    try:
        probe: dict[str, Any] = ffmpeg.probe(
            video_path, v="error", select_streams="v:0", show_entries="packet=pts_time"
        )
    except ffmpeg.Error as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if exc.stderr else ""
        raise NormalizationError(
            f"Cannot sample timestamps for '{video_path}': ffprobe failed. {stderr.strip()}"
        ) from exc
    return _parse_packet_pts(probe)


def frame_count(video_path: pathlib.Path) -> int:
    """Given a path to a video file, return the number of frames present in the video.

    NOTE: if no streams are present, returns 0. If multiple streams are present, returns frame count
          of the first video stream.
    """
    assert video_path.exists()
    # Count decoded frames (nb_read_frames), not packets. A packet is not always one frame, so
    # counting packets can both over- and under-report the true frame count, which would make the
    # "differing frames" warning below unreliable -- exactly the integrity check we care about here.
    probe_resp = ffmpeg.probe(
        video_path, v="error", select_streams="v:0", count_frames=None, show_entries="stream=nb_read_frames"
    )

    # No video streams present
    if len(probe_resp["streams"]) == 0:
        return 0

    return int(probe_resp["streams"][0]["nb_read_frames"])


def has_audio_track(video_path: pathlib.Path) -> bool:
    """Given a path to a video file, return whether or not there is an audio track present."""
    assert video_path.exists()
    probe_output = ffmpeg.probe(
        video_path,
        show_streams=None,
        select_streams="a",
        loglevel="error",
    )
    return len(probe_output["streams"]) > 0
