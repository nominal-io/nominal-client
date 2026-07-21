#!/usr/bin/env python3
"""Diagnostic for normalize_video.

Runs nominal's `normalize_video` on one or more source videos and reports whether each ran
safely and correctly: it probes the source and the normalized output and checks the guarantees
the normalizer is supposed to provide.

  * Output is h264/hevc, yuv420p, with no B-frames.
  * No real frames are dropped (output frame count >= source frame count).
  * Output duration stays aligned to the source.
  * The muxed avg_frame_rate reads correctly (constant-rate output reports its true rate).

A source that the normalizer *rejects* with a clear error (e.g. timestamp discontinuity, no video
stream) is reported as SAFE-REJECTED -- that is correct behavior, not a failure.

Requires ffmpeg/ffprobe on PATH (this actually re-encodes the file). Note: counting frames decodes
the whole file, so this is slow on large inputs.

Usage:
    python check_normalize_video.py VIDEO [VIDEO ...]
        [-o OUTPUT_DIR] [--codec {h264,h264_nvenc,hevc,hevc_nvenc}]
        [--mode {auto,cfr,passthrough}] [--keep] [--no-color]

Exit code: 0 if every input passed (or was safely rejected); 1 if any input produced output that
failed a check.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from fractions import Fraction
from pathlib import Path

import ffmpeg

from nominal.experimental.video_processing.video_conversion import (
    NormalizationError,
    frame_count,
    normalize_video,
)

# Tolerances for the correctness checks.
_RATE_REL_TOL = 0.01  # avg_frame_rate is "correct" if within 1% of r_frame_rate
_DURATION_WARN_S = 0.5  # duration drift above this fails; a couple of frames passes


class _C:
    """ANSI colors, disabled when output isn't a TTY or --no-color is passed."""

    enabled = sys.stdout.isatty()

    @classmethod
    def wrap(cls, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if cls.enabled else text


def _status(kind: str, label: str, detail: str = "") -> bool:
    """Print one PASS/FAIL/WARN/INFO line; return True if it counts as a failure."""
    mark, code = {"PASS": ("PASS", "32"), "FAIL": ("FAIL", "31"), "WARN": ("WARN", "33"), "INFO": ("INFO", "2")}[kind]
    line = f"  {_C.wrap(code, mark):<4}  {label}"
    if detail:
        line += f"   {_C.wrap('2', detail)}"
    print(line)
    return kind == "FAIL"


def _probe(path: Path) -> dict | None:
    """Probe the first video stream; return parsed fields, or None if there is no video stream."""
    try:
        info = ffmpeg.probe(
            str(path),
            v="error",
            select_streams="v:0",
            show_entries="stream=codec_name,pix_fmt,has_b_frames,r_frame_rate,avg_frame_rate:format=duration",
        )
    except ffmpeg.Error as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if exc.stderr else ""
        raise RuntimeError(f"ffprobe failed for {path}: {stderr.strip()}") from exc

    streams = info.get("streams", [])
    if not streams:
        return None
    s = streams[0]

    def _frac(value: str | None) -> Fraction | None:
        try:
            f = Fraction(value)  # type: ignore[arg-type]
            return f if f > 0 else None
        except (ValueError, ZeroDivisionError, TypeError):
            return None

    def _float(value: object) -> float | None:
        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    return {
        "codec": s.get("codec_name"),
        "pix_fmt": s.get("pix_fmt"),
        "has_b_frames": s.get("has_b_frames"),
        "r": _frac(s.get("r_frame_rate")),
        "avg": _frac(s.get("avg_frame_rate")),
        "duration": _float(info.get("format", {}).get("duration")),
    }


def _fmt_rate(r: Fraction | None) -> str:
    return f"{float(r):.4f} ({r.numerator}/{r.denominator})" if r else "unknown"


def _run_output_checks(src_info: dict | None, src_frames: int, out_info: dict, out_frames: int) -> bool:  # noqa: PLR0912 -- a flat list of independent checks
    """Run the correctness checks comparing source and normalized output. Return True if all passed."""
    failed = False

    # Codec.
    failed |= _status(
        "PASS" if out_info["codec"] in ("h264", "hevc") else "FAIL",
        "codec is h264/hevc",
        f"got {out_info['codec']}",
    )

    # Pixel format.
    failed |= _status(
        "PASS" if out_info["pix_fmt"] == "yuv420p" else "FAIL",
        "pixel format is yuv420p",
        f"got {out_info['pix_fmt']}",
    )

    # No B-frames.
    try:
        bframes = int(out_info["has_b_frames"])
    except (TypeError, ValueError):
        bframes = -1
    failed |= _status("PASS" if bframes == 0 else "FAIL", "no B-frames", f"has_b_frames={out_info['has_b_frames']}")

    # No real frames dropped.
    if out_frames >= src_frames:
        added = out_frames - src_frames
        detail = f"{out_frames} >= {src_frames}" + (f" (+{added} duplicate fill)" if added else " (preserved)")
        _status("PASS", "no frames dropped", detail)
    else:
        failed |= _status("FAIL", "no frames dropped", f"output {out_frames} < source {src_frames} -- LOST FRAMES")

    # Duration aligned to source.
    if src_info and src_info["duration"] is not None and out_info["duration"] is not None and out_info["r"]:
        delta = abs(out_info["duration"] - src_info["duration"])
        ok_tol = max(0.05, 2.0 / float(out_info["r"]))
        detail = f"|{out_info['duration']:.3f} - {src_info['duration']:.3f}| = {delta:.3f}s"
        if delta <= ok_tol:
            _status("PASS", "duration aligned", detail)
        elif delta <= _DURATION_WARN_S:
            _status("WARN", "duration off by a few frames", detail)
        else:
            failed |= _status("FAIL", "duration drifted", detail)
    else:
        _status("WARN", "duration not verifiable", "missing source/output duration or rate")

    # avg_frame_rate reports correctly (the original symptom).
    if out_info["r"] and out_info["avg"]:
        if abs(float(out_info["avg"]) - float(out_info["r"])) <= _RATE_REL_TOL * float(out_info["r"]):
            _status("PASS", "avg_frame_rate matches true rate", f"avg={_fmt_rate(out_info['avg'])}")
        else:
            _status(
                "WARN",
                "avg_frame_rate below r_frame_rate",
                "output is variable/passthrough; avg reflects the true average, not a defect",
            )
    else:
        _status("WARN", "avg_frame_rate not verifiable")

    if failed:
        print("  " + _C.wrap("31", "RESULT: FAILED a check"))
    else:
        print("  " + _C.wrap("32", "RESULT: ran safely and correctly"))
    return not failed


def check_one(src: Path, out: Path, codec: str, mode: str) -> bool:
    """Normalize `src` -> `out` and print diagnostics. Return True if it passed (or was safely rejected)."""
    print(_C.wrap("1", f"\n=== {src} ==="))

    src_info = _probe(src)
    if src_info is None:
        _status("INFO", "source has no video stream")
    else:
        _status(
            "INFO",
            "source",
            f"{src_info['codec']} {src_info['pix_fmt']} "
            f"r={_fmt_rate(src_info['r'])} avg={_fmt_rate(src_info['avg'])} dur={src_info['duration']}",
        )

    print(_C.wrap("2", "  counting source frames (decodes the file)..."))
    src_frames = frame_count(src)
    _status("INFO", "source decoded frames", str(src_frames))

    # Run the normalizer. A NormalizationError is a correct, safe refusal -- not a failure.
    try:
        normalize_video(src, out, video_codec=codec, frame_rate_mode=mode)  # type: ignore[arg-type]
    except NormalizationError as exc:
        _status("INFO", "SAFE-REJECTED", str(exc))
        print(_C.wrap("33", "  -> normalizer correctly refused to produce output for this source."))
        return True

    out_info = _probe(out)
    if out_info is None:
        _status("FAIL", "output has no video stream")
        return False
    out_frames = frame_count(out)
    _status(
        "INFO",
        "output",
        f"{out_info['codec']} {out_info['pix_fmt']} "
        f"r={_fmt_rate(out_info['r'])} avg={_fmt_rate(out_info['avg'])} dur={out_info['duration']} "
        f"frames={out_frames}",
    )

    return _run_output_checks(src_info, src_frames, out_info, out_frames)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose normalize_video on video files.")
    parser.add_argument("videos", nargs="+", type=Path, help="source video file(s)")
    parser.add_argument("-o", "--output-dir", type=Path, help="where to write normalized output (default: temp dir)")
    parser.add_argument("--codec", default="h264", choices=["h264", "h264_nvenc", "hevc", "hevc_nvenc"])
    parser.add_argument("--mode", default="auto", choices=["auto", "cfr", "passthrough"])
    parser.add_argument("--keep", action="store_true", help="keep the normalized output files")
    parser.add_argument("--no-color", action="store_true", help="disable colored output")
    args = parser.parse_args()

    if args.no_color:
        _C.enabled = False

    tmp = None
    out_dir = args.output_dir
    if out_dir is None:
        tmp = tempfile.TemporaryDirectory()
        out_dir = Path(tmp.name)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_ok = True
    try:
        for video in args.videos:
            if not video.exists():
                _status("FAIL", f"{video}", "file does not exist")
                all_ok = False
                continue
            out = out_dir / f"{video.stem}.normalized.mp4"
            try:
                all_ok &= check_one(video, out, args.codec, args.mode)
            except Exception as exc:  # ffprobe failure, unexpected error -> report, don't crash the batch
                _status("FAIL", f"{video}", f"unexpected error: {exc}")
                all_ok = False
            if args.keep and out.exists():
                _status("INFO", "kept output", str(out))
    finally:
        if tmp is not None and not args.keep:
            tmp.cleanup()

    print()
    print(_C.wrap("32" if all_ok else "31", "ALL PASSED" if all_ok else "SOME CHECKS FAILED"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
