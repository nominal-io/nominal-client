from __future__ import annotations

import math
import pathlib
import re
import shutil
from fractions import Fraction

import pytest

from nominal.experimental.video_processing import (
    AnyResolutionType,
    VideoResolution,
    normalize_video,
    scale_factor_from_resolution,
)

# Source is (width, height, sar); a None sar is undefined. Expected is the output (width, height).
_GEOMETRY_CASES = [
    pytest.param((1920, 1080, "1"), "480p", (640, 360), id="16x9-source-to-480p"),
    pytest.param(
        (1920, 1080, "1"), VideoResolution(resolution_width=640, resolution_height=480), (640, 360), id="explicit-480p"
    ),
    pytest.param((720, 480, "32/27"), "480p", (640, 360), id="anamorphic-to-480p"),
    pytest.param((1920, 1080, None), "480p", (640, 360), id="undefined-sar-to-480p"),
    pytest.param((641, 361, "1"), "480p", (640, 360), id="odd-source-to-480p"),
    pytest.param((320, 240, "1"), "480p", (320, 240), id="small-source-not-upscaled"),
    pytest.param(
        (320, 240, "1"),
        VideoResolution(resolution_width=640, resolution_height=480, allow_upscaling=True),
        (640, 480),
        id="small-source-upscaled",
    ),
    pytest.param((1080, 1920, "1"), "480p", (270, 480), id="portrait-to-480p"),
    pytest.param((1920, 1080, "1"), VideoResolution(resolution_height=720), (1280, 720), id="height-only"),
    pytest.param((1920, 1080, "1"), VideoResolution(resolution_width=1280), (1280, 720), id="width-only"),
    pytest.param((1920, 1080, "1"), VideoResolution(allow_upscaling=True), (1920, 1080), id="no-bounds-is-identity"),
    # 1232 * (640 / 1232) == 639.9999999999999 in doubles.
    pytest.param((1232, 693, "1"), "480p", (640, 360), id="float-rounding-does-not-undershoot-bound"),
    pytest.param((1920, 1080, "1"), VideoResolution(resolution_width=4), (4, 2), id="minimum-height"),
    pytest.param((1080, 1920, "1"), VideoResolution(resolution_height=4), (2, 4), id="minimum-width"),
    pytest.param((320, 320, "1"), VideoResolution(resolution_width=2, resolution_height=2), (2, 2), id="two-pixel"),
    # Sides that would round to 0 are clamped to 2, since ffmpeg reads 0 as "keep the source size".
    pytest.param((1920, 1080, "1"), VideoResolution(resolution_width=2), (2, 2), id="clamp-tiny-width"),
    pytest.param((1080, 1920, "1"), VideoResolution(resolution_height=2), (2, 2), id="clamp-tiny-height"),
    pytest.param((720, 480, "32/27"), VideoResolution(resolution_width=2), (2, 2), id="clamp-anamorphic"),
    pytest.param((2000, 2, "1"), "480p", (640, 2), id="clamp-extreme-landscape"),
    pytest.param((2, 2000, "1"), "480p", (2, 480), id="clamp-extreme-portrait"),
]

_FILTER_PATTERN = re.compile(r"^scale='(?P<width>[^']+)':'(?P<height>[^']+)',setsar=1/1$")


def _source_sar(sar: str | None) -> Fraction:
    return Fraction(1) if sar is None else Fraction(sar)


def _evaluate_filter(resolution: AnyResolutionType, source: tuple[int, int, str | None]) -> tuple[int, int]:
    """Evaluate the generated ffmpeg expressions in Python (only arithmetic, min, max and trunc, in doubles)."""
    width, height, sar = source
    match = _FILTER_PATTERN.match(scale_factor_from_resolution(resolution))
    assert match is not None, "filter must be scale=<quoted width expr>:<quoted height expr>,setsar=1/1"
    names = {
        "iw": float(width),
        "ih": float(height),
        "sar": float(_source_sar(sar)),
        "min": min,
        "max": max,
        "trunc": math.trunc,
    }
    out_width = eval(match["width"], {"__builtins__": {}}, names)
    out_height = eval(match["height"], {"__builtins__": {}}, names)
    return int(out_width), int(out_height)


@pytest.mark.parametrize(("source", "resolution", "expected"), _GEOMETRY_CASES)
def test_scale_filter_fits_within_bounds_uniformly(
    source: tuple[int, int, str | None], resolution: AnyResolutionType, expected: tuple[int, int]
) -> None:
    """The generated filter scales uniformly to fit within the requested bounds, with even dimensions."""
    assert _evaluate_filter(resolution, source) == expected


@pytest.mark.parametrize("kwargs", [{"resolution_width": 641}, {"resolution_height": 0}, {"resolution_height": -2}])
def test_invalid_resolution_rejected(kwargs: dict[str, int]) -> None:
    """Odd or non-positive bounds are rejected at construction."""
    with pytest.raises(ValueError):
        VideoResolution(**kwargs)


_requires_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None, reason="ffmpeg not installed"
)


def _make_source(path: pathlib.Path, width: int, height: int, sar: str | None = "1") -> None:
    import ffmpeg

    stream = ffmpeg.input(f"testsrc=size={width}x{height}:rate=30:duration=0.5", f="lavfi")
    stream = stream.filter("setsar", "0" if sar is None else sar)
    # yuv444p permits odd dimensions, letting us build inputs that yuv420p h264 would reject.
    stream.output(str(path), vcodec="libx264", pix_fmt="yuv444p").overwrite_output().run(quiet=True)


def _probe_geometry(path: pathlib.Path) -> tuple[int, int, str, str]:
    import ffmpeg

    stream = ffmpeg.probe(
        str(path),
        v="error",
        select_streams="v:0",
        show_entries="stream=width,height,sample_aspect_ratio,display_aspect_ratio",
    )["streams"][0]
    return (
        stream["width"],
        stream["height"],
        stream.get("sample_aspect_ratio", "N/A"),
        stream.get("display_aspect_ratio", "N/A"),
    )


@_requires_ffmpeg
@pytest.mark.parametrize(("source", "resolution", "expected"), _GEOMETRY_CASES)
def test_normalize_video_geometry(
    tmp_path: pathlib.Path,
    source: tuple[int, int, str | None],
    resolution: AnyResolutionType,
    expected: tuple[int, int],
) -> None:
    """normalize_video produces the expected dimensions with square pixels and the source display aspect ratio."""
    src = tmp_path / "src.mp4"
    out = tmp_path / "out.mp4"
    _make_source(src, *source)

    normalize_video(src, out, resolution=resolution)

    out_width, out_height = expected
    display_aspect = Fraction(out_width, out_height)
    assert _probe_geometry(out) == (
        out_width,
        out_height,
        "1:1",
        f"{display_aspect.numerator}:{display_aspect.denominator}",
    )


@_requires_ffmpeg
def test_normalize_video_without_resolution_keeps_source_geometry(tmp_path: pathlib.Path) -> None:
    """Without a resolution, the source dimensions and SAR are passed through unchanged."""
    src = tmp_path / "src.mp4"
    out = tmp_path / "out.mp4"
    _make_source(src, 720, 480, "32/27")

    normalize_video(src, out)

    assert _probe_geometry(out) == (720, 480, "32:27", "16:9")


@_requires_ffmpeg
def test_normalize_video_native_odd_dimensions_fail(tmp_path: pathlib.Path) -> None:
    """Without a resolution, odd-sized sources fail rather than being silently cropped."""
    src = tmp_path / "src.mp4"
    _make_source(src, 641, 361)

    # Without an explicit resolution, don't silently crop customer data to make encoding succeed. The exception type
    # is ffmpeg's and not part of the contract.
    with pytest.raises(Exception):
        normalize_video(src, tmp_path / "out.mp4")
