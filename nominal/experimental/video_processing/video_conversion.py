from __future__ import annotations

import logging
import pathlib
import shlex

import ffmpeg

from nominal.core._types import PathLike
from nominal.experimental.video_processing.audio_timeline import (
    DEFAULT_MAX_AUDIO_HOLE_SECONDS,
    AudioTimelineError,
    audio_repair_filter,
    measure_audio_timeline,
    probe_audio_stream,
    survives_strict_segmentation,
)
from nominal.experimental.video_processing.resolution import (
    AnyResolutionType,
    scale_factor_from_resolution,
)

logger = logging.getLogger(__name__)

DEFAULT_VIDEO_CODEC = "h264"
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
    repair_audio: bool = True,
    max_audio_hole_seconds: float = DEFAULT_MAX_AUDIO_HOLE_SECONDS,
) -> None:
    """Convert video file to an h264 encoded video file using ffmpeg.

    This function will also perform several other processing tasks to ensure that video is
    properly encoded in a way that is best supported by nominal.
    This includes:
        * Ensuring that there are key-frames (I-frames) present approximately every 2s of video content
        * Video is encoded with H264
        * Audio is encoded with AAC
        * Video has YUV4:2:0 planar color space
        * Audio content is made to match the timeline the file declares, when the two disagree

    Timestamps are never moved. Video timestamps are passed through untouched, and audio packets
    keep the timestamps the source gave them; only the audio *content* is adjusted, by filling
    silence where the file says sound is missing and dropping samples the file gives no time to
    play. When the audio is already coherent no audio filter is applied at all, so such files are
    converted exactly as they were before this check existed.

    While this package includes bindings to use ffmpeg installed on your local system, it does not
    include ffmpeg as a dependency due to the GPLv3 licensing present in the standard H264 processing library
    contained within, thus, you must have ffmpeg installed locally to use this.

    Args:
        input_path: Path to video file on local filesystem.
        output_path: Path to write converted video file to.
            NOTE: it is expected that the output file is either an mkv or a mp4 file.
        key_frame_interval: Number of seconds between keyframes allowed in the output video.
            NOTE: While this field is technically optional, setting the right value here
                  can be essential to allowing fluid playback on the frontend, in particular,
                  in network constrained environments. Setting this value too low or too high
                  can impact performance negatively-- typically, a value at or around 2s is considered
                  "best of both worlds" as a reasonable default value.
        force: If true, forcibly delete existing output path if already exists.
        resolution: If provided, re-scale the video to the provided resolution
        num_threads: If provided, the number of CPU cores to tell ffmpeg to use.
            NOTE: If not provided, ffmpeg will choose. Typically, this amounts to the number of cores present
                  on the machine
        repair_audio: If true, inspect the audio timeline, rebuild the audio onto it when the two
            disagree, and verify the converted file can be segmented before returning.
        max_audio_hole_seconds: Longest single run of missing audio that will be filled with
            silence. A gap larger than this is treated as a corrupt timestamp rather than a real
            dropout and is left alone, rather than synthesizing that much silence from one bad
            number. Raise it to fill such a gap anyway.

    Raises:
        AudioTimelineError: If the converted file still cannot be segmented. Reported here rather
            than after a long upload that would be rejected on arrival.

    NOTE: this requires that you have installed ffmpeg on your system with support for H264.
    """
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)
    assert input_path.exists(), "Input path must exist"
    assert output_path.suffix.lower() in (".mkv", ".mp4")

    if output_path.exists():
        if force:
            logger.info(f"Output file {output_path} already exists! Deleting...")
            output_path.unlink()
        else:
            raise FileExistsError(f"Cannot convert {input_path} to {output_path}: output path already exists!")

    output_kwargs: dict[str, str | None] = dict(
        acodec=DEFAULT_AUDIO_CODEC,
        vcodec=DEFAULT_VIDEO_CODEC,
        force_key_frames="source",
        pix_fmt=DEFAULT_PIXEL_FORMAT,
    )

    # Only touch the audio when it has been measured to need it: an unnecessary filter is a
    # chance to damage a file that was already fine.
    if repair_audio and (audio_filter := audio_repair_filter(input_path, max_audio_hole_seconds)) is not None:
        output_kwargs["af"] = audio_filter

    # If user has opted out of forcing key-frames, keep key frames at the same timestamps as
    # present in the initial video.
    if key_frame_interval is None:
        output_kwargs["force_key_frames"] = "source"
    else:
        output_kwargs["force_key_frames"] = f"expr:gte(t,n_forced*{key_frame_interval})"

    # If user specified an output resolution, add respective video filters
    if resolution is not None:
        output_kwargs["vf"] = scale_factor_from_resolution(resolution)

    input_kwargs = {}
    if num_threads is not None:
        input_kwargs["threads"] = str(num_threads)

    # Run ffmpeg in subprocess
    video_in = ffmpeg.input(str(input_path), **input_kwargs)
    video_out = video_in.output(str(output_path), **output_kwargs)
    logger.info(f"Running command: '{shlex.join(video_out.compile())}'")
    video_out.run()

    if repair_audio:
        _assert_output_will_segment(input_path, output_path)

    # Warn the user if the number of frames changes as a result of re-encoding the video
    frames_before = frame_count(input_path)
    frames_after = frame_count(output_path)
    if frames_before != frames_after:
        logger.warning(
            "H264 re-encoded video '%s' has differing frames from original '%s' (%d vs. %d)",
            output_path,
            input_path,
            frames_after,
            frames_before,
        )


def _assert_output_will_segment(input_path: pathlib.Path, output_path: pathlib.Path) -> None:
    """Raise if the converted file still would not segment, so it is never uploaded in vain.

    Normalizing and uploading a long video costs minutes; finding out afterwards that it is still
    unusable costs those minutes twice. Checking here turns that into a few seconds and an error
    naming what is still wrong.
    """
    if probe_audio_stream(output_path) is None or survives_strict_segmentation(output_path):
        return

    # Only now is the expensive measurement worth running: it exists to explain the failure.
    timeline = measure_audio_timeline(output_path)
    detail = timeline.describe() if timeline is not None else "its audio timeline is unusable"
    raise AudioTimelineError(
        f"Normalized '{input_path}' to '{output_path}', but the result still cannot be segmented: "
        f"{detail}. Uploading it would fail after the transfer, so it is being reported now. "
        f"The output has been left in place for inspection."
    )


def frame_count(video_path: pathlib.Path) -> int:
    """Given a path to a video file, return the number of frames present in the video.

    NOTE: if no streams are present, returns 0. If multiple streams are present, returns frame count
          of the first video stream.
    """
    assert video_path.exists()
    probe_resp = ffmpeg.probe(
        video_path, v="error", select_streams="v:0", count_packets=None, show_entries="stream=nb_read_packets"
    )

    # No video streams present
    if len(probe_resp["streams"]) == 0:
        return 0

    return int(probe_resp["streams"][0]["nb_read_packets"])


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
