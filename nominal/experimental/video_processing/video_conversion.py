from __future__ import annotations

import logging
import pathlib
import shlex
import subprocess
from enum import Enum

import ffmpeg

from nominal.experimental.video_processing.resolution import (
    AnyResolutionType,
    VideoResolution,
    _resolution_from_specifier,
    scale_factor_from_resolution,
)

logger = logging.getLogger(__name__)

__all__ = [
    "normalize_video",
    "check_gpu_acceleration",
    "frame_count",
    "has_audio_track",
    "get_video_rotation",
    "GPUAcceleration",
    "GPU_CODEC_MAP",
    "GPU_PRESET_MAP",
]

DEFAULT_VIDEO_CODEC = "h264"
DEFAULT_AUDIO_CODEC = "aac"
DEFAULT_PIXEL_FORMAT = "yuv420p"
DEFAULT_KEY_FRAME_INTERVAL_SEC = 2


# Hardware acceleration codec mappings
class GPUAcceleration(Enum):
    """Supported GPU acceleration types."""

    NONE = "none"
    NVIDIA = "nvidia"  # NVENC
    INTEL = "intel"  # Quick Sync Video
    AMD = "amd"  # AMF
    APPLE = "apple"  # VideoToolbox (Apple Silicon/Intel)


# GPU codec mappings
GPU_CODEC_MAP = {
    GPUAcceleration.NVIDIA: "h264_nvenc",
    GPUAcceleration.INTEL: "h264_qsv",
    GPUAcceleration.AMD: "h264_amf",
    GPUAcceleration.APPLE: "h264_videotoolbox",
}

# Valid presets for each GPU type
GPU_PRESET_MAP = {
    GPUAcceleration.NVIDIA: ["default", "slow", "medium", "fast", "hp", "hq", "bd", "ll", "llhq", "llhp", "lossless"],
    GPUAcceleration.INTEL: ["veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"],
    GPUAcceleration.AMD: ["speed", "balanced", "quality"],
    GPUAcceleration.APPLE: ["veryslow", "slower", "slow", "medium", "fast", "faster", "veryfast"],
}


def _get_available_gpu_acceleration() -> list[GPUAcceleration]:
    """Detect available GPU acceleration options on the system.

    This function checks if ffmpeg has been compiled with support for various
    hardware acceleration encoders by parsing the output of `ffmpeg -encoders`.

    Returns:
        List of available GPU acceleration types, ordered by preference.
        Empty list if no GPU acceleration is available.

    Examples:
        >>> available = check_gpu_acceleration(verbose=False)
        >>> if available:
        ...     print(f"Using GPU: {available[0].value}")
        ... else:
        ...     print("No GPU acceleration available")
    """
    available: list[GPUAcceleration] = []

    try:
        # Get list of available encoders from ffmpeg
        result = subprocess.run(["ffmpeg", "-encoders"], check=False, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            logger.warning("ffmpeg command failed, cannot detect GPU acceleration")
            return available

        encoders_output = result.stdout + result.stderr

        # Check for specific hardware encoders (order matters for preference)
        if "h264_nvenc" in encoders_output:
            available.append(GPUAcceleration.NVIDIA)
        if "h264_qsv" in encoders_output:
            available.append(GPUAcceleration.INTEL)
        if "h264_amf" in encoders_output:
            available.append(GPUAcceleration.AMD)
        if "h264_videotoolbox" in encoders_output:
            available.append(GPUAcceleration.APPLE)

    except subprocess.TimeoutExpired:
        logger.warning("ffmpeg command timed out while detecting GPU encoders")
    except FileNotFoundError:
        logger.warning("ffmpeg not found in PATH, cannot detect GPU acceleration")
    except Exception as e:
        logger.warning(f"Could not detect available GPU encoders: {e}")

    return available


def check_gpu_acceleration(verbose: bool = True) -> list[GPUAcceleration]:
    """Check and optionally print available GPU acceleration options on this system.

    Args:
        verbose: If True, prints the results to stdout. If False, just returns the list.

    Returns:
        List of available GPU acceleration types.
    """
    available = _get_available_gpu_acceleration()

    if verbose:
        if not available:
            print("No GPU acceleration available. CPU encoding will be used.")
            print("\nTo enable GPU acceleration, ensure you have:")
            print("  - Compatible GPU hardware")
            print("  - Proper GPU drivers installed")
            print("  - ffmpeg compiled with hardware acceleration support")
        else:
            print("Available GPU acceleration options:")
            for gpu in available:
                codec = GPU_CODEC_MAP.get(gpu, "unknown")
                print(f"  - {gpu.value}: {codec}")

            print(f"\nRecommended usage: gpu_acceleration='{available[0].value}'")

    return available


def normalize_video(
    input_path: pathlib.Path,
    output_path: pathlib.Path,
    key_frame_interval: int | None = DEFAULT_KEY_FRAME_INTERVAL_SEC,
    force: bool = True,
    resolution: AnyResolutionType | None = None,
    gpu_acceleration: GPUAcceleration | str | None = None,
    gpu_preset: str = "fast",
    preserve_aspect_ratio: bool = True,
) -> None:
    """Convert video file to an h264 encoded video file using ffmpeg with optional GPU acceleration.

    This function will also perform several other processing tasks to ensure that video is
    properly encoded in a way that is best supported by nominal.
    This includes:
        * Ensuring that there are key-frames (I-frames) present approximately every 2s of video content
        * Video is encoded with H264 (CPU or GPU accelerated)
        * Audio is encoded with AAC
        * Video has YUV4:2:0 planar color space

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
        resolution: If provided, re-scale the video to the provided resolution.
        gpu_acceleration: Type of GPU acceleration to use. Can be GPUAcceleration enum,
            string ("nvidia", "intel", "amd", "apple"), or None for CPU encoding.
            If "auto", will automatically detect and use the best available option.
        gpu_preset: Encoding preset for GPU acceleration. Options vary by GPU:
            - NVIDIA: "default", "slow", "medium", "fast", "hp", "hq", "bd", "ll", "llhq", "llhp", "lossless"
            - Intel: "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"
            - AMD: "speed", "balanced", "quality"
            - Apple: "veryslow", "slower", "slow", "medium", "fast", "faster", "veryfast"
        preserve_aspect_ratio: If True, maintain the original aspect ratio when resizing.
            Adds letterboxing/pillarboxing as needed to fit the target resolution.
            If False, stretches the video to exactly fill the target resolution.

    NOTE: this requires that you have ffmpeg installed on your system with support for H264.
          For GPU acceleration, you need appropriate drivers and ffmpeg compiled with hardware support.

    Examples:
        # Basic CPU encoding (original behavior)
        normalize_video(input_path, output_path)

        # Auto-detect and use best available GPU acceleration
        normalize_video(input_path, output_path, gpu_acceleration="auto")

        # Explicitly use NVIDIA GPU acceleration with letterboxing
        normalize_video(input_path, output_path, resolution="1080p", gpu_acceleration="nvidia", gpu_preset="fast")

        # Process vertical video with letterboxing to fit horizontal resolution
        normalize_video(input_path, output_path, resolution="1080p", preserve_aspect_ratio=True)

        # Stretch video to exact resolution (may distort)
        normalize_video(input_path, output_path, resolution="1080p", preserve_aspect_ratio=False)

        # Check what GPU options are available
        from nominal.experimental.video_processing.video_conversion import check_gpu_acceleration
        available_gpus = check_gpu_acceleration()  # Prints options and returns list

        # Check video rotation metadata (for information only)
        from nominal.experimental.video_processing.video_conversion import get_video_rotation
        rotation = get_video_rotation(input_path)
        print(f"Video has rotation metadata: {rotation} degrees")
    """
    assert input_path.exists(), "Input path must exist"
    assert output_path.suffix.lower() in (".mkv", ".mp4")

    if output_path.exists():
        if force:
            logger.info(f"Output file {output_path} already exists! Deleting...")
            output_path.unlink()
        else:
            raise FileExistsError(f"Cannot convert {input_path} to {output_path}: output path already exists!")

    # Determine video codec based on GPU acceleration preference
    video_codec = DEFAULT_VIDEO_CODEC
    additional_args = {}

    if gpu_acceleration:
        if isinstance(gpu_acceleration, str):
            if gpu_acceleration == "auto":
                # Auto-detect best available GPU acceleration
                available_gpu = _get_available_gpu_acceleration()
                if available_gpu:
                    gpu_acceleration = available_gpu[0]  # Use first available
                    logger.info(f"Auto-detected GPU acceleration: {gpu_acceleration.value}")
                else:
                    logger.warning("No GPU acceleration available, falling back to CPU encoding")
                    gpu_acceleration = None
            else:
                # Convert string to enum
                try:
                    gpu_acceleration = GPUAcceleration(gpu_acceleration.lower())
                except ValueError as e:
                    valid_options = [gpu.value for gpu in GPUAcceleration]
                    logger.error(f"Invalid GPU acceleration type: {gpu_acceleration}. Valid options: {valid_options}")
                    raise ValueError(
                        f"Unsupported GPU acceleration: {gpu_acceleration}. Valid options: {valid_options}"
                    ) from e

        if gpu_acceleration and gpu_acceleration != GPUAcceleration.NONE:
            if gpu_acceleration in GPU_CODEC_MAP:
                video_codec = GPU_CODEC_MAP[gpu_acceleration]
                logger.info(f"Using GPU acceleration: {gpu_acceleration.value} with codec {video_codec}")

                # Validate preset for this GPU type
                valid_presets = GPU_PRESET_MAP.get(gpu_acceleration, [])
                if valid_presets and gpu_preset not in valid_presets:
                    logger.warning(
                        "Preset '%s' not in recommended presets for %s: %s. Using anyway.",
                        gpu_preset,
                        gpu_acceleration.value,
                        valid_presets,
                    )

                # Add GPU-specific encoding parameters
                if gpu_acceleration == GPUAcceleration.NVIDIA:
                    additional_args.update(
                        {
                            "preset": gpu_preset,
                            "rc": "vbr",  # Variable bitrate
                            "cq": "23",  # Constant quality (similar to CRF)
                        }
                    )
                elif gpu_acceleration == GPUAcceleration.INTEL:
                    additional_args.update(
                        {
                            "preset": gpu_preset,
                            "global_quality": "23",
                        }
                    )
                elif gpu_acceleration == GPUAcceleration.AMD:
                    additional_args.update(
                        {
                            "quality": gpu_preset,
                            "rc": "cqp",  # Constant quantization parameter
                            "qp_i": "23",
                            "qp_p": "23",
                        }
                    )
                elif gpu_acceleration == GPUAcceleration.APPLE:
                    additional_args.update(
                        {
                            "preset": gpu_preset,
                            "q:v": "23",  # Quality level
                        }
                    )
            else:
                logger.warning(f"GPU acceleration {gpu_acceleration.value} not supported, using CPU encoding")
                gpu_acceleration = None

    # Determine if input video has an audio track. If it doesn't, add in an empty audio track
    # to allow for seamless play of this video content alongside content with audio tracks.
    # While the backend will do this for you automatically, it dramatically faster to do it here
    # than in the backend since we are already re-encoding video.
    output_kwargs: dict[str, str | None] = dict(
        acodec=DEFAULT_AUDIO_CODEC,
        vcodec=video_codec,
        force_key_frames="source",
        pix_fmt=DEFAULT_PIXEL_FORMAT,
        **additional_args,
    )

    # If user has opted out of forcing key-frames, keep key frames at the same timestamps as
    # present in the initial video.
    if key_frame_interval is None:
        output_kwargs["force_key_frames"] = "source"
    else:
        output_kwargs["force_key_frames"] = f"expr:gte(t,n_forced*{key_frame_interval})"

    # Build video filters for scaling with letterboxing
    if resolution is not None:
        if preserve_aspect_ratio:
            # Use letterboxing to maintain aspect ratio
            target_width, target_height = _get_resolution_dimensions(resolution)
            video_filter = _build_letterbox_filter(target_width, target_height)
            logger.info(f"Applying letterboxing to {target_width}x{target_height}")
        else:
            # Stretch to exact resolution (may distort aspect ratio)
            video_filter = scale_factor_from_resolution(resolution)
            logger.info("Scaling without preserving aspect ratio")

        output_kwargs["vf"] = video_filter

    # Run ffmpeg in subprocess
    video_in = ffmpeg.input(str(input_path))
    video_out = video_in.output(str(output_path), **output_kwargs)
    logger.info(f"Running command: '{shlex.join(video_out.compile())}'")
    video_out.run()

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


def _get_resolution_dimensions(resolution: AnyResolutionType) -> tuple[int, int]:
    """Get width and height from resolution parameter.

    Args:
        resolution: Resolution specifier or VideoResolution object.

    Returns:
        Tuple of (width, height) in pixels.

    Raises:
        ValueError: If resolution cannot be determined.
    """
    if isinstance(resolution, VideoResolution):
        width = resolution.resolution_width
        height = resolution.resolution_height
    else:
        res_obj = _resolution_from_specifier(resolution)
        width = res_obj.resolution_width
        height = res_obj.resolution_height

    if width is None or height is None:
        raise ValueError(f"Cannot determine target dimensions from resolution: {resolution}")

    return width, height


def _build_letterbox_filter(target_width: int, target_height: int) -> str:
    """Build ffmpeg filter for letterboxing video to exact target resolution.

    This scales the video to fit within the target resolution while maintaining
    aspect ratio, then adds padding (letterboxing/pillarboxing) to reach the
    exact target dimensions.

    Args:
        target_width: Target width in pixels.
        target_height: Target height in pixels.

    Returns:
        FFmpeg video filter string.
    """
    # Scale to fit within target resolution, maintaining aspect ratio
    scale_filter = f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease"

    # Add padding to reach exact target resolution
    # (ow-iw)/2 centers horizontally, (oh-ih)/2 centers vertically
    pad_filter = f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2:black"

    return f"{scale_filter},{pad_filter}"


def get_video_rotation(video_path: pathlib.Path) -> int:
    """Get the rotation angle of a video file from its metadata.

    Args:
        video_path: Path to the video file.

    Returns:
        Rotation angle in degrees (0, 90, 180, or 270).
        Returns 0 if no rotation metadata is found.
    """
    assert video_path.exists()

    try:
        # Use ffprobe to get rotation information
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream_side_data=rotation",
                "-of",
                "csv=p=0",
                str(video_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0 and result.stdout.strip():
            rotation = float(result.stdout.strip())
            # Normalize rotation to 0, 90, 180, 270
            rotation = int(rotation) % 360
            if rotation < 0:
                rotation += 360
            return rotation

        # Fallback: try to get rotation from stream metadata using ffmpeg-python
        probe_output = ffmpeg.probe(str(video_path))

        if "streams" in probe_output:
            for stream in probe_output["streams"]:
                if stream.get("codec_type") == "video":
                    # Check for rotation in tags
                    tags = stream.get("tags", {})
                    if "rotate" in tags:
                        rotation = int(tags["rotate"]) % 360
                        if rotation < 0:
                            rotation += 360
                        return rotation

                    # Check side data for displaymatrix
                    side_data = stream.get("side_data_list", [])
                    for data in side_data:
                        if "rotation" in data:
                            rotation = int(float(data["rotation"])) % 360
                            if rotation < 0:
                                rotation += 360
                            return rotation

    except Exception as e:
        logger.warning(f"Could not determine video rotation: {e}")

    return 0


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
