from __future__ import annotations

import pathlib
import time
from typing import Optional, cast

import click

from nominal.cli.util.global_decorators import global_options
from nominal.experimental.video_processing.resolution import (
    AnyResolutionType,
    ResolutionSpecifier,
    VideoResolution,
)
from nominal.experimental.video_processing.video_conversion import (
    check_gpu_acceleration,
    frame_count,
    get_video_rotation,
    has_audio_track,
    normalize_video,
)


@click.group(name="video")
def video_cmd() -> None:
    """Video processing and conversion commands."""
    pass


@video_cmd.command("convert")
@click.option(
    "-i",
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=True, path_type=pathlib.Path),
    help="Input video file path",
)
@click.option(
    "-o",
    "--output",
    "output_path",
    required=True,
    type=click.Path(path_type=pathlib.Path),
    help="Output video file path (.mp4 or .mkv)",
)
@click.option(
    "-r",
    "--resolution",
    type=click.Choice(["480p", "720p", "1080p", "1440p", "2160p"]),
    help="Target resolution for output video",
)
@click.option("--width", type=int, help="Custom output width in pixels (must be even)")
@click.option("--height", type=int, help="Custom output height in pixels (must be even)")
@click.option(
    "--gpu",
    type=click.Choice(["auto", "nvidia", "intel", "amd", "apple", "none"]),
    default="auto",
    show_default=True,
    help="GPU acceleration type to use",
)
@click.option("--preset", default="fast", show_default=True, help="Encoding preset (speed vs quality tradeoff)")
@click.option(
    "--preserve-aspect/--stretch",
    default=True,
    show_default=True,
    help="Preserve aspect ratio with letterboxing vs stretch to fill",
)
@click.option(
    "--keyframe-interval", type=int, default=2, show_default=True, help="Interval between keyframes in seconds"
)
@click.option("--force/--no-force", default=True, show_default=True, help="Overwrite output file if it exists")
@global_options
def convert_video(
    input_path: pathlib.Path,
    output_path: pathlib.Path,
    resolution: Optional[str],
    width: Optional[int],
    height: Optional[int],
    gpu: str,
    preset: str,
    preserve_aspect: bool,
    keyframe_interval: int,
    force: bool,
) -> None:
    """Convert video with GPU acceleration and resolution control.

    This command normalizes videos for optimal playback, with options for:
    - GPU acceleration (NVIDIA, Intel, AMD, Apple)
    - Resolution scaling with letterboxing
    - Custom encoding presets
    - Keyframe optimization

    Examples:
        nom video convert -i input.mp4 -o output.mp4 --resolution 1080p
        nom video convert -i vertical.mp4 -o horizontal.mp4 --width 1920 --height 1080
        nom video convert -i raw.mov -o optimized.mp4 --gpu nvidia --preset fast
    """
    # Validate output file extension
    if output_path.suffix.lower() not in [".mp4", ".mkv"]:
        raise click.BadParameter("Output file must have .mp4 or .mkv extension")

    # Determine resolution
    target_resolution: AnyResolutionType | None = None
    if width and height:
        if width % 2 != 0 or height % 2 != 0:
            raise click.BadParameter("Width and height must be even numbers")
        target_resolution = VideoResolution(resolution_width=width, resolution_height=height)
    elif resolution:
        target_resolution = cast(ResolutionSpecifier, resolution)
    elif width or height:
        raise click.BadParameter("Both width and height must be specified together")

    # Convert GPU string to enum or None
    gpu_accel = None if gpu == "none" else gpu

    # Show video info before conversion
    click.echo(f"📹 Input: {input_path}")
    click.echo(f"📁 Output: {output_path}")

    # Get video info
    rotation = get_video_rotation(input_path)
    frames = frame_count(input_path)
    has_audio = has_audio_track(input_path)

    if rotation != 0:
        click.echo(f"🔄 Rotation metadata: {rotation}° (orientation will be preserved)")
    if target_resolution:
        if isinstance(target_resolution, str):
            click.echo(f"🎯 Target resolution: {target_resolution}")
        else:
            click.echo(
                f"🎯 Target resolution: {target_resolution.resolution_width}x{target_resolution.resolution_height}"
            )
    click.echo(f"🎬 Frame count: {frames:,}")
    click.echo(f"🔊 Audio track: {'Yes' if has_audio else 'No'}")

    if preserve_aspect:
        click.echo("📦 Aspect ratio: Preserved with letterboxing")
    else:
        click.echo("⚠️  Aspect ratio: Will be stretched to fit")

    # Show GPU acceleration info
    if gpu_accel:
        available_gpu = check_gpu_acceleration(verbose=False)
        if available_gpu:
            click.echo(f"🚀 GPU acceleration: {gpu}")
        else:
            click.echo("🖥️  GPU acceleration: Not available, using CPU")
            gpu_accel = None
    else:
        click.echo("🖥️  GPU acceleration: Disabled")

    click.echo()

    try:
        # Start timing
        start_time = time.time()

        # Perform conversion
        with click.progressbar(length=1, label="Converting video") as bar:
            normalize_video(
                input_path=input_path,
                output_path=output_path,
                resolution=target_resolution,
                gpu_acceleration=gpu_accel,
                gpu_preset=preset,
                preserve_aspect_ratio=preserve_aspect,
                key_frame_interval=keyframe_interval,
                force=force,
            )
            bar.update(1)

        # End timing
        end_time = time.time()
        duration = end_time - start_time

        # Format duration in a human-readable way
        def format_duration(seconds: float) -> str:
            if seconds < 60:
                return f"{seconds:.1f} seconds"
            elif seconds < 3600:
                minutes = int(seconds // 60)
                remaining_seconds = seconds % 60
                return f"{minutes}m {remaining_seconds:.1f}s"
            else:
                hours = int(seconds // 3600)
                remaining_minutes = int((seconds % 3600) // 60)
                remaining_seconds = seconds % 60
                return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"

        # Show results
        input_size = input_path.stat().st_size / (1024 * 1024)
        output_size = output_path.stat().st_size / (1024 * 1024)
        compression_ratio = output_size / input_size

        # Calculate processing speed
        processing_speed = input_size / duration if duration > 0 else 0

        click.echo()
        click.secho("✅ Conversion completed successfully!", fg="green")
        click.echo(f"⏱️  Processing time: {format_duration(duration)}")
        click.echo(f"🚀 Processing speed: {processing_speed:.1f} MB/s")
        click.echo(f"📊 Input size:  {input_size:.1f} MB")
        click.echo(f"📊 Output size: {output_size:.1f} MB")
        click.echo(f"📊 Compression: {compression_ratio:.2f}x")

        # Calculate frames per second if we have frame count
        try:
            if frames > 0 and duration > 0:
                fps_processed = frames / duration
                click.echo(f"🎬 Processing rate: {fps_processed:.0f} frames/second")
        except (ZeroDivisionError, TypeError):
            pass  # Skip if frame count unavailable

        # Show GPU acceleration performance hint
        if gpu_accel and processing_speed > 0:
            # Rough estimate: GPU should be 3-10x faster than CPU
            estimated_cpu_time = duration * 5  # Conservative estimate
            if estimated_cpu_time > 60:
                time_saved = format_duration(estimated_cpu_time - duration)
                click.secho(f"💡 GPU acceleration likely saved ~{time_saved} vs CPU encoding", fg="cyan")

        if compression_ratio > 1.2:
            click.secho("ℹ️  Output is larger than input - consider adjusting quality settings", fg="yellow")
        elif compression_ratio < 0.3:
            click.secho("ℹ️  High compression achieved - verify quality is acceptable", fg="cyan")

    except Exception as e:
        click.secho(f"❌ Conversion failed: {e}", fg="red", err=True)
        raise click.ClickException(str(e))


@video_cmd.command("check-gpu")
@global_options
def check_gpu() -> None:
    """Check available GPU acceleration options on this system.

    This command detects which hardware acceleration encoders are available
    in your ffmpeg installation and provides recommendations for usage.
    """
    click.echo("🔍 Checking GPU acceleration capabilities...\n")

    available = check_gpu_acceleration(verbose=True)

    if available:
        click.echo("\n💡 Recommended usage:")
        click.echo(f"   nom video convert -i input.mp4 -o output.mp4 --gpu {available[0].value}")
        click.echo("   nom video convert -i input.mp4 -o output.mp4 --gpu auto")
    else:
        click.echo("\n💡 To enable GPU acceleration:")
        click.echo("   • Install compatible GPU drivers")
        click.echo("   • Ensure ffmpeg was compiled with hardware acceleration support")
        click.echo("   • On Windows: Install ffmpeg with --enable-nvenc/--enable-amf/--enable-qsv")
        click.echo("   • On macOS: Use ffmpeg with VideoToolbox support")
        click.echo("   • On Linux: Install appropriate codec packages")


@video_cmd.command("info")
@click.argument("video_path", type=click.Path(exists=True, path_type=pathlib.Path))
@global_options
def video_info(video_path: pathlib.Path) -> None:
    """Get detailed information about a video file.

    This command analyzes a video file and displays technical information
    including dimensions, rotation, frame count, and audio tracks.

    Example:
        nom video info my_video.mp4
    """
    import ffmpeg

    click.echo(f"📹 Analyzing: {video_path}\n")

    try:
        # Get basic video information using ffprobe
        probe = ffmpeg.probe(str(video_path))

        # Find video and audio streams
        video_stream = None
        audio_streams = []

        for stream in probe.get("streams", []):
            if stream.get("codec_type") == "video":
                video_stream = stream
            elif stream.get("codec_type") == "audio":
                audio_streams.append(stream)

        if video_stream:
            width = video_stream.get("width", "Unknown")
            height = video_stream.get("height", "Unknown")
            codec = video_stream.get("codec_name", "Unknown")
            fps = video_stream.get("r_frame_rate", "Unknown")

            # Calculate duration
            duration = float(probe.get("format", {}).get("duration", 0))
            duration_str = f"{duration:.2f}s" if duration else "Unknown"

            click.echo("🎬 Video Information:")
            click.echo(f"   Resolution: {width}x{height}")
            click.echo(f"   Codec: {codec}")
            click.echo(f"   Frame rate: {fps}")
            click.echo(f"   Duration: {duration_str}")

            # Get rotation
            rotation = get_video_rotation(video_path)
            if rotation != 0:
                click.echo(f"   Rotation: {rotation}°")

            # Get frame count
            frames = frame_count(video_path)
            click.echo(f"   Frame count: {frames:,}")

        else:
            click.secho("⚠️  No video stream found", fg="yellow")

        # Audio information
        click.echo("\n🔊 Audio Information:")
        if audio_streams:
            for i, stream in enumerate(audio_streams):
                codec = stream.get("codec_name", "Unknown")
                channels = stream.get("channels", "Unknown")
                sample_rate = stream.get("sample_rate", "Unknown")
                click.echo(f"   Track {i + 1}: {codec}, {channels} channels, {sample_rate} Hz")
        else:
            click.echo("   No audio tracks")

        # File information
        file_size = video_path.stat().st_size / (1024 * 1024)
        bitrate = probe.get("format", {}).get("bit_rate")
        bitrate_str = f"{int(bitrate) // 1000} kbps" if bitrate else "Unknown"

        click.echo("\n📁 File Information:")
        click.echo(f"   Size: {file_size:.1f} MB")
        click.echo(f"   Bitrate: {bitrate_str}")
        click.echo(f"   Format: {probe.get('format', {}).get('format_name', 'Unknown')}")

    except Exception as e:
        click.secho(f"❌ Failed to analyze video: {e}", fg="red", err=True)
        raise click.ClickException(str(e))


@video_cmd.command("presets")
@global_options
def list_presets() -> None:
    """List available encoding presets for different GPU types.

    This command shows the recommended encoding presets for each type of
    GPU acceleration, helping you choose the right speed/quality balance.
    """
    from nominal.experimental.video_processing.video_conversion import GPU_PRESET_MAP

    click.echo("🎛️  Available encoding presets by GPU type:\n")

    preset_descriptions = {
        "nvidia": {
            "fast": "Fastest encoding, good quality",
            "medium": "Balanced speed and quality",
            "slow": "Higher quality, slower encoding",
            "hq": "High quality mode",
            "hp": "High performance mode",
            "default": "Default NVENC settings",
        },
        "intel": {
            "veryfast": "Fastest encoding",
            "faster": "Very fast encoding",
            "fast": "Fast encoding (recommended)",
            "medium": "Balanced (default)",
            "slow": "Higher quality",
            "slower": "Much higher quality",
            "veryslow": "Best quality",
        },
        "amd": {
            "speed": "Prioritize encoding speed",
            "balanced": "Balance speed and quality (recommended)",
            "quality": "Prioritize output quality",
        },
        "apple": {
            "veryfast": "Fastest encoding",
            "fast": "Fast encoding (recommended)",
            "medium": "Balanced (default)",
            "slow": "Higher quality",
            "veryslow": "Best quality",
        },
    }

    for gpu_type, presets in GPU_PRESET_MAP.items():
        if gpu_type.value in preset_descriptions:
            click.echo(f"🔧 {gpu_type.value.upper()} ({gpu_type.name}):")
            descriptions = preset_descriptions[gpu_type.value]
            for preset in presets:
                desc = descriptions.get(preset, "")
                if desc:
                    click.echo(f"   {preset:<12} - {desc}")
                else:
                    click.echo(f"   {preset}")
            click.echo()

    click.echo("💡 Usage example:")
    click.echo("   nom video convert -i input.mp4 -o output.mp4 --gpu nvidia --preset fast")
