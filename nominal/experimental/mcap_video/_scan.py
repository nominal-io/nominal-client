"""Derive a direct-MCAP registration from the file itself."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterator, Mapping

from mcap.reader import McapReader, make_reader

from nominal.core._types import PathLike
from nominal.experimental.mcap_video._bitstream import (
    FrameDecodeError,
    VideoFrame,
    extract_cdr_frame,
    extract_protobuf_frame,
    split_annex_b,
    split_length_prefixed,
)
from nominal.experimental.mcap_video._sps import SpsInfo, SpsParseError, parse_sps
from nominal.experimental.mcap_video._types import (
    McapBitstreamFormat,
    McapChunkRange,
    McapMessageEncoding,
    McapVideoChannelSpec,
    McapVideoCodec,
    McapVideoScan,
)

# Schema names that identify a topic as compressed video. Both spellings of the same Foxglove schema
# appear in the wild: the protobuf full name, and the ROS 2 message type rosbag2 records.
VIDEO_SCHEMA_NAMES = frozenset(
    {
        "foxglove.CompressedVideo",
        "foxglove_msgs/msg/CompressedVideo",
    }
)

# How far into a topic to look for a parameter set before giving up. The first message of a topic is
# usually an IDR carrying an SPS, but a recording started mid-stream can open on delta frames, so a
# few seconds' worth are searched rather than assuming frame zero.
_SPS_SEARCH_LIMIT = 300

_NANOS_PER_SECOND = 1_000_000_000


class McapVideoScanError(Exception):
    """Raised when an MCAP cannot be turned into a registration."""


@contextmanager
def _opened(source: PathLike | BinaryIO) -> Iterator[BinaryIO]:
    """Yield a readable stream, leaving a caller-supplied one open.

    A stream the caller passed in is theirs to close; only a path opened here is closed here.
    """
    if isinstance(source, (str, Path)):
        with Path(source).open("rb") as handle:
            yield handle
        return
    yield source


def default_channel_name(topic: str) -> str:
    """The channel name a topic registers under unless the caller renames it.

    Topic paths are hierarchical and channel names are not, so the separators are flattened rather
    than dropped: `/cameras/ball_eo` becomes `cameras_ball_eo`, which stays unique where taking the
    last segment would collide across cameras.
    """
    return topic.strip("/").replace("/", "_")


def scan_mcap_video(
    source: PathLike | BinaryIO,
    *,
    tags: Mapping[str, str] | None = None,
) -> McapVideoScan:
    """Read an MCAP and derive what a direct-MCAP registration needs.

    Reads the summary section for the chunk index, topics and timing, which is why a multi-gigabyte
    recording scans in about a second rather than in minutes. The codec string and picture size are
    not in the summary -- they live in the H.264 sequence parameter set -- so one keyframe per video
    topic is additionally sought out and parsed. Nothing else in the file is read.

    Args:
        source: Path to an MCAP, or an open binary stream positioned anywhere (it is seekable
            reading, so position does not matter).
        tags: Tags to apply to every derived channel. Part of the series identity, so changing them
            later registers a different series rather than updating this one.

    Returns:
        The scan. Inspect and adjust it, then register it.

    Raises:
        McapVideoScanError: the file has no summary section, carries no video topics, or carries a
            topic whose decoder configuration cannot be derived.
    """
    frozen_tags = MappingProxyType(dict(tags or {}))
    with _opened(source) as handle:
        reader = make_reader(handle)
        summary = reader.get_summary()
        if summary is None:
            raise McapVideoScanError(
                "this MCAP has no summary section, so it is not indexed. Direct-MCAP playback "
                "range-reads the object using the chunk index, so an unindexed file cannot be "
                "registered; re-record or re-write it with indexing enabled."
            )

        video_channels = {}
        telemetry_topics = []
        for channel_id, channel in summary.channels.items():
            schema = summary.schemas.get(channel.schema_id)
            if schema is not None and schema.name in VIDEO_SCHEMA_NAMES:
                video_channels[channel_id] = channel
            else:
                telemetry_topics.append(channel.topic)

        if not video_channels:
            raise McapVideoScanError(
                "no compressed-video topics found. Direct-MCAP playback reads topics whose schema is "
                f"one of {sorted(VIDEO_SCHEMA_NAMES)}; a recording of raw or per-frame-compressed "
                "images (sensor_msgs/Image, CompressedImage) is not a video stream and has to be "
                "ingested instead."
            )

        chunks_by_channel: dict[int, list[McapChunkRange]] = {channel_id: [] for channel_id in video_channels}
        for chunk_index in summary.chunk_indexes:
            for channel_id, ranges in chunks_by_channel.items():
                # A chunk index names the channels it contains, so a topic's chunk list is a filter
                # over the summary rather than a second pass over the file.
                if channel_id in chunk_index.message_index_offsets:
                    ranges.append(
                        McapChunkRange(
                            start=chunk_index.message_start_time,
                            end=chunk_index.message_end_time,
                        )
                    )

        statistics = summary.statistics
        specs = []
        for channel_id, channel in sorted(video_channels.items(), key=lambda item: item[1].topic):
            ranges = chunks_by_channel[channel_id]
            if not ranges:
                raise McapVideoScanError(
                    f"topic {channel.topic!r} appears in the summary but in no chunk, so it carries no frames to play"
                )
            message_encoding = _message_encoding(channel.message_encoding, channel.topic)
            parameter_set = _read_parameter_set(reader, channel_id, channel.topic, message_encoding)
            _reject_unsupported_codec(parameter_set.frame, channel.topic)

            # This channel's own span, not the file's. A camera that starts late or stops early spans
            # less than the recording does, and taking the file's span would give it a start time
            # before its first frame and a frame rate diluted by the difference.
            #
            # Exact rather than chunk-derived. A chunk's range covers every message in it, so a topic
            # sharing a chunk with another inherits that one's timestamps -- which for topics written
            # in separate passes is not a small error. The first timestamp already came free with the
            # parameter set; the last costs one more chunk, seeked to directly.
            start = parameter_set.first_log_time
            end = _last_log_time(reader, channel_id, channel.topic, max(chunk.start for chunk in ranges), start)
            message_count = (statistics.channel_message_counts.get(channel_id, 0) if statistics else 0) or 0
            specs.append(
                McapVideoChannelSpec(
                    channel=default_channel_name(channel.topic),
                    topic=channel.topic,
                    tags=frozen_tags,
                    message_encoding=message_encoding,
                    codec=McapVideoCodec.H264,
                    bitstream_format=parameter_set.bitstream_format,
                    codec_string=parameter_set.sps.codec_string,
                    width=parameter_set.sps.width,
                    height=parameter_set.sps.height,
                    frame_rate=_frame_rate(message_count, start, end, channel.topic),
                    start=start,
                    end=end,
                    chunks=tuple(ranges),
                )
            )

        return McapVideoScan(
            channels=tuple(specs),
            telemetry_topics=tuple(sorted(telemetry_topics)),
            chunk_count=len(summary.chunk_indexes),
            message_count=statistics.message_count if statistics else 0,
        )


def _message_encoding(raw: str, topic: str) -> McapMessageEncoding:
    lowered = raw.lower()
    if "protobuf" in lowered:
        return McapMessageEncoding.PROTOBUF
    if "cdr" in lowered or "ros2" in lowered:
        return McapMessageEncoding.CDR
    raise McapVideoScanError(
        f"topic {topic!r} uses message encoding {raw!r}; direct-MCAP playback reads protobuf and CDR"
    )


def _extract_frame(message: bytes, encoding: McapMessageEncoding) -> VideoFrame:
    if encoding is McapMessageEncoding.PROTOBUF:
        return extract_protobuf_frame(message)
    return extract_cdr_frame(message)


@dataclass(frozen=True)
class _ParameterSet:
    """What one topic's first readable keyframe told us."""

    frame: VideoFrame
    bitstream_format: McapBitstreamFormat
    sps: SpsInfo
    first_log_time: int


def _read_parameter_set(
    reader: McapReader,
    channel_id: int,
    topic: str,
    encoding: McapMessageEncoding,
) -> _ParameterSet:
    """Find the first parameter set on a topic, and the bitstream format that decoded it.

    The bitstream format is inferred from which splitter yields a parseable SPS rather than declared
    anywhere, because nothing in the MCAP records it -- and the player must be told which, since a
    length-prefixed stream fed to an Annex B decoder produces nothing at all.
    """
    searched = 0
    first_log_time: int | None = None
    last_error: Exception | None = None
    for _schema, message_channel, message in reader.iter_messages(topics=[topic]):
        if message_channel.id != channel_id:
            continue
        searched += 1
        if first_log_time is None:
            first_log_time = message.log_time
        try:
            frame = _extract_frame(message.data, encoding)
        except FrameDecodeError as error:
            last_error = error
            if searched >= _SPS_SEARCH_LIMIT:
                break
            continue

        for bitstream_format, splitter in (
            (McapBitstreamFormat.ANNEX_B, split_annex_b),
            (McapBitstreamFormat.LENGTH_PREFIXED, split_length_prefixed),
        ):
            try:
                sps = parse_sps(splitter(frame.data))
            except SpsParseError as error:
                last_error = error
                continue
            if sps is not None:
                return _ParameterSet(
                    frame=frame,
                    bitstream_format=bitstream_format,
                    sps=sps,
                    first_log_time=first_log_time,
                )

        if searched >= _SPS_SEARCH_LIMIT:
            break

    detail = f" (last error: {last_error})" if last_error is not None else ""
    raise McapVideoScanError(
        f"no H.264 parameter set in the first {searched} frames of {topic!r}, so the codec string "
        f"and picture size cannot be derived{detail}"
    )


def _last_log_time(reader: McapReader, channel_id: int, topic: str, last_chunk_start: int, fallback: int) -> int:
    """The timestamp of a topic's final message.

    Seeked to rather than scanned for: starting at the beginning of the last chunk that contains the
    topic reads that chunk and no earlier one, so the cost is one decompression however long the
    recording is.
    """
    latest = fallback
    for _schema, message_channel, message in reader.iter_messages(topics=[topic], start_time=last_chunk_start):
        if message_channel.id == channel_id and message.log_time > latest:
            latest = message.log_time
    return latest


def _reject_unsupported_codec(frame: VideoFrame, topic: str) -> None:
    """Refuse a codec this scanner cannot describe.

    The API accepts H.265, but its parameter sets are a different syntax that this scanner does not
    parse. Saying so is the point: the alternative -- what the internal script did -- is to label
    every stream H.264, which registers cleanly and then fails in the browser with an error naming
    nothing on this path.
    """
    declared = (frame.format or "").lower()
    if declared and "265" not in declared and "hevc" not in declared:
        return
    if "265" in declared or "hevc" in declared:
        raise McapVideoScanError(
            f"topic {topic!r} declares format {frame.format!r}. This scanner derives H.264 "
            "configuration only; register an H.265 channel by supplying its channel spec directly."
        )


def _frame_rate(message_count: int, start: int, end: int, topic: str) -> float:
    """Frames per second, from the channel's own first and last frame.

    Derived rather than read: the SPS carries timing only in its optional VUI block, which these
    recorders generally omit.

    Counts intervals rather than frames -- `n` frames between the first and last span `n - 1` of
    them -- which is what makes this exact for an evenly sampled topic instead of over by one
    frame's worth.
    """
    span_seconds = (end - start) / _NANOS_PER_SECOND
    if message_count < 2 or span_seconds <= 0:
        raise McapVideoScanError(
            f"topic {topic!r} has {message_count} message(s) over {span_seconds:.6f}s, so no frame "
            "rate can be derived from it"
        )
    return round((message_count - 1) / span_seconds, 6)


__all__ = ["McapVideoScanError", "VIDEO_SCHEMA_NAMES", "default_channel_name", "scan_mcap_video"]
