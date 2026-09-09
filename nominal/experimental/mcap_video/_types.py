"""The decoder configuration a direct-MCAP registration is made of.

These mirror `nominal.video.v3.DirectMcapVideoChannel` without being generated from it, so a scan can
be inspected, retagged and renamed before anything is persisted -- and so the scanner is usable
without the transport bump that carries the generated bindings.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Sequence

from nominal.ts import IntegralNanosecondsUTC

# Mirrors the server's caps. Checked locally so an oversized scan is caught before an upload and a
# round trip, not after; the server enforces these independently and remains the authority.
MAX_CHUNK_RANGES_PER_REQUEST = 50_000
MAX_CHUNK_RANGES_PER_CHANNEL = 25_000
MAX_CHANNELS_PER_FILE = 64


class McapMessageEncoding(Enum):
    """Wire envelope of the CompressedVideo messages on a topic."""

    PROTOBUF = "PROTOBUF"
    CDR = "CDR"


class McapVideoCodec(Enum):
    """Video codec carried by a topic."""

    H264 = "H264"
    H265 = "H265"


class McapBitstreamFormat(Enum):
    """How NAL units are delimited inside a sample."""

    ANNEX_B = "ANNEX_B"
    LENGTH_PREFIXED = "LENGTH_PREFIXED"


@dataclass(frozen=True)
class McapChunkRange:
    """The time range covered by one MCAP chunk containing a topic."""

    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC


@dataclass(frozen=True)
class McapVideoChannelSpec:
    """One video topic and everything playback needs to decode it.

    `channel` is the name the topic is registered under in the series catalog and is the only field
    a caller would ordinarily change; the rest are derived from the file and describe what is
    actually in it.
    """

    channel: str
    topic: str
    tags: Mapping[str, str]
    message_encoding: McapMessageEncoding
    codec: McapVideoCodec
    bitstream_format: McapBitstreamFormat
    codec_string: str
    width: int
    height: int
    frame_rate: float
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC
    chunks: Sequence[McapChunkRange]

    def __repr__(self) -> str:
        # The chunk list runs to hundreds of entries and would bury everything worth reading, which
        # is the whole point of being able to print a spec before registering it.
        return (
            f"McapVideoChannelSpec(channel={self.channel!r}, topic={self.topic!r}, tags={dict(self.tags)!r}, "
            f"codec={self.codec.value}, codec_string={self.codec_string!r}, "
            f"{self.width}x{self.height} @ {self.frame_rate:.3f}fps, "
            f"message_encoding={self.message_encoding.value}, bitstream_format={self.bitstream_format.value}, "
            f"chunks={len(self.chunks)})"
        )


@dataclass(frozen=True)
class McapVideoScan:
    """What one MCAP holds, as a registration would describe it.

    Returned by `scan_mcap_video`. Inspect it, adjust the channel names and tags, then hand it to
    registration -- the two steps are separate so the derived configuration and the topic-to-channel
    mapping can be checked before they are persisted.
    """

    channels: tuple[McapVideoChannelSpec, ...]
    """Every video topic found, in topic order."""

    telemetry_topics: tuple[str, ...]
    """Every non-video topic. These are the ones to ingest."""

    chunk_count: int
    """Chunks in the file as a whole, whatever topics they carry."""

    message_count: int
    """Messages in the file as a whole."""

    @property
    def video_topics(self) -> tuple[str, ...]:
        """Topics that must NOT be ingested.

        Ingesting a video topic converts it to HLS, and playback prefers ingested segments over a
        direct-MCAP registration, so ingesting these is what quietly disables the feature. Pass them
        to an ingest's exclusion list.
        """
        return tuple(channel.topic for channel in self.channels)

    @property
    def total_chunk_ranges(self) -> int:
        """Chunk ranges across every channel -- what the request-level cap is measured against."""
        return sum(len(channel.chunks) for channel in self.channels)

    def with_tags(self, tags: Mapping[str, str]) -> McapVideoScan:
        """Return a copy with `tags` applied to every channel, replacing any already set."""
        frozen = MappingProxyType(dict(tags))
        return replace(self, channels=tuple(replace(c, tags=frozen) for c in self.channels))

    def rename_channels(self, names: Mapping[str, str]) -> McapVideoScan:
        """Return a copy with channels renamed, keyed by topic.

        Keyed by topic rather than by current channel name because the topic is the stable identity
        of the thing being renamed, and renaming twice should not depend on what the first rename
        produced.

        Raises:
            KeyError: if a key names no topic in this scan, which is nearly always a typo rather
                than an intentional no-op.
        """
        known = {channel.topic for channel in self.channels}
        unknown = set(names) - known
        if unknown:
            raise KeyError(f"no such topic(s) in this scan: {sorted(unknown)}")
        return replace(
            self,
            channels=tuple(replace(c, channel=names.get(c.topic, c.channel)) for c in self.channels),
        )

    def validate(self) -> None:
        """Raise if this scan could not be registered as it stands.

        The server enforces all of this too. Checking here turns a rejected request into an error
        that names the offending channel, and does it before a multi-gigabyte upload rather than
        after.

        Raises:
            ValueError: if the scan is empty, exceeds a documented cap, or carries a channel whose
                decoder configuration is incomplete.
        """
        if not self.channels:
            raise ValueError("scan found no video topics, so there is nothing to register")
        if len(self.channels) > MAX_CHANNELS_PER_FILE:
            raise ValueError(
                f"{len(self.channels)} channels exceeds the {MAX_CHANNELS_PER_FILE} per file the API accepts"
            )
        if self.total_chunk_ranges > MAX_CHUNK_RANGES_PER_REQUEST:
            raise ValueError(
                f"{self.total_chunk_ranges} chunk ranges exceeds the {MAX_CHUNK_RANGES_PER_REQUEST} "
                "the API accepts across one request"
            )
        for channel in self.channels:
            if len(channel.chunks) > MAX_CHUNK_RANGES_PER_CHANNEL:
                raise ValueError(
                    f"channel {channel.channel!r} has {len(channel.chunks)} chunk ranges, over the "
                    f"{MAX_CHUNK_RANGES_PER_CHANNEL} per channel the API accepts"
                )
            if not channel.codec_string:
                raise ValueError(f"channel {channel.channel!r} has no codec string")
            if channel.width <= 0 or channel.height <= 0:
                raise ValueError(f"channel {channel.channel!r} has a nonsensical size {channel.width}x{channel.height}")
            if channel.frame_rate <= 0:
                raise ValueError(f"channel {channel.channel!r} has a nonsensical frame rate {channel.frame_rate}")
