from __future__ import annotations

import logging
import urllib.parse
import warnings
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Mapping, Type

from conjure_python_client import ConjureHTTPError
from nominal_api import scout_video_api
from nominal_video import Sink, Src, Stream, StreamOptions

from nominal.core.exceptions import (
    LegacyVideoDeprecationWarning,
    NominalVideoStreamError,
    NominalVideoStreamNotOpenError,
)
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    from nominal.core.dataset import Dataset
    from nominal.core.video import Video

logger = logging.getLogger(__name__)


@dataclass
class VideoStream:
    """A live video stream from any source to a video channel on a Nominal dataset via WHIP.

    Use ``VideoStream.create()`` to construct — it resolves the WHIP endpoint
    from the Nominal video and prepares the pipeline configuration. The pipeline
    itself is not started until ``open()`` is called (or the context manager is entered).

    Requires ``pip install 'nominal[video]'`` and GStreamer 1.20+ on your system.

    Example::

        from nominal.experimental.video import VideoStream, Src, StreamOptions

        dataset = client.create_dataset("my stream")

        # Context manager — open/close handled automatically:
        with VideoStream.create(dataset, Src.camera(), channel="camera/front") as stream:
            stream.run()

        # Timed stream — run for a fixed timeout then exit:
        with VideoStream.create(video, Src.udp_rtp(5000)) as stream:
            stream.run(timedelta(seconds=30))

        # Manual lifecycle — useful when you need the stream object outside a with block,
        # or to restart after a NominalVideoStreamError (e.g. source disconnected):
        stream = VideoStream.create(video, Src.rtsp("rtsp://192.168.1.10/live"))
        stream.open()
        try:
            stream.run()
        except NominalVideoStreamError:
            stream.restart()  # re-opens the pipeline with the same WHIP endpoint
            stream.run()
        finally:
            stream.close()

        # Push frames manually from your own source.
        # frame_bytes must be raw RGB bytes: width * height * 3 bytes per frame.
        # Use Src.app(width, height, format=ImageFormat.Bgr) if your source is BGR (e.g. OpenCV).
        with VideoStream.create(video, Src.app(1280, 720)) as stream:
            while capturing:
                frame_bytes: bytes = capture_rgb_frame()  # 1280 * 720 * 3 bytes
                stream.send_frame(frame_bytes, timestamp_ns=time.time_ns())
    """

    rid: str
    src: Src
    options: StreamOptions | None
    whip_sink: Sink = field(repr=False)
    _stream: Stream | None = field(default=None, init=False, repr=False)

    @classmethod
    def create(
        cls,
        target: Dataset | Video,
        src: Src,
        options: StreamOptions | None = None,
        *,
        channel: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> VideoStream:
        """Create a VideoStream for a video channel on a dataset.

        Resolves the WHIP endpoint from Nominal and configures the pipeline.
        The pipeline is not started until ``open()`` is called.

        Args:
            target: The dataset owning the video channel to stream to. Passing a legacy standalone
                `Video` is deprecated.
            src: Video source. Common options:

                - ``Src.camera()`` — local webcam
                - ``Src.rtsp("rtsp://...")`` — RTSP stream
                - ``Src.udp_rtp(port)`` — incoming RTP over UDP
                - ``Src.udp_mpegts(port)`` — incoming MPEG-TS over UDP
                - ``Src.file("path/to/video.mp4")`` — video file
                - ``Src.app(width, height)`` — push frames manually via send_frame()

            options: Encoding options — codec, bitrate, resolution, overlay, fps, etc.
                Defaults to H264 at 4 Mbps with no overlay.
            channel: Name of the video channel on the dataset to stream to. Required when streaming to a
                dataset; unused when streaming to a legacy `Video`.
            tags: Tags identifying the channel's series. Defaults to none.

        Returns:
            A configured VideoStream, ready to open.

        Raises:
            ValueError: `channel` was omitted for a dataset target, or supplied for a `Video` target.
            NominalVideoStreamError: Nominal rejected the request for a WHIP endpoint.
        """
        resp = cls._request_whip_stream(target, channel=channel, tags=tags)

        whip_url = resp.whip_url
        parsed = urllib.parse.urlparse(whip_url)
        endpoint = urllib.parse.urlunparse(parsed._replace(query=""))
        query_params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        token_list = query_params.get("token")
        token = token_list[0] if token_list else None

        stun_url: str | None = None
        if resp.ice_servers:
            if len(resp.ice_servers) > 1:
                logger.warning(
                    "Received %d ICE servers, using only the first one with urls: %s",
                    len(resp.ice_servers),
                    resp.ice_servers[0].urls,
                )
            if resp.ice_servers[0].urls:
                stun_url = resp.ice_servers[0].urls[0].replace("stun:", "stun://", 1)

        whip_sink = Sink.whip(endpoint=endpoint, token=token, stun_server=stun_url)
        return cls(rid=target.rid, src=src, options=options, whip_sink=whip_sink)

    @staticmethod
    def _request_whip_stream(
        target: Dataset | Video,
        *,
        channel: str | None,
        tags: Mapping[str, str] | None,
    ) -> scout_video_api.GenerateWhipStreamResponse:
        """Resolve a WHIP endpoint for a dataset channel, or for a legacy video (deprecated)."""
        # Imported here rather than at module scope: nominal.core.video imports this package lazily,
        # and a top-level import would close that cycle.
        from nominal.core.video import Video

        clients = target._clients
        if isinstance(target, Video):
            if channel is not None:
                raise ValueError("'channel' does not apply when streaming to a legacy `Video`")
            warnings.warn(
                "Streaming to a standalone `Video` is deprecated in favor of video channels on a dataset. "
                "Pass a `Dataset` with `channel=...` instead.",
                LegacyVideoDeprecationWarning,
                stacklevel=3,
            )
            try:
                return clients.video.generate_whip_stream(clients.auth_header, target.rid)
            except ConjureHTTPError as e:
                raise NominalVideoStreamError(f"failed to create WHIP stream for video {target.rid!r}: {e}") from e

        if channel is None:
            raise ValueError("'channel' is required when streaming to a dataset")
        request = scout_video_api.GenerateWhipStreamV2Request(
            channel_series=scout_video_api.VideoChannelSeries(
                data_source=scout_video_api.VideoDataSourceChannel(
                    channel=channel,
                    data_source_rid=target.rid,
                    tags={**(tags or {})},
                )
            )
        )
        try:
            return clients.video.generate_whip_stream_v2(clients.auth_header, request)
        except ConjureHTTPError as e:
            raise NominalVideoStreamError(
                f"failed to create WHIP stream for channel {channel!r} on dataset {target.rid!r}: {e}"
            ) from e

    def open(self) -> None:
        """Build and start the GStreamer pipeline. Idempotent — safe to call multiple times.

        Raises:
            NominalVideoStreamError: if the pipeline fails to start (e.g. device not found, bad source URL).
        """
        if self._stream is not None:
            return
        try:
            self._stream = Stream(self.src, self.whip_sink, options=self.options)
            self._stream.open()
        except Exception as e:
            self._stream = None
            raise NominalVideoStreamError("failed to start video stream") from e

    def close(self) -> None:
        """Stop the pipeline and release all resources. Idempotent — safe to call multiple times.

        After close(), open() can be called again to restart with the same WHIP endpoint.
        """
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def run(self, timeout: timedelta | float | None = None) -> None:
        """Block until the stream ends, errors, or Ctrl+C is pressed.

        Calls close() internally when done, so no explicit cleanup is needed after run().

        Args:
            timeout: How long to stream before stopping — either a timedelta or a number of seconds.
                If None, runs until the source ends naturally (e.g. end of file) or until interrupted with Ctrl+C.

        Raises:
            NominalVideoStreamNotOpenError: if the stream is not open — call open() first or use as a context manager.
            NominalVideoStreamError: if the pipeline encounters an unrecoverable error.
        """
        if self._stream is None:
            raise NominalVideoStreamNotOpenError()
        try:
            seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
            self._stream.run(seconds)
        except RuntimeError as e:
            raise NominalVideoStreamError("Error running video stream") from e
        finally:
            self.close()

    def restart(self) -> None:
        """Stop and restart the pipeline.

        Useful for recovering from errors or reconnecting after a source interruption.
        Reuses the same WHIP endpoint resolved at create() time.
        """
        self.close()
        self.open()

    def send_frame(self, data: bytes, timestamp_ns: IntegralNanosecondsUTC | None = None) -> bool:
        """Push a raw video frame into the pipeline. Only valid when using ``Src.app()``.

        Args:
            data: Raw frame bytes. Format must match the format passed to ``Src.app()``
                (default is RGB — width * height * 3 bytes).
            timestamp_ns: Absolute timestamp in nanoseconds (Unix epoch). If None,
                the pipeline assigns a timestamp automatically.

        Returns:
            True if the frame was accepted, False if the internal buffer is full.

        Raises:
            NominalVideoStreamNotOpenError: if the stream is not open.
        """
        if self._stream is None:
            raise NominalVideoStreamNotOpenError()
        return bool(self._stream.send_frame(data, timestamp_ns))

    def __enter__(self) -> VideoStream:
        """Enter context manager, opening the pipeline."""
        self.open()
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit context manager, closing the pipeline."""
        self.close()
