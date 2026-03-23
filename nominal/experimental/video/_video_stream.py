from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Type

from conjure_python_client import ConjureHTTPError
from nominal_video import Sink, Src, Stream, StreamOptions

from nominal.core.exceptions import NominalVideoError, NominalVideoStreamNotOpenError

if TYPE_CHECKING:
    from nominal.core.video import Video


@dataclass
class VideoStream:
    """A live video stream from any source to a Nominal video via WHIP.

    Use ``VideoStream.create()`` to construct — it resolves the WHIP endpoint
    from the Nominal video and prepares the pipeline configuration. The pipeline
    itself is not started until ``open()`` is called (or the context manager is entered).

    Requires ``pip install 'nominal[video]'`` and GStreamer 1.20+ on your system.

    Example::

        from nominal.experimental.video import VideoStream, Src, StreamOptions

        video = client.create_video("my stream")

        # Context manager — open/close handled automatically:
        with VideoStream.create(video, Src.camera()) as stream:
            stream.run()

        # Timed stream — run for a fixed timeout then exit:
        with VideoStream.create(video, Src.udp_rtp(5000)) as stream:
            stream.run(timedelta(seconds=30))

        # Manual lifecycle — useful when you need the stream object outside a with block,
        # or to restart after a NominalVideoError (e.g. source disconnected):
        stream = VideoStream.create(video, Src.rtsp("rtsp://192.168.1.10/live"))
        stream.open()
        try:
            stream.run()
        except NominalVideoError:
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
        video: Video,
        src: Src,
        options: StreamOptions | None = None,
    ) -> VideoStream:
        """Create a VideoStream for a Nominal video.

        Resolves the WHIP endpoint from Nominal and configures the pipeline.
        The pipeline is not started until ``open()`` is called.

        Args:
            video: The Nominal video to stream to.
            src: Video source. Common options:

                - ``Src.camera()`` — local webcam
                - ``Src.rtsp("rtsp://...")`` — RTSP stream
                - ``Src.udp_rtp(port)`` — incoming RTP over UDP
                - ``Src.udp_mpegts(port)`` — incoming MPEG-TS over UDP
                - ``Src.file("path/to/video.mp4")`` — video file
                - ``Src.app(width, height)`` — push frames manually via send_frame()

            options: Encoding options — codec, bitrate, resolution, overlay, fps, etc.
                Defaults to H264 at 4 Mbps with no overlay.

        Returns:
            A configured VideoStream, ready to open.
        """
        try:
            resp = video._clients.video.generate_whip_stream(video._clients.auth_header, video.rid)
        except ConjureHTTPError as e:
            raise NominalVideoError(f"failed to create WHIP stream for video {video.rid!r}: {e}") from e

        whip_url = resp.whip_url
        parsed = urllib.parse.urlparse(whip_url)
        endpoint = urllib.parse.urlunparse(parsed._replace(query=""))
        query_params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        token_list = query_params.get("token")
        token = token_list[0] if token_list else None

        stun_url: str | None = None
        if resp.ice_servers and resp.ice_servers[0].urls:
            stun_url = resp.ice_servers[0].urls[0].replace("stun:", "stun://", 1)

        whip_sink = Sink.whip(endpoint=endpoint, token=token, stun_server=stun_url)
        return cls(rid=video.rid, src=src, options=options, whip_sink=whip_sink)

    def open(self) -> None:
        """Build and start the GStreamer pipeline. Idempotent — safe to call multiple times.

        Raises:
            NominalVideoError: if the pipeline fails to start (e.g. device not found, bad source URL).
        """
        if self._stream is not None:
            return
        try:
            self._stream = Stream(self.src, self.whip_sink, options=self.options)
            self._stream.open()
        except RuntimeError as e:
            self._stream = None
            raise NominalVideoError("failed to start video stream") from e

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
            NominalVideoError: if the pipeline encounters an unrecoverable error.
            KeyboardInterrupt: if interrupted with Ctrl+C.
        """
        if self._stream is None:
            raise NominalVideoStreamNotOpenError()
        try:
            seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
            self._stream.run(seconds)
        except RuntimeError as e:
            raise NominalVideoError(f"video stream error: {e}") from e
        finally:
            self.close()

    def restart(self) -> None:
        """Stop and restart the pipeline.

        Useful for recovering from errors or reconnecting after a source interruption.
        Reuses the same WHIP endpoint resolved at create() time.
        """
        self.close()
        self.open()

    def send_frame(self, data: bytes, timestamp_ns: int | None = None) -> bool:
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
        self.open()
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        self.close()
