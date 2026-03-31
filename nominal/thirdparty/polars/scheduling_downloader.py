from __future__ import annotations

import dataclasses
import logging
import pathlib
import time
from concurrent.futures import Future

from typing_extensions import Self

from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DownloadTicket:
    """Handle for a submitted download. Tracks destination and per-part completion.

    Wraps the individual range-request part futures returned by submit_download().
    The file is complete when all part futures have resolved.
    """

    destination: pathlib.Path
    file_size_bytes: int
    _part_futures: list[Future[None]] = dataclasses.field(repr=False)

    @property
    def done(self) -> bool:
        """True if all parts have been downloaded."""
        return all(f.done() for f in self._part_futures)

    def result(self, timeout: float | None = None) -> pathlib.Path:
        """Block until all parts are downloaded. Returns the destination path.

        The timeout applies to the total wait across all parts, not per-part.
        Raises the first exception encountered if any part failed.

        On timeout or error, cancels any not-yet-started parts to avoid
        wasted pool work. Parts already running cannot be interrupted and
        will continue to completion in the background.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            for i, f in enumerate(self._part_futures):
                remaining = None if deadline is None else max(0, deadline - time.monotonic())
                f.result(timeout=remaining)
        except (Exception, KeyboardInterrupt):
            # Cancel any parts that haven't started yet
            for remaining_future in self._part_futures[i + 1 :]:
                remaining_future.cancel()
            raise
        return self.destination


class SchedulingDownloader:
    """Generic download service that accepts URLs and downloads files to a directory.

    Uses MultipartFileDownloader.submit_download() to submit range-request parts
    into a single shared thread pool. Parts from multiple files naturally interleave
    via the pool's FIFO queue -- earlier-submitted files' parts process first, and
    one slow part from a large file doesn't block other files' parts.

    No outer thread pool is needed. The only pool is MultipartFileDownloader's
    internal pool, which processes all range-request parts.

    Usage:
        with SchedulingDownloader.create(output_dir=Path("/tmp/dl")) as dl:
            ticket = dl.submit("https://s3.example.com/file.gz", file_size=1024, filename="out.gz")
            path = ticket.result()  # blocks until all parts complete
            dl.cleanup(ticket)      # delete file from disk
    """

    def __init__(self, output_dir: pathlib.Path, downloader: MultipartFileDownloader):
        """Initialize with an output directory and a multipart downloader."""
        self._output_dir = output_dir
        self._downloader = downloader

    @classmethod
    def create(
        cls,
        output_dir: pathlib.Path,
        max_workers: int = 8,
        download_timeout: float = 30.0,
        max_part_retries: int = 3,
    ) -> Self:
        """Create a SchedulingDownloader with a new MultipartFileDownloader."""
        downloader = MultipartFileDownloader.create(
            max_workers=max_workers,
            timeout=download_timeout,
            max_part_retries=max_part_retries,
        )
        return cls(output_dir=output_dir, downloader=downloader)

    def submit(self, url: str, file_size_bytes: int, filename: str) -> DownloadTicket:
        """Submit a file for download. Returns immediately with a ticket.

        The file is split into range-request parts and submitted to the shared
        thread pool. Parts from this file interleave with parts from other
        previously-submitted files.

        Args:
            url: Presigned URL to download from.
            file_size_bytes: Known file size (skips the HEAD probe).
            filename: Name for the file within the output directory.

        Returns:
            A DownloadTicket whose result() blocks until all parts are downloaded.
        """
        destination = self._output_dir / filename
        item = DownloadItem(
            provider=PresignedURLProvider.from_static(url),
            destination=destination,
            file_size=file_size_bytes,
        )
        part_futures = self._downloader.submit_download(item)
        return DownloadTicket(
            destination=destination,
            file_size_bytes=file_size_bytes,
            _part_futures=part_futures,
        )

    def cleanup(self, ticket: DownloadTicket) -> None:
        """Delete the downloaded file from disk.

        Must only be called after ticket.result() has returned or after
        confirming ticket.done is True. Calling cleanup while parts are
        still being written will cause download errors.
        """
        ticket.destination.unlink(missing_ok=True)

    def close(self) -> None:
        """Shut down the underlying downloader (pool + session)."""
        self._downloader.close()

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Exit context manager, shutting down the downloader."""
        self.close()
