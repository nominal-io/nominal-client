from __future__ import annotations

import logging
import multiprocessing
import os
import pathlib
import re
import threading
import time
import traceback
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from types import TracebackType
from typing import Callable, Iterable, Mapping, Self, Sequence, Type

import requests

from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.exceptions import NominalDownloadRangeError

logger = logging.getLogger(__name__)


def _parse_content_range(value: str) -> tuple[int, int, int | None]:
    """Parse a single HTTP byte range, whose representation length may be unknown."""
    match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/(\*|[0-9]+)", value, re.IGNORECASE)
    if match is None:
        raise NominalDownloadRangeError(f"Invalid Content-Range: {value!r}")
    start, end, total = match.groups()
    return int(start), int(end), None if total == "*" else int(total)


@dataclass
class PresignedURLProvider:
    """Thread-safe presigned URL cache that refreshes on schedule or when invalidated."""

    fetch_fn: Callable[[], str]
    """Function used to fetch a fresh presigned URL when the current one expires"""
    ttl_secs: float
    """Time-to-Live for the presigned URLs"""
    skew_secs: float
    """Buffer around TTL to ensure that URLs are still fresh by the time they are used"""

    # Pair of url + deadline, where the deadline is the latest monotonic clock time we consider the URL valid for
    _stamped_url: tuple[str, float] | None = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def get_url(self, *, force: bool = False) -> str:
        now = time.monotonic()
        with self._lock:
            if force or self._stamped_url is None or now >= self._stamped_url[1]:
                url = self.fetch_fn()
                deadline = now + max(0.0, self.ttl_secs - self.skew_secs)
                self._stamped_url = (url, deadline)
                logger.debug("Refreshed presigned URL with deadline %f", deadline)

            return self._stamped_url[0]

    def invalidate(self) -> None:
        with self._lock:
            logger.info("Invalidating presigned URL")
            self._stamped_url = None


@dataclass(frozen=True)
class DownloadItem:
    """Description of a single file download."""

    provider: PresignedURLProvider
    destination: pathlib.Path
    part_size: int = DEFAULT_CHUNK_SIZE


@dataclass
class DownloadResults:
    """Outcome for multi-file downloads."""

    succeeded: Sequence[pathlib.Path]
    failed: Mapping[pathlib.Path, Exception]


@dataclass(frozen=True)
class _PlannedDownload:
    item: DownloadItem
    total_size: int
    etag: str | None
    identity: os.stat_result

    @classmethod
    def create(cls, item: DownloadItem, size: int, etag: str | None) -> Self:
        # Exclusive creation also protects against a competing writer after preflight.
        output = item.destination.open("xb")
        with output:
            plan = cls(item, size, etag, os.fstat(output.fileno()))
            try:
                output.truncate(size)
            except BaseException:
                output.close()
                plan.remove_partial()
                raise
        return plan

    def owns_destination(self) -> bool:
        try:
            return os.path.samestat(self.identity, self.item.destination.lstat())
        except FileNotFoundError:
            return False

    def remove_partial(self) -> None:
        if self.owns_destination():
            self.item.destination.unlink(missing_ok=True)

    def write(self, start: int, data: bytearray) -> None:
        path = self.item.destination
        with path.open("r+b") as output:
            if not os.path.samestat(self.identity, os.fstat(output.fileno())):
                raise FileExistsError(f"Destination was replaced during download: {path}")
            output.seek(start)
            written = output.write(data)
            if written != len(data):
                raise OSError(f"Short write to {path} at offset {start}: wrote {written}/{len(data)} bytes")


@dataclass
class MultipartFileDownloader:
    """High-performance downloader for presigned S3 URLs using parallel ranged GETs.
    - Re-signs on demand when the URL expires.
    - Reuses a single HTTP session & thread pool.

    Downloads preserve stored bytes without decoding Content-Encoding, because range offsets
    and lengths refer to the encoded representation.
    """

    max_workers: int
    timeout: float
    max_part_retries: int

    _session: requests.Session = field(repr=False)
    _pool: ThreadPoolExecutor = field(repr=False)
    _closed: bool = field(default=False, repr=False)

    @classmethod
    def create(
        cls,
        *,
        max_workers: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        header_provider: HeaderProvider | None = None,
    ) -> Self:
        """Create a downloader for single and batched transfers.

        Args:
            max_workers: Maximum number of parallel threads to use.

                **Note:** Defaults to the number of CPU cores.
            timeout: Maximum amount of time before considering a connection dead
            max_part_retries: Maximum amount of retries to perform per part download (IO, presigned url expiry,
                4xx error, and source file changing mid download are all things that may cause a retry)
            header_provider: Additional headers to attach to every request issued by the session.

        Returns:
            Constructed MultipartFileDownloader prepared to begin downloading.
        """
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
            logger.info("Inferring core count as %d", max_workers)

        session = create_multipart_request_session(pool_size=max_workers, header_provider=header_provider)
        pool = ThreadPoolExecutor(max_workers=max_workers)
        return cls(max_workers, timeout, max_part_retries, _session=session, _pool=pool, _closed=False)

    # ---- lifecycle ----

    def close(self) -> None:
        if not self._closed:
            try:
                self._pool.shutdown(wait=True)
            finally:
                self._session.close()
                self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    # ---- public API ----

    def download_file(self, item: DownloadItem) -> pathlib.Path:
        """Download a single file using a presigned URL provider."""
        res = self.download_files([item])
        if item.destination in res.failed:
            raise res.failed[item.destination]
        return item.destination

    def download_files(self, items: Sequence[DownloadItem]) -> DownloadResults:
        """Download files concurrently, reporting per-file transfer failures.

        Every destination must be distinct and available. An empty object creates an
        empty file. A failed transfer removes its partially written file after all
        workers writing that file have settled.
        """
        if self._closed:
            raise RuntimeError("downloader is closed")
        destinations: set[pathlib.Path] = set()
        for item in items:
            if item.part_size <= 0:
                raise ValueError("part_size must be positive")
            path = item.destination.resolve()
            if path in destinations:
                raise FileExistsError(f"Duplicate download destination: {item.destination}")
            destinations.add(path)
            self._check_destination(item.destination)

        plans: list[_PlannedDownload] = []
        failed: dict[pathlib.Path, Exception] = {}
        try:
            for item in items:
                try:
                    size, etag = self._head_or_probe(item.provider)
                    plans.append(_PlannedDownload.create(item, size, etag))
                except Exception as exc:
                    failed[item.destination] = exc
                    logger.error("Planning failed for %s", item.destination, exc_info=exc)
            self._run_downloads(plans, failed)
        except BaseException:
            for plan in plans:
                plan.remove_partial()
            raise

        succeeded = []
        for plan in plans:
            path = plan.item.destination
            if path in failed:
                plan.remove_partial()
            elif plan.owns_destination():
                succeeded.append(path)
            else:
                failed[path] = FileExistsError(f"Destination was replaced during download: {path}")
        return DownloadResults(succeeded, failed)

    def _run_downloads(self, plans: Sequence[_PlannedDownload], failed: dict[pathlib.Path, Exception]) -> None:
        """Schedule ranges round-robin so a large file cannot queue ahead of an entire batch."""
        ready = deque((plan, 0) for plan in plans if plan.total_size)
        pending: dict[Future[None], pathlib.Path] = {}
        try:
            while ready or pending:
                while ready and len(pending) < self.max_workers:
                    plan, start = ready.popleft()
                    path = plan.item.destination
                    if path in failed:
                        continue
                    future = self._pool.submit(self._fetch_range_bytes, plan, start)
                    pending[future] = path
                    next_start = start + plan.item.part_size
                    if next_start < plan.total_size:
                        ready.append((plan, next_start))

                if pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        path = pending.pop(future)
                        error = future.exception()
                        if error is None:
                            continue
                        if not isinstance(error, Exception):
                            raise error
                        # A retained error must not pin a whole range in a worker's locals.
                        if error.__traceback__ is not None:
                            traceback.clear_frames(error.__traceback__)
                        failed.setdefault(path, error)
        finally:
            # An interruption or a rejected submission must drain active writers before
            # download_files removes any of their output files.
            for future in pending:
                future.cancel()
            wait(pending)

    # ---- planning helpers ----

    def _head_or_probe(self, provider: PresignedURLProvider) -> tuple[int, str | None]:
        """Discover the stored size and optional ETag, refreshing an expired URL when necessary."""
        for attempt in range(3):
            url = provider.get_url(force=(attempt > 0))

            with self._session.head(url, timeout=self.timeout) as response:
                if response.status_code == 200 and "Content-Length" in response.headers:
                    return int(response.headers["Content-Length"]), response.headers.get("ETag")

            # A server may ignore Range and return the whole object. Only its headers
            # are needed for this probe; streaming avoids buffering that body.
            with self._session.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=self.timeout) as response:
                content_range = response.headers.get("Content-Range", "")
                if response.status_code == 206:
                    start, end, total = _parse_content_range(content_range)
                    if start != 0 or end != 0 or total is None or total <= 0:
                        raise NominalDownloadRangeError(f"Invalid Content-Range in size probe: {content_range!r}")
                    return total, response.headers.get("ETag")
                if response.status_code == 200:
                    return int(response.headers["Content-Length"]), response.headers.get("ETag")
                if response.status_code == 416 and content_range == "bytes */0":
                    return 0, response.headers.get("ETag")
                if self._is_expired_status(response) and attempt < 2:
                    provider.invalidate()
                    continue
                response.raise_for_status()
                raise NominalDownloadRangeError(f"Unexpected HTTP {response.status_code} in size probe")

        raise RuntimeError("Could not determine object size")

    # ---- IO helpers ----

    def _check_destination(self, path: pathlib.Path) -> None:
        logger.info("Preparing file destination %s", path)

        parent = path.parent
        if not parent.exists():
            raise FileNotFoundError(f"Output directory does not exist: {parent}")

        if path.exists():
            raise FileExistsError(f"Destination already exists: {path}")

    @staticmethod
    def _read_part(start: int, end: int, chunks: Iterable[bytes]) -> bytearray:
        """Read exactly one part, stopping oversized streams before writing to the destination."""
        expected = end - start + 1
        data = bytearray()
        for chunk in chunks:
            data += chunk
            if len(data) > expected:
                # A server that ignores Range answers with the whole object. Writing that at
                # `start` would corrupt the file, so stop reading rather than buffer it all.
                raise NominalDownloadRangeError(
                    f"Expected {expected} bytes for range {start}-{end}, got more; "
                    "the server may not support ranged downloads"
                )
        if len(data) != expected:
            raise RuntimeError(f"Expected {expected} bytes for range {start}-{end}, got {len(data)}")
        return data

    # ---- HTTP helpers ----

    def _is_expired_status(self, resp: requests.Response) -> bool:
        # Expired/invalid presigns typically yield 403; 400/401 also show up in some stacks
        return resp.status_code in (400, 401, 403)

    def _fetch_range_bytes(self, plan: _PlannedDownload, start: int) -> None:
        """Fetch the planned range and verify its identity before writing its bytes."""
        end = min(plan.total_size, start + plan.item.part_size) - 1
        provider = plan.item.provider
        headers = {"Range": f"bytes={start}-{end}"}

        for attempt in range(self.max_part_retries):
            url = provider.get_url()
            try:
                with self._session.get(url, headers=headers, stream=True, timeout=self.timeout) as r:
                    if self._is_expired_status(r) and attempt + 1 < self.max_part_retries:
                        provider.invalidate()
                        continue  # refresh & retry
                    r.raise_for_status()

                    if plan.etag and r.headers.get("ETag") and r.headers["ETag"] != plan.etag:
                        raise RuntimeError("ETag mismatch across parts (object changed during download)")

                    if r.status_code == 206:
                        actual = r.headers.get("Content-Range", "")
                        first, last, total = _parse_content_range(actual)
                        if (first, last) != (start, end) or (total is not None and total != plan.total_size):
                            raise NominalDownloadRangeError(
                                f"Expected Content-Range bytes {start}-{end}/{plan.total_size}, got {actual!r}"
                            )
                    elif r.status_code != 200 or start != 0 or end + 1 != plan.total_size:
                        raise NominalDownloadRangeError(
                            f"Unexpected HTTP {r.status_code} for range {start}-{end}; "
                            "the server may not support ranged downloads"
                        )

                    # Ranges address encoded bytes; iter_content would decompress them.
                    data = self._read_part(start, end, r.raw.stream(1024 * 1024, decode_content=False))
            except NominalDownloadRangeError:
                raise  # retrying an invalid range would fetch the same wrong bytes
            except Exception as exc:
                if attempt + 1 == self.max_part_retries or (
                    isinstance(exc, requests.HTTPError)
                    and exc.response is not None
                    and 400 <= exc.response.status_code < 500
                    and not self._is_expired_status(exc.response)
                ):
                    raise
                continue

            # A disk failure is local. Refetching the same range cannot repair it.
            plan.write(start, data)
            return

        raise ValueError("max_part_retries must be positive")
