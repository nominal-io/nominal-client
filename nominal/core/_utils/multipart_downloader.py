from __future__ import annotations

import dataclasses
import logging
import math
import multiprocessing
import pathlib
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from types import TracebackType
from typing import Callable, Iterable, Mapping, Sequence, Type

import requests
from typing_extensions import Self

from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.networking import create_multipart_request_session

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class PresignedURLProvider:
    """Thread-safe presigned URL cache that refreshes on schedule or when invalidated."""

    fetch_fn: Callable[[], str]
    """Function used to fetch a fresh presigned URL when the current one expires"""
    ttl_secs: float
    """Time-to-Live for the presigned URLs"""
    skew_secs: float
    """Buffer around TTL to ensure that URLs are still fresh by the time they are used"""

    # Pair of url + deadline, where the deadline is the latest monotonic clock time we consider the URL valid for
    _stamped_url: tuple[str, float] | None = dataclasses.field(default=None, repr=False)
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock, repr=False)

    def get_url(self, *, force: bool = False) -> str:
        now = time.monotonic()
        with self._lock:
            if force or self._stamped_url is None or now >= self._stamped_url[1]:
                url = self.fetch_fn()
                deadline = now + max(0.0, self.ttl_secs - self.skew_secs)
                self._stamped_url = (url, deadline)
                logger.debug("Refreshed presigned url with deadline of %f ('%s')", deadline, url)

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
class _DataChunkBounds:
    """Internal dataclass for representing the byte boundaries of a chunk of data."""

    index: int
    start_bytes: int
    end_bytes: int


@dataclass(frozen=True)
class _PlannedDownload:
    """Internal dataclass for representing the state of a file to download"""

    item: DownloadItem
    total_size: int
    etag: str | None

    def ranges(self) -> Iterable[_DataChunkBounds]:
        parts = max(1, math.ceil(self.total_size / self.item.part_size))
        for i in range(parts):
            start = i * self.item.part_size
            end = min(self.total_size - 1, start + self.item.part_size - 1)
            yield _DataChunkBounds(index=i, start_bytes=start, end_bytes=end)


@dataclass
class MultipartFileDownloader:
    """High-performance downloader for presigned S3 URLs using parallel ranged GETs.
    - Re-signs on demand when the URL expires.
    - Reuses a single HTTP session & thread pool.
    """

    max_workers: int
    timeout: float
    max_part_retries: int

    _session: requests.Session = field(repr=False)
    _pool: ThreadPoolExecutor = field(repr=False)
    _closed: bool = field(default=False, repr=False)

    @classmethod
    def create(cls, *, max_workers: int | None = None, timeout: float = 30.0, max_part_retries: int = 3) -> Self:
        """Factor for MultipartFileDownloader

        Args:
            max_workers: Maxmimum number of parallel threads to use.
                NOTE: defaults to the number of CPU cores
            timeout: Maximum amount of time before considering a connection dead
            max_part_retries: Maximum amount of retries to perform per part download (IO, presigned url expiry,
                4xx error, and source file changing mid download are all things that may cause a retry)

        Returns:
            Constructed MultipartFileDownloader prepared to begin downloading.
        """
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
            logger.info("Inferring core count as %d", max_workers)

        session = create_multipart_request_session(pool_size=max_workers)
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
        if item.destination in res.succeeded:
            return item.destination
        elif item.destination in res.failed:
            raise res.failed[item.destination]
        else:
            # Should technically be impossible...
            raise RuntimeError(f"Unknown error downloading to {item.destination}")

    def download_files(self, items: Sequence[DownloadItem]) -> DownloadResults:
        """Download many files using a shared thread pool."""
        failed: dict[pathlib.Path, Exception] = {}

        # Ensure destination directories exist
        logger.info("Validating destinations for download")
        for it in items:
            self._check_destination(it.destination)

        # Probe & preallocate files to generate a plan
        plans: list[_PlannedDownload] = []
        for it in items:
            if it.destination in failed:
                continue

            try:
                plan = self._plan_item(it)
                self._preallocate(it.destination, plan.total_size)
                plans.append(plan)
            except Exception as ex:
                failed[it.destination] = ex
                logger.error("Planning failed for %s: %s", it.destination, ex, exc_info=ex)

        if failed:
            logger.warning("Failed to plan downloads for %d files!", len(failed))

        # Execute plans with error collection
        logger.info("Starting downloads for %d files", len(plans))
        exec_failed = self._run_downloads(plans, collect_errors=True)
        succeeded = [p.item.destination for p in plans if p.item.destination not in failed]
        logger.info(
            "Successfully downloaded %d files (%d total, %d failed to plan, %d failed)",
            len(succeeded),
            len(items),
            len(failed),
            len(exec_failed),
        )
        failed.update(exec_failed)

        # Delete any failed file downloads
        if failed:
            logger.warning("Clearing out artifacts from %d failed file downloads", len(failed))
            for file in failed:
                if file.exists():
                    logger.info("Removing failed artifact %s", file)
                    file.unlink()

        return DownloadResults(succeeded, failed)

    def _run_downloads(
        self, plans: Sequence[_PlannedDownload], *, collect_errors: bool
    ) -> dict[pathlib.Path, Exception]:
        """Submit all parts for all plans, consume completions, and write to disk.


        If `collect_errors` is False, any failure is raised immediately.
        If True, errors are captured and returned in a map of destination->Exception.
        """
        # Build a map of futures to (destination, start)
        fut_map: dict[Future[None], tuple[pathlib.Path, int]] = {}
        for plan in plans:
            logger.info("Starting download for file %s (%.2f MB)", plan.item.destination, plan.total_size / 1e6)
            for data_chunk in plan.ranges():
                fut = self._pool.submit(
                    self._fetch_range_bytes,
                    plan.item.provider,
                    data_chunk.start_bytes,
                    data_chunk.end_bytes,
                    plan.etag,
                    plan.item.destination,
                )
                fut_map[fut] = (plan.item.destination, data_chunk.start_bytes)

        failed: dict[pathlib.Path, Exception] = {}
        for fut in as_completed(list(fut_map.keys())):
            dest, start = fut_map[fut]
            try:
                _ = fut.result()
            except Exception as ex:
                logger.error("Failed part for %s @%d: %s", dest, start, ex, exc_info=ex)

                # Cancel remaining futures for this destination to avoid wasted work
                for f, (d, _) in fut_map.items():
                    if d == dest:
                        f.cancel()

                if collect_errors:
                    failed[dest] = ex
                else:
                    raise ex

        return failed

    # ---- planning helpers ----

    def _head_or_probe(self, provider: PresignedURLProvider) -> tuple[int, str | None]:
        """Discover (total_size, etag). Refresh once if the current URL is stale.

        Within platforms that support ETag (notably, AWS), this will typically be some hash or metadata
        that can be used as a trivial check that the file being downloaded has not changed substantially.
        This ETag may not be present on all platforms, in which case, None will be provided and any subsequent
        checks will assume the file is not changing during downloads.
        """
        for attempt in range(3):
            url = provider.get_url(force=(attempt > 0))

            r = self._session.head(url, timeout=self.timeout)
            if r.ok and "Content-Length" in r.headers:
                return int(r.headers["Content-Length"]), r.headers.get("ETag")

            r = self._session.get(url, headers={"Range": "bytes=0-0"}, timeout=self.timeout)
            if r.ok:
                total = (
                    int(r.headers["Content-Range"].split("/")[-1])
                    if "Content-Range" in r.headers
                    else int(r.headers["Content-Length"])
                )
                return total, r.headers.get("ETag")

            if self._is_expired_status(r):
                provider.invalidate()
                continue

            r.raise_for_status()

        raise RuntimeError("Could not determine object size/ETag (presigned URL kept failing)")

    def _plan_item(self, item: DownloadItem) -> _PlannedDownload:
        total_size, etag = self._head_or_probe(item.provider)
        return _PlannedDownload(
            item=item,
            total_size=total_size,
            etag=etag,
        )

    # ---- IO helpers ----

    def _check_destination(self, path: pathlib.Path) -> None:
        logger.info("Preparing file destination %s", path)

        parent = path.parent
        if not parent.exists():
            raise FileNotFoundError(f"Output directory does not exist: {parent}")

        if path.exists():
            raise FileExistsError(f"Destination already exists: {path}")

    def _preallocate(self, path: pathlib.Path, total_size_bytes: int) -> None:
        logger.info("Preallocating %s to %f MB", path, total_size_bytes / 1e6)
        # Create file and open in read + write binary mode
        with path.open("wb") as f:
            f.truncate(total_size_bytes)

    @staticmethod
    def _write_part(path: pathlib.Path, start: int, data: bytes) -> None:
        # Open existing file in read + write binary mode
        # Not creating the file with `wb` as it is already pre-allocated before downloads begin
        with path.open("r+b") as f:
            f.seek(start)
            f.write(data)

    # ---- HTTP helpers ----

    def _is_expired_status(self, resp: requests.Response) -> bool:
        # Expired/invalid presigns typically yield 403; 400/401 also show up in some stacks
        return resp.status_code in (400, 401, 403)

    def _fetch_range_bytes(
        self,
        provider: PresignedURLProvider,
        start: int,
        end: int,
        expected_etag: str | None,
        destination: pathlib.Path,
    ) -> None:
        """Fetch a single range [start, end] inclusive with automatic re-sign on expiry-ish responses."""
        headers = {"Range": f"bytes={start}-{end}"}
        last_ex: Exception | None = None

        for _ in range(self.max_part_retries):
            url = provider.get_url()
            try:
                r = self._session.get(url, headers=headers, stream=True, timeout=self.timeout)
                if self._is_expired_status(r):
                    provider.invalidate()
                    continue  # refresh & retry
                r.raise_for_status()

                if expected_etag and r.headers.get("ETag") and r.headers["ETag"] != expected_etag:
                    raise RuntimeError("ETag mismatch across parts (object changed during download)")

                self._write_part(
                    destination,
                    start,
                    b"".join(chunk for chunk in r.iter_content(1024 * 1024) if chunk),
                )
                return

            except Exception as ex:
                last_ex = ex
                if isinstance(ex, requests.HTTPError) and 400 <= r.status_code < 500 and not self._is_expired_status(r):
                    break

        raise last_ex if last_ex else RuntimeError("Unknown error downloading range")
