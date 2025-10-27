# multipart_downloader.py
from __future__ import annotations

import logging
import math
import os
import pathlib
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from types import TracebackType
from typing import Callable, Iterable, Sequence, Tuple, Type

import requests
from requests.adapters import HTTPAdapter
from typing_extensions import Self
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class PresignedURLProvider:
    """Thread-safe presigned URL cache that refreshes on schedule or when invalidated.

    fetch_fn: callable to provide a fresh presigned URL
    ttl: seconds to reuse the cached URL
    skew: refresh earlier than TTL by this many seconds
    """

    def __init__(self, fetch_fn: Callable[[], str], ttl: float = 60.0, skew: float = 5.0) -> None:
        self._fetch_fn = fetch_fn
        self._ttl = ttl
        self._skew = skew
        self._url: str | None = None
        self._deadline: float = 0.0
        self._lock = threading.Lock()

    def get(self, *, force: bool = False) -> str:
        now = time.monotonic()
        with self._lock:
            if force or self._url is None or now >= self._deadline:
                self._url = self._fetch_fn()
                self._deadline = now + max(0.0, self._ttl - self._skew)
                logger.debug("Refreshed presigned url with %s second deadline ('%s')", self._deadline, self._url)
            return self._url

    def invalidate(self) -> None:
        with self._lock:
            logger.info("Invalidated presigned URL ('%s')", self._url)
            self._deadline = 0.0


@dataclass(frozen=True)
class DownloadItem:
    """Description of a single file download."""

    provider: PresignedURLProvider
    destination: pathlib.Path
    part_size: int = 64 * 1024 * 1024  # 64 MiB
    force: bool = False


@dataclass
class DownloadResults:
    """Outcome for multi-file downloads."""

    succeeded: list[pathlib.Path]
    failed: dict[pathlib.Path, Exception]


class MultipartFileDownloader:
    """High-performance downloader for presigned S3 URLs using parallel ranged GETs.
    - Re-signs on demand when the URL expires.
    - Reuses a single HTTP session & thread pool.
    """

    def __init__(
        self,
        *,
        max_workers: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
    ) -> None:
        """Initializer for MultipartFileDownloader

        Args:
            max_workers: Maxmimum number of parallel threads to use.
                NOTE: defaults to the number of CPU cores
            timeout: Maximum amount of time before considering a connection dead
            max_part_retries: Maximum amount of retries to perform per part download
        """
        if max_workers is None:
            core_count = os.cpu_count()
            if core_count is None:
                max_workers = 8
                logger.warning("Cannot infer number of CPU cores... using %d workers", max_workers)
            else:
                logger.info("Inferring core count as %d", core_count)
                max_workers = core_count

        self.max_workers = max_workers
        self.timeout = timeout
        self.max_part_retries = max_part_retries
        self._session = self._make_session(max_workers)
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._closed = False

    # ---- lifecycle ----

    def close(self) -> None:
        if not self._closed:
            try:
                self._pool.shutdown(wait=True, cancel_futures=True)
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
        self._prepare_destination(item.destination, force=item.force)
        total_size, etag = self._head_or_probe(item.provider)
        self._preallocate(item.destination, total_size)

        ranges = list(self._iter_ranges(total_size, item.part_size))

        logger.info("Starting download for file %s", item.destination)
        writer_lock = threading.Lock()
        fut_map = self._submit_parts(item.provider, ranges, etag)
        self._consume_and_write(item.destination, fut_map, writer_lock)
        return item.destination

    def download_files(self, items: Sequence[DownloadItem]) -> DownloadResults:
        """Download many files using a shared thread pool."""
        failed: dict[pathlib.Path, Exception] = {}
        plans: dict[pathlib.Path, Tuple[int, str | None, int]] = {}

        # Prepare destinations
        for it in items:
            try:
                self._prepare_destination(it.destination, force=it.force)
            except Exception as ex:
                failed[it.destination] = ex
                logger.error("Destination prep failed for %s: %s", it.destination, ex, exc_info=ex)

        # Probe & preallocate
        for it in items:
            if it.destination in failed:
                continue
            try:
                total, etag = self._head_or_probe(it.provider)
                self._preallocate(it.destination, total)
                plans[it.destination] = (total, etag, it.part_size)
            except Exception as ex:
                failed[it.destination] = ex
                logger.error("Planning failed for %s: %s", it.destination, ex, exc_info=ex)

        # Submit all parts for all planned files
        writer_locks: dict[pathlib.Path, threading.Lock] = {d: threading.Lock() for d in plans}
        fut_map: dict[Future[bytes], Tuple[pathlib.Path, int]] = {}

        for it in items:
            plan = plans.get(it.destination)
            if not plan:
                continue

            logger.info("Scheduling download for file %s", it.destination)
            total, etag, part_size = plan
            for _, start, end in self._iter_ranges(total, part_size):
                fut = self._pool.submit(self._fetch_range_bytes, it.provider, start, end, etag)
                fut_map[fut] = (it.destination, start)

        # Consume completions
        succeeded: set[pathlib.Path] = set()
        for fut in as_completed(list(fut_map.keys())):
            dest, start = fut_map[fut]
            try:
                data = fut.result()
                self._write_part(dest, start, data, writer_locks[dest])
                succeeded.add(dest)
            except Exception as ex:
                # First error wins for that file; additional part errors will overwrite but same effect
                failed[dest] = ex
                logger.error("Failed part for %s @%d: %s", dest, start, ex, exc_info=ex)

        # Only count as success if it planned and never failed
        ok = [d for d in plans.keys() if d in succeeded and d not in failed]
        return DownloadResults(succeeded=ok, failed=failed)

    # ---- planning helpers ----

    def _make_session(self, pool_size: int) -> requests.Session:
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=pool_size)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def _head_or_probe(self, provider: PresignedURLProvider) -> Tuple[int, str | None]:
        """Discover (total_size, etag). Refresh once if the current URL is stale."""
        for attempt in range(2):
            url = provider.get(force=(attempt > 0))

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

    def _iter_ranges(self, total_size: int, part_size: int) -> Iterable[Tuple[int, int, int]]:
        """Yield (index, start, end) inclusive ranges."""
        parts = max(1, math.ceil(total_size / part_size))
        for i in range(parts):
            start = i * part_size
            end = min(total_size - 1, start + part_size - 1)
            yield (i, start, end)

    # ---- IO helpers ----

    def _prepare_destination(self, path: pathlib.Path, *, force: bool) -> None:
        logger.info("Preparing file destination %s (force=%s)", path, force)

        parent = path.parent
        if not parent.exists():
            if force:
                parent.mkdir(parents=True, exist_ok=True)
            else:
                raise FileNotFoundError(f"Output directory does not exist and force=False: {parent}")

        if path.exists():
            if path.is_dir():
                raise ValueError(f"Destination exists as a directory: {path}")
            if force:
                path.unlink()
            else:
                raise FileExistsError(f"Destination exists and force=False: {path}")

    def _preallocate(self, path: pathlib.Path, total_size: int) -> None:
        logger.info("Preallocating %s to %f MB", path, total_size / 1e6)
        with path.open("wb") as f:
            f.truncate(total_size)

    def _write_part(self, path: pathlib.Path, start: int, data: bytes, lock: threading.Lock | None) -> None:
        if lock:
            lock.acquire()
        try:
            with path.open("r+b") as f:
                f.seek(start)
                f.write(data)
        finally:
            if lock:
                lock.release()

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
    ) -> bytes:
        """Fetch a single range [start, end] inclusive with automatic re-sign on expiry-ish responses."""
        headers = {"Range": f"bytes={start}-{end}"}
        last_ex: Exception | None = None

        for _ in range(self.max_part_retries):
            url = provider.get()
            try:
                r = self._session.get(url, headers=headers, stream=True, timeout=self.timeout)
                if self._is_expired_status(r):
                    provider.invalidate()
                    continue  # refresh & retry
                r.raise_for_status()

                if expected_etag and r.headers.get("ETag") and r.headers["ETag"] != expected_etag:
                    raise RuntimeError("ETag mismatch across parts (object changed during download)")

                return b"".join(chunk for chunk in r.iter_content(1024 * 1024) if chunk)

            except Exception as ex:
                last_ex = ex
                if isinstance(ex, requests.HTTPError) and 400 <= r.status_code < 500 and not self._is_expired_status(r):
                    break

        raise last_ex if last_ex else RuntimeError("Unknown error downloading range")

    # ---- scheduling ----

    def _submit_parts(
        self,
        provider: PresignedURLProvider,
        ranges: Sequence[Tuple[int, int, int]],
        etag: str | None,
    ) -> dict[Future[bytes], Tuple[int, int]]:
        """Submit range fetches to the pool. Returns a map for completion handling."""
        fut_map: dict[Future[bytes], Tuple[int, int]] = {}
        for _idx, start, end in ranges:
            fut = self._pool.submit(self._fetch_range_bytes, provider, start, end, etag)
            fut_map[fut] = (start, end)
        return fut_map

    def _consume_and_write(
        self,
        destination: pathlib.Path,
        fut_map: dict[Future[bytes], Tuple[int, int]],
        writer_lock: threading.Lock,
    ) -> None:
        for fut in as_completed(list(fut_map.keys())):
            start, _ = fut_map[fut]
            data = fut.result()
            self._write_part(destination, start, data, writer_lock)
