from __future__ import annotations

import dataclasses
import logging
import math
import multiprocessing
import pathlib
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Callable, Iterable, Mapping, Sequence, Type

import requests
from typing_extensions import Self

from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session

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
    def create(
        cls,
        *,
        max_workers: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        header_provider: HeaderProvider | None = None,
    ) -> Self:
        """Factor for MultipartFileDownloader

        Args:
            max_workers: Maxmimum number of parallel threads to use.
                NOTE: defaults to the number of CPU cores
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
        if item.destination in res.succeeded:
            return item.destination
        elif item.destination in res.failed:
            raise res.failed[item.destination]
        else:
            # Should technically be impossible...
            raise RuntimeError(f"Unknown error downloading to {item.destination}")

    def download_files(self, items: Sequence[DownloadItem]) -> DownloadResults:
        """Download many files using a shared thread pool.

        Files that fail (either during planning or execution) are recorded in DownloadResults.failed
        and any partially-written artifacts are deleted from disk. Successfully downloaded files are
        recorded in DownloadResults.succeeded.
        """
        plan_failures: dict[pathlib.Path, Exception] = {}

        # Ensure destination directories exist
        logger.info("Validating destinations for download")
        for it in items:
            self._check_destination(it.destination)

        # Probe & preallocate files to generate a plan
        plans: list[_PlannedDownload] = []
        for it in items:
            if it.destination in plan_failures:
                continue

            try:
                plan = self._plan_item(it)
                self._preallocate(it.destination, plan.total_size)
                plans.append(plan)
            except Exception as ex:
                plan_failures[it.destination] = ex
                logger.error("Planning failed for %s", it.destination, exc_info=ex)

        if plan_failures:
            logger.warning("Failed to plan downloads for %d files!", len(plan_failures))

        # Execute plans with error collection
        logger.info("Starting downloads for %d files", len(plans))
        exec_failures = self._run_downloads(plans, collect_errors=True)

        # Partition items broadly into failures vs. successes
        all_failures = {**plan_failures, **exec_failures}
        all_successes = [p.item.destination for p in plans if p.item.destination not in all_failures]
        logger.info(
            "Successfully downloaded %d files (%d total, %d failed to plan, %d failed)",
            len(all_successes),
            len(items),
            len(plan_failures),
            len(exec_failures),
        )

        # Delete any failed file downloads
        if all_failures:
            logger.warning("Clearing out artifacts from %d failed file downloads", len(all_failures))
            for file in all_failures:
                if file.exists():
                    logger.info("Removing failed artifact %s", file)
                    file.unlink()

        return DownloadResults(all_successes, all_failures)

    def download_files_pipelined(
        self,
        items: Sequence[DownloadItem],
        *,
        on_file_planned: Callable[[pathlib.Path], None] | None = None,
        on_file_complete: Callable[[pathlib.Path], None] | None = None,
    ) -> DownloadResults:
        """Download many files, pipelining link-generation with downloads.

        Unlike :meth:`download_files` (which fetches every presigned link sequentially before any
        download starts), this starts a file's byte downloads as soon as its own link is fetched,
        size is probed, and the file is preallocated. Planning runs on a *dedicated* pool while part
        downloads run on the shared download pool, so the two never contend for the same FIFO queue:
        a file begins downloading immediately while other links are still being generated, and
        per-part parallelism is preserved.

        Args:
            items: The files to download.
            on_file_planned: Optional callback invoked with each destination once its presigned link
                is fetched, size probed, and the file preallocated (i.e. it is ready to download).
            on_file_complete: Optional callback invoked with each destination as soon as that file
                finishes downloading successfully. Both callbacks are invoked from the calling thread
                (not a worker), so they are serialized and safe to use to drive progress bars.

        Returns:
            A :class:`DownloadResults` partitioning destinations into succeeded and failed.
            Files that fail (destination validation, link/size planning, or any part download)
            are recorded in ``failed`` and their partial artifacts deleted, mirroring
            :meth:`download_files`.
        """
        failures: dict[pathlib.Path, Exception] = {}

        # A dedicated planning pool keeps the slow, server-side link-generation + preallocation off
        # the download pool's FIFO queue, so part downloads are never stuck behind pending plans.
        # Futures are tracked as Future[Any] because planning and part-download futures share one
        # pending set and are dispatched by membership in plan_futs.
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="presign-plan") as plan_pool:
            plan_futs: dict[Future[Any], DownloadItem] = {}
            for it in items:
                try:
                    self._check_destination(it.destination)
                except Exception as ex:
                    failures[it.destination] = ex
                    logger.error("Invalid destination %s", it.destination, exc_info=ex)
                    continue
                plan_futs[plan_pool.submit(self._plan_and_preallocate, it)] = it

            # Reactively drive a growing set of futures: planning futures resolve into per-file part
            # futures (submitted to the download pool), which we add back into the pending set.
            part_futs: dict[Future[Any], tuple[pathlib.Path, int]] = {}
            remaining_parts: dict[pathlib.Path, int] = {}
            succeeded: list[pathlib.Path] = []
            pending: set[Future[Any]] = set(plan_futs)

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    if fut in plan_futs:
                        self._handle_plan_complete(
                            fut, plan_futs, part_futs, remaining_parts, failures, pending, on_file_planned
                        )
                    else:
                        self._handle_part_complete(
                            fut, part_futs, remaining_parts, failures, succeeded, on_file_complete
                        )

        logger.info("Successfully downloaded %d files (%d total, %d failed)", len(succeeded), len(items), len(failures))
        self._cleanup_failed_artifacts(failures)
        return DownloadResults(succeeded, failures)

    def _plan_and_preallocate(self, item: DownloadItem) -> _PlannedDownload:
        """Fetch the presigned link, probe size/etag, and preallocate the destination file.

        Runs entirely on the planning pool so link generation and preallocation are threaded and do
        not block the calling thread or the download pool.
        """
        plan = self._plan_item(item)
        self._preallocate(item.destination, plan.total_size)
        return plan

    def _handle_plan_complete(
        self,
        fut: Future[Any],
        plan_futs: dict[Future[Any], DownloadItem],
        part_futs: dict[Future[Any], tuple[pathlib.Path, int]],
        remaining_parts: dict[pathlib.Path, int],
        failures: dict[pathlib.Path, Exception],
        pending: set[Future[Any]],
        on_file_planned: Callable[[pathlib.Path], None] | None,
    ) -> None:
        """Resolve a completed planning future and submit the file's part-download futures."""
        item = plan_futs.pop(fut)
        try:
            plan = fut.result()  # link + size probe + preallocation already done on the planning pool
        except Exception as ex:
            failures[item.destination] = ex
            logger.error("Planning failed for %s", item.destination, exc_info=ex)
            return

        if on_file_planned is not None:
            on_file_planned(item.destination)

        chunk_bounds = list(plan.ranges())
        remaining_parts[item.destination] = len(chunk_bounds)
        for data_chunk in chunk_bounds:
            part_fut = self._pool.submit(
                self._fetch_range_bytes,
                plan.item.provider,
                data_chunk.start_bytes,
                data_chunk.end_bytes,
                plan.etag,
                item.destination,
            )
            part_futs[part_fut] = (item.destination, data_chunk.start_bytes)
            pending.add(part_fut)

    def _handle_part_complete(
        self,
        fut: Future[Any],
        part_futs: dict[Future[Any], tuple[pathlib.Path, int]],
        remaining_parts: dict[pathlib.Path, int],
        failures: dict[pathlib.Path, Exception],
        succeeded: list[pathlib.Path],
        on_file_complete: Callable[[pathlib.Path], None] | None,
    ) -> None:
        """Resolve a completed part-download future, firing on_file_complete on the last part."""
        dest, start = part_futs.pop(fut)
        # A prior part for this destination already failed (and cancelled the rest); ignore.
        if dest in failures:
            return
        try:
            fut.result()
        except Exception as ex:
            logger.error("Failed part for %s @%d", dest, start, exc_info=ex)
            failures[dest] = ex
            # Cancel any not-yet-started parts for this destination to avoid wasted work.
            for part_fut, (other_dest, _) in part_futs.items():
                if other_dest == dest:
                    part_fut.cancel()
            return

        remaining_parts[dest] -= 1
        if remaining_parts[dest] == 0:
            succeeded.append(dest)
            logger.debug("Completed download for %s", dest)
            if on_file_complete is not None:
                on_file_complete(dest)

    def _cleanup_failed_artifacts(self, failures: Mapping[pathlib.Path, Exception]) -> None:
        """Delete partially-written files for any failed downloads."""
        if not failures:
            return
        logger.warning("Clearing out artifacts from %d failed file downloads", len(failures))
        for file in failures:
            if file.exists():
                logger.info("Removing failed artifact %s", file)
                file.unlink()

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
                logger.error("Failed part for %s @%d", dest, start, exc_info=ex)

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
        logger.debug("Preparing file destination %s", path)

        parent = path.parent
        if not parent.exists():
            raise FileNotFoundError(f"Output directory does not exist: {parent}")

        if path.exists():
            raise FileExistsError(f"Destination already exists: {path}")

    def _preallocate(self, path: pathlib.Path, total_size_bytes: int) -> None:
        logger.debug("Preallocating %s to %f MB", path, total_size_bytes / 1e6)
        # Create file and open in read + write binary mode
        with path.open("wb") as f:
            f.truncate(total_size_bytes)

    @staticmethod
    def _write_part(path: pathlib.Path, start: int, data: bytes) -> None:
        # Open existing file in read + write binary mode
        # Not creating the file with `wb` as it is already pre-allocated before downloads begin
        with path.open("r+b") as f:
            f.seek(start)
            written = f.write(data)
            if written != len(data):
                raise OSError(
                    f"Short write to {path} at offset {start}: wrote {written}/{len(data)} bytes. "
                    f"This may indicate disk full, permission issues, or filesystem errors."
                )

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
                if (
                    isinstance(ex, requests.HTTPError)
                    and ex.response is not None
                    and 400 <= ex.response.status_code < 500
                    and not self._is_expired_status(ex.response)
                ):
                    break

        raise last_ex if last_ex else RuntimeError("Unknown error downloading range")
