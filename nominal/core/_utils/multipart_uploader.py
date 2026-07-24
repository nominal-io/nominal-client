from __future__ import annotations

import logging
import math
import pathlib
import random
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from types import TracebackType
from typing import Callable, Iterable, Sequence, Type

import requests
from nominal_api import upload_api
from typing_extensions import Self

from nominal.core._utils.filenames import validate_upload_filename
from nominal.core._utils.multipart import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_NUM_WORKERS,
    _abort,
    _complete_multipart_upload,
    _initiate_multipart_upload,
    _sign_and_put_part,
    path_upload_name,
)
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.core.filetype import FileType

logger = logging.getLogger(__name__)

# Files at or below the uploader's `small_file_route_max_bytes` are uploaded single-shot via the
# backend `upload_file` endpoint (one request) instead of the multipart flow. That endpoint holds
# a server request thread for the whole transfer, so we hard-cap the opt-in threshold well inside
# the "brief hold" zone to keep it safe under concurrency (large bodies belong on multipart, whose
# parts stream directly to S3). Experimental.
MAX_SMALL_FILE_ROUTE_BYTES = 1024 * 1024  # 1 MiB

# Adaptive-concurrency (AIMD) defaults.
_AIMD_DECREASE = 0.5  # multiplicative decrease on a throttle
_AIMD_COOLDOWN_S = 1.0  # debounce: at most one decrease per this window (one overload != many cuts)
_THROTTLE_MAX_RETRIES = 8  # per-file retry budget when the server is throttling
_THROTTLE_BACKOFF_BASE_S = 0.5
_THROTTLE_BACKOFF_CAP_S = 30.0


class _AdaptiveLimiter:
    """AIMD concurrency limiter (TCP-congestion-style).

    Gates concurrent operations at a dynamic `limit`: additive-increase on success,
    multiplicative-decrease on throttle, so it converges to whatever concurrency the current
    context (server rate limit, network, machine) actually allows and rides just under it.
    """

    def __init__(
        self,
        *,
        initial: float,
        min_limit: int,
        max_limit: int,
        decrease: float = _AIMD_DECREASE,
        cooldown: float = _AIMD_COOLDOWN_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._min = min_limit
        self._max = max_limit
        self._limit = float(max(min_limit, min(initial, max_limit)))
        self._decrease = decrease
        self._cooldown = cooldown
        self._clock = clock
        self._in_flight = 0
        self._last_decrease = float("-inf")
        self._cv = threading.Condition()

    def acquire(self) -> None:
        with self._cv:
            while self._in_flight >= int(self._limit):
                self._cv.wait()
            self._in_flight += 1

    def release(self) -> None:
        with self._cv:
            self._in_flight -= 1
            self._cv.notify()

    def on_success(self) -> None:
        with self._cv:
            if self._limit < self._max:
                self._limit = min(float(self._max), self._limit + 1.0 / self._limit)  # ~+1 per `limit` successes
                self._cv.notify()

    def on_throttle(self) -> None:
        with self._cv:
            now = self._clock()
            if now - self._last_decrease < self._cooldown:
                return  # one overload event throttles many files at once; only cut once per window
            self._last_decrease = now
            self._limit = max(float(self._min), self._limit * self._decrease)

    @property
    def limit(self) -> float:
        with self._cv:
            return self._limit


def _is_throttle_error(exc: BaseException) -> bool:
    """True if `exc` looks like server rate-limiting (a 429) or its retry-exhaustion."""
    if isinstance(exc, requests.exceptions.RetryError):  # conjure Retry exhausted on 429/503
        return True
    status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
    if status == 429:
        return True
    return "429" in str(exc)


def _throttle_backoff(attempt: int) -> float:
    """Exponential backoff with full jitter, capped — spreads retries so they don't re-herd."""
    ceiling = min(_THROTTLE_BACKOFF_CAP_S, _THROTTLE_BACKOFF_BASE_S * (2**attempt))
    return random.uniform(0.0, ceiling)


@dataclass(frozen=True)
class _PartBounds:
    """Byte range of one multipart part. `part_number` is 1-indexed (S3 requires 1..N)."""

    part_number: int
    offset: int
    size: int


@dataclass(frozen=True)
class _PlannedUpload:
    """A file whose upload has been initiated: object key, upload id, and part layout."""

    path: pathlib.Path
    key: str
    upload_id: str
    total_size: int
    part_size: int

    def parts(self) -> Iterable[_PartBounds]:
        # An empty file still yields exactly one (zero-byte) part so completion has a part to list.
        num_parts = max(1, math.ceil(self.total_size / self.part_size))
        for i in range(num_parts):
            offset = i * self.part_size
            size = min(self.part_size, self.total_size - offset)
            yield _PartBounds(part_number=i + 1, offset=offset, size=size)


class _FileUpload:
    """Owns one file's Future and settles it exactly once.

    `complete`/`abort` are injected so this stays pure coordination — no pool, no client.
    The owning uploader populates `part_futures` after construction, then wires each
    part-future's done-callback to `on_part_done`.
    """

    def __init__(
        self,
        future: "Future[str]",
        num_parts: int,
        complete: Callable[[Sequence[str | None]], str],
        abort: Callable[[BaseException], None],
    ) -> None:
        self.future = future
        self.part_futures: list[Future[str | None]] = []
        self._remaining = num_parts
        self._complete = complete
        self._abort = abort
        self._settled = False
        self._lock = threading.Lock()

    def on_part_done(self, fut: "Future[str | None]") -> None:
        # Decide the transition under the lock; run the (network) effect outside it.
        with self._lock:
            if self._settled:
                return  # absorbs cancelled/extra siblings after settling
            exc = fut.exception()
            if exc is None:
                self._remaining -= 1
                if self._remaining > 0:
                    return
                self._settled = True
                failed = False
            else:
                self._settled = True
                failed = True

        if failed:
            self._fail(exc)
        else:
            self._finish()

    def _finish(self) -> None:
        try:
            # All parts have succeeded here, so every result is available without blocking; the ETags
            # (part-number order) let complete skip list_parts for single-part uploads.
            part_etags = [f.result() for f in self.part_futures]
            self.future.set_result(self._complete(part_etags))
        except Exception as ce:  # completion itself failed
            self.future.set_exception(ce)
            self._safe_abort(ce)

    def _fail(self, exc: BaseException) -> None:
        for pf in self.part_futures:
            pf.cancel()
        self.future.set_exception(exc)
        self._safe_abort(exc)

    def _safe_abort(self, exc: BaseException) -> None:
        try:
            self._abort(exc)
        except Exception:
            logger.warning("best-effort multipart abort failed", exc_info=True)


@dataclass(frozen=True)
class _PendingUpload:
    """A file described and validated at enqueue time, before its upload is initiated."""

    path: pathlib.Path
    file_type: FileType
    name: str
    part_size: int
    total_size: int


@dataclass
class MultipartUploader:
    """Uploads many files to S3 over one shared thread pool + HTTP session.

    Enqueue files with `enqueue_file`; track completion via the returned futures. Each file's
    whole multipart lifecycle (initiate -> sign+PUT parts -> complete/abort) is collapsed behind
    a single `Future[str]` that resolves to the object's S3 location or raises.

    Invariant: no pool task ever blocks waiting on another pool task (that would deadlock the
    single bounded pool). initiate/part/complete/abort are non-blocking submissions or callbacks.

    `max_files_in_flight` (via `create`) caps how many files are uploading at once, applying
    backpressure at `enqueue_file` (on the caller thread, never a pool worker). Keeping it low
    with a high `max_workers` keeps the pool busy with part-uploads instead of bursting every
    file's metadata calls up front.

    `small_file_route_max_bytes` (EXPERIMENTAL) routes files at/below that size through the
    backend single-shot `upload_file` endpoint (one request) rather than multipart, collapsing
    the per-file metadata that rate-limits many-small-file batches. Larger files use multipart.
    """

    max_workers: int
    timeout: float
    max_part_retries: int

    _upload_client: upload_api.UploadService = field(repr=False)
    _auth_header: str = field(repr=False)
    _workspace_rid: str | None = field(repr=False)
    _session: requests.Session = field(repr=False)
    _pool: ThreadPoolExecutor = field(repr=False)
    _closed: bool = field(default=False, repr=False)
    # Bounds files-in-flight; acquired in enqueue_file, released when the file's future settles.
    _file_slots: threading.BoundedSemaphore | None = field(default=None, repr=False)
    # If set, files with size <= this go single-shot via upload_file instead of multipart.
    _small_file_route_max_bytes: int | None = field(default=None, repr=False)
    # AIMD files-in-flight limiter; when set (adaptive mode) it replaces the fixed _file_slots cap.
    _limiter: _AdaptiveLimiter | None = field(default=None, repr=False)

    @classmethod
    def create(
        cls,
        *,
        upload_client: upload_api.UploadService,
        auth_header: str,
        workspace_rid: str | None,
        max_workers: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        max_files_in_flight: int | None = None,
        small_file_route_max_bytes: int | None = None,
        adaptive_concurrency: bool = False,
        header_provider: HeaderProvider | None = None,
    ) -> Self:
        """Create a MultipartUploader.

        `small_file_route_max_bytes` (EXPERIMENTAL): if set, files whose size is <= this many
        bytes are uploaded single-shot via the backend `upload_file` endpoint (one request)
        instead of multipart — avoiding the per-file metadata burst that rate-limits many-small-
        file batches. Guarded to `MAX_SMALL_FILE_ROUTE_BYTES`; larger files use multipart.

        `adaptive_concurrency` (EXPERIMENTAL): replace the fixed files-in-flight cap with an AIMD
        limiter that discovers the context's real concurrency ceiling at runtime — growing on
        success, backing off on server throttling (429), and retrying throttled small-file uploads
        with jittered backoff so they complete rather than fail. `max_files_in_flight` (or
        `max_workers`) becomes the ceiling it won't exceed. Applies to the small-file route.
        """
        if max_workers is None:
            max_workers = DEFAULT_NUM_WORKERS
        if max_files_in_flight is not None and max_files_in_flight <= 0:
            raise ValueError(f"max_files_in_flight must be positive, got {max_files_in_flight}")
        if small_file_route_max_bytes is not None and not (0 < small_file_route_max_bytes <= MAX_SMALL_FILE_ROUTE_BYTES):
            raise ValueError(
                f"small_file_route_max_bytes must be in (0, {MAX_SMALL_FILE_ROUTE_BYTES}] bytes "
                f"(the single-shot upload endpoint holds a server thread per upload; route larger "
                f"files through multipart), got {small_file_route_max_bytes}"
            )
        session = create_multipart_request_session(pool_size=max_workers, header_provider=header_provider)
        pool = ThreadPoolExecutor(max_workers=max_workers)
        limiter: _AdaptiveLimiter | None = None
        file_slots: threading.BoundedSemaphore | None = None
        if adaptive_concurrency:
            ceiling = max_files_in_flight if max_files_in_flight is not None else max_workers
            limiter = _AdaptiveLimiter(initial=1, min_limit=1, max_limit=ceiling)
        elif max_files_in_flight is not None:
            file_slots = threading.BoundedSemaphore(max_files_in_flight)
        return cls(
            max_workers,
            timeout,
            max_part_retries,
            _upload_client=upload_client,
            _auth_header=auth_header,
            _workspace_rid=workspace_rid,
            _session=session,
            _pool=pool,
            _closed=False,
            _file_slots=file_slots,
            _small_file_route_max_bytes=small_file_route_max_bytes,
            _limiter=limiter,
        )

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

    def enqueue_file(
        self,
        path: pathlib.Path,
        *,
        file_type: FileType | None = None,
        name: str | None = None,
        part_size: int = DEFAULT_CHUNK_SIZE,
    ) -> "Future[str]":
        """Schedule a file upload and return a future for its S3 location.

        Obvious errors (missing file, invalid object name) surface here synchronously; upload
        failures surface via the returned future.

        Non-blocking, unless the uploader was created with `max_files_in_flight`: then this blocks
        until fewer than that many files are still uploading, so an unbounded list can be enqueued
        without opening every file's multipart upload (and bursting its metadata) at once.
        """
        file_type = file_type if file_type is not None else FileType.from_path(path)
        name = name if name is not None else path_upload_name(path, file_type)
        validate_upload_filename(name)
        total_size = path.stat().st_size  # raises FileNotFoundError synchronously if missing

        pending = _PendingUpload(path=path, file_type=file_type, name=name, part_size=part_size, total_size=total_size)
        gate = self._limiter or self._file_slots  # adaptive limiter takes precedence over the fixed cap
        if gate is not None:
            gate.acquire()  # backpressure: at most (adaptively-)bounded files open at once
        future: Future[str] = Future()
        if gate is not None:
            future.add_done_callback(self._on_file_settled)  # feed the limiter + free the slot on settle
        runner = (
            self._run_small_file_upload
            if self._small_file_route_max_bytes is not None and total_size <= self._small_file_route_max_bytes
            else self._run_upload
        )
        try:
            self._pool.submit(runner, pending, future)
        except BaseException:
            # Scheduling failed (e.g. pool already shut down): settle the future so its slot releases.
            if gate is not None and not future.done():
                future.cancel()
            raise
        return future

    def _on_file_settled(self, future: "Future[str]") -> None:
        # Feed the adaptive limiter from EVERY file's outcome (both the small-file and multipart
        # paths settle here), then always release the slot. Growing only on the small-file path
        # would leave the limit stuck at its initial value for multipart-only workloads.
        try:
            if self._limiter is not None and not future.cancelled():
                exc = future.exception()
                if exc is None:
                    self._limiter.on_success()
                elif _is_throttle_error(exc):
                    self._limiter.on_throttle()
        finally:
            gate = self._limiter or self._file_slots
            if gate is not None:
                gate.release()

    # ---- internals (run on pool threads) ----

    def _run_upload(self, pending: _PendingUpload, future: "Future[str]") -> None:
        try:
            safe_filename = f"{pending.name}{pending.file_type.extension}"
            key, upload_id = _initiate_multipart_upload(
                self._upload_client,
                self._auth_header,
                safe_filename,
                pending.file_type.mimetype,
                self._workspace_rid,
            )
            plan = _PlannedUpload(
                path=pending.path,
                key=key,
                upload_id=upload_id,
                total_size=pending.total_size,
                part_size=pending.part_size,
            )
            bounds = list(plan.parts())
            file_upload = _FileUpload(
                future=future,
                num_parts=len(bounds),
                complete=partial(self._complete_upload, key, upload_id),
                abort=partial(_abort, self._upload_client, self._auth_header, key, upload_id),
            )
            # Submit all parts first, THEN wire callbacks — so a failure's sibling-cancel sees
            # the full list and no part is submitted into an already-settled coordinator.
            for b in bounds:
                file_upload.part_futures.append(self._pool.submit(self._upload_part, plan, b))
            for pf in file_upload.part_futures:
                pf.add_done_callback(file_upload.on_part_done)
        except Exception as e:
            # Broad on purpose: any failure before the coordinator is wired MUST settle the future
            # here, or the enqueuer's future would hang forever (the pool swallows task exceptions).
            # An initiate failure has nothing to abort; a post-initiate failure here is only
            # reachable if the pool was shut down mid-enqueue (unsupported concurrent enqueue/close)
            # and may orphan the initiated upload — acceptable under the non-atomic failure model.
            if not future.done():
                future.set_exception(e)

    def _run_small_file_upload(self, pending: _PendingUpload, future: "Future[str]") -> None:
        """Single-shot upload of a small file via the backend `upload_file` endpoint (no multipart).

        One request instead of initiate + sign + PUT + list_parts + complete. Always passes
        `size_bytes` so the server streams to S3 (and doesn't silently cap at its in-memory limit).
        Reads the whole (small) file into memory.
        """
        try:
            safe_filename = f"{pending.name}{pending.file_type.extension}"
            body = pending.path.read_bytes()
            future.set_result(self._upload_file(safe_filename, body, pending.total_size))
        except Exception as e:  # settle the future so its slot releases and the caller sees the error
            if not future.done():
                future.set_exception(e)

    def _upload_file(self, file_name: str, body: bytes, size: int) -> str:
        """Call the backend upload_file endpoint once. In adaptive mode, retry a throttled upload
        with jittered backoff (holding this file's slot while it waits) so it completes rather than
        surfacing a 429 — signaling the limiter to shrink on each throttle. Success feedback is fed
        centrally in `_on_file_settled` (so both this path and multipart grow the limit).
        """
        attempt = 0
        while True:
            try:
                return self._upload_client.upload_file(
                    self._auth_header, body, file_name, size_bytes=size, workspace=self._workspace_rid
                )
            except Exception as e:
                if self._limiter is not None and _is_throttle_error(e) and attempt < _THROTTLE_MAX_RETRIES:
                    self._limiter.on_throttle()  # shrink promptly on each throttle during retries
                    time.sleep(_throttle_backoff(attempt))  # wait out the throttle (keeps holding the slot)
                    attempt += 1
                    continue
                raise

    def _upload_part(self, plan: _PlannedUpload, bounds: _PartBounds) -> str | None:
        with plan.path.open("rb") as f:
            f.seek(bounds.offset)
            data = f.read(bounds.size)
        response = _sign_and_put_part(
            self._upload_client,
            self._session,
            self._auth_header,
            plan.key,
            plan.upload_id,
            bounds.part_number,
            data,
            num_retries=self.max_part_retries,
            timeout=self.timeout,
        )
        # S3 returns the part's ETag on the PUT; hand it back so a single-part upload can complete
        # without a list_parts round-trip. None (header absent) makes complete fall back to list_parts.
        return response.headers.get("ETag")

    def _complete_upload(self, key: str, upload_id: str, part_etags: Sequence[str | None]) -> str:
        # Single part: complete straight from the PUT's ETag, skipping list_parts (a 25% cut in a
        # single-part file's control-plane calls). Multi-part, or a missing ETag: fall back to
        # list_parts as the authoritative source.
        etag = part_etags[0] if len(part_etags) == 1 else None
        known = [etag] if etag else None
        return _complete_multipart_upload(self._upload_client, self._auth_header, key, upload_id, known)
