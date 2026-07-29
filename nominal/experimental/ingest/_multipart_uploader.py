"""Driver-per-file multipart uploader for the experimental ingest path.

Three pools with one paced admission gate between them and the Nominal API. Each multipart file
is one sequential driver function rather than a graph of callbacks, which is what makes the
concurrency auditable: see `MultipartUploader` for the wait DAG.
"""

from __future__ import annotations

import logging
import math
import pathlib
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, CancelledError, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Iterable, Type

import requests
from nominal_api import upload_api
from typing_extensions import Self

from nominal.core._utils.filenames import validate_upload_filename
from nominal.core._utils.multipart import (
    DEFAULT_CHUNK_SIZE,
    _abort,
    _complete_multipart_upload,
    _initiate_multipart_upload,
    _put_part,
    _sign_part,
    _wrap_multipart_retry_exception,
    path_upload_name,
)
from nominal.core._utils.networking import create_multipart_request_session
from nominal.core.exceptions import (
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalRequestThrottledError,
)
from nominal.core.filetype import FileType
from nominal.experimental.ingest._upload_pacing import (
    _ABORT_THROTTLE_DEADLINE_S,
    DEFAULT_MAX_BACKOFF_DURATION_S,
    DEFAULT_THROTTLE_DEADLINE_S,
    NOMINAL_MAX_CONCURRENCY,
    _GlobalBackoff,
    _ThrottleGate,
)

if TYPE_CHECKING:
    from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

# DEFAULT single-shot threshold: files at or below this size are uploaded in one request via the
# backend `upload_file` endpoint instead of the multipart flow (whose minimum cost is three
# requests). Sits comfortably inside the hard ceiling below, so callers have headroom to raise it.
DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES = 1024 * 1024  # 1 MiB

# Hard CEILING on the `small_file_route_max_bytes` knob (not the default — that is above). The
# single-shot endpoint is disproportionately expensive server-side for large transfers, and
# measured throughput of the two routes reaches parity around this size, above which multipart is
# strictly better (its bytes stream directly to storage and its parts parallelize). Experimental.
MAX_SMALL_FILE_ROUTE_BYTES = 4 * 1024 * 1024  # 4 MiB

# Benchmark-tuned default for concurrent direct-to-storage PUT streams. Aggregate throughput
# rises with stream count only until the network path saturates, and on some uplinks pushing
# past saturation actively degrades every stream, so more is not reliably faster. Raise this
# when the path to storage is measured to reward more concurrent streams — e.g. a benchmark on
# a high-bandwidth egress showing N streams sustaining more aggregate throughput than 10 do.
DEFAULT_MAX_STORAGE_WORKERS = 10

_STORAGE_MIN_PART_SIZE_BYTES = 5 * 1024 * 1024  # provider minimum for every part but the last
_STORAGE_MAX_PARTS = 10_000  # provider maximum parts per multipart upload


@dataclass(frozen=True)
class _PartBounds:
    """Byte range of one multipart part. `part_number` is 1-indexed (S3 requires 1..N)."""

    part_number: int
    offset: int
    size: int


@dataclass(frozen=True)
class _PartResult:
    """One successfully uploaded part: its number and the storage provider's ETag for it."""

    part_number: int
    etag: str


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
    """Uploads many files via driver-per-file structured concurrency over three pools.

    Enqueue files with `enqueue_file`; track completion via the returned futures. Each file's
    whole lifecycle is collapsed behind a single `Future[str]` that resolves to the object's
    storage location or raises.

    Work is split across a small pool (one single-shot `upload_file` task per small-route file), a
    driver pool (one sequential driver per multipart file, which is also the in-flight bound on
    open multipart uploads), and a part pool (one sign+PUT task per part, submitted only by
    running drivers). Neither route can starve the other's workers in any batch shape.

    Wait DAG (deadlock-freedom by inspection): small tasks and part tasks wait only on the
    gate; drivers wait on part futures and the gate; the gate waits only on time.

    `small_file_route_max_bytes` (EXPERIMENTAL) routes files at/below that size through the
    backend single-shot `upload_file` endpoint (one request) rather than multipart, collapsing
    the per-file metadata that rate-limits many-small-file batches. Larger files use multipart.
    """

    timeout: float
    max_part_retries: int

    _upload_client: upload_api.UploadService = field(repr=False)
    _auth_header: str = field(repr=False)
    _workspace_rid: str | None = field(repr=False)
    # The session used for direct-to-storage PUTs; always owned (and closed) by this uploader.
    # The upload client itself is the caller's (usually the shared conjure client): never closed here.
    _session: requests.Session = field(repr=False)
    _small_pool: ThreadPoolExecutor = field(repr=False)
    _driver_pool: ThreadPoolExecutor = field(repr=False)
    _part_pool: ThreadPoolExecutor = field(repr=False)
    _gate: _ThrottleGate = field(repr=False)
    # If set, files with 0 < size <= this go single-shot via upload_file instead of multipart.
    _small_file_route_max_bytes: int | None = field(default=None, repr=False)
    _closed: bool = field(default=False, repr=False)
    # Set by a cancelling close: every part task short-circuits instead of uploading. This is how
    # the part lane is revoked -- see `close` for why the executor's own cancellation cannot be.
    _draining: threading.Event = field(default_factory=threading.Event, repr=False)
    # Every future handed out by `enqueue_file`, so a cancelling close can settle the queued ones
    # itself (see `close`). `_issue_lock` serializes issuing against the transition to `_closed`:
    # a future appended under it is guaranteed to be covered by close's cancel pass.
    _issued_futures: list[Future[str]] = field(default_factory=list, repr=False)
    _issue_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @classmethod
    def create(
        cls,
        client: NominalClient,
        *,
        max_storage_workers: int | None = None,
        max_small_file_workers: int | None = None,
        max_files_in_flight: int | None = None,
        small_file_route_max_bytes: int | None = DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES,
        max_nominal_concurrency: int = NOMINAL_MAX_CONCURRENCY,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        per_request_retry_timeout: float = DEFAULT_THROTTLE_DEADLINE_S,
        max_backoff_duration: float = DEFAULT_MAX_BACKOFF_DURATION_S,
    ) -> Self:
        """Create a MultipartUploader sized for one batch of uploads.

        Args:
            client: The `NominalClient` to upload with. Uploads land in the client's configured
                workspace, and requests ride the client's shared transport, whose short jittered
                throttle retries are the local layer of the two-layer backoff. The uploader
                never closes anything it borrows from the client.
            max_storage_workers: Part-pool size — the number of concurrent direct-to-storage PUT
                streams. Defaults to `DEFAULT_MAX_STORAGE_WORKERS` (benchmark-tuned). Raising it
                may degrade throughput rather than improve it: past the network path's
                saturation point, streams contend with each other. Raise it when the path to
                storage is measured to reward more concurrent streams.
            max_small_file_workers: Small-pool size. Defaults to `max_nominal_concurrency` —
                small files spend their whole task inside the nominal lane, so extra threads
                beyond the lane width would only queue at its entrance.
            max_files_in_flight: Driver-pool size, which *is* the bound on concurrently open
                multipart uploads. Defaults to half of `max_storage_workers` (rounded up), which
                keeps the part pool fed without piling up a hard-to-drain tail of half-done
                files; there is no unbounded mode.
            max_nominal_concurrency: Width of the nominal lane — the maximum number of Nominal
                API requests in flight at once, across every pool. Benchmark-tuned to fill the
                server's admission budget without provoking refusals; see
                `NOMINAL_MAX_CONCURRENCY`.
            small_file_route_max_bytes: EXPERIMENTAL. Files whose size is <= this many bytes
                are uploaded single-shot via the backend `upload_file` endpoint (one request)
                instead of multipart — avoiding the per-file metadata burst that rate-limits
                many-small-file batches. Defaults to `DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES`;
                guarded to `MAX_SMALL_FILE_ROUTE_BYTES`. Pass None to disable the route and
                send every file through multipart.
            timeout: Per-request timeout for direct-to-storage part PUTs.
            max_part_retries: Attempts per part before the file fails.
            per_request_retry_timeout: Wall-clock budget, per request, for retrying while the
                server is throttling. Throttling is not a terminal error inside that budget;
                exceeding it raises `NominalRequestThrottledError`. Non-throttle errors always
                fail immediately.
            max_backoff_duration: Cap on the shared storm damper's delay. Under sustained
                throttling the damper doubles toward this cap and every retry sleeps a jittered
                fraction of it; successes decay it back to zero.

        Returns:
            An uploader ready to accept files. Close it (or use it as a context manager) to
            release its threads and sessions.

        Raises:
            ValueError: A pool size is not positive, or `small_file_route_max_bytes` is outside
                `(0, MAX_SMALL_FILE_ROUTE_BYTES]`.
        """
        storage_workers = DEFAULT_MAX_STORAGE_WORKERS if max_storage_workers is None else max_storage_workers
        small_file_workers = max_nominal_concurrency if max_small_file_workers is None else max_small_file_workers
        files_in_flight = math.ceil(storage_workers / 2) if max_files_in_flight is None else max_files_in_flight
        for label, value in (
            ("max_storage_workers", storage_workers),
            ("max_small_file_workers", small_file_workers),
            ("max_files_in_flight", files_in_flight),
            ("max_nominal_concurrency", max_nominal_concurrency),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive, got {value}")
        if small_file_route_max_bytes is not None and not (
            0 < small_file_route_max_bytes <= MAX_SMALL_FILE_ROUTE_BYTES
        ):
            raise ValueError(
                f"small_file_route_max_bytes must be in (0, {MAX_SMALL_FILE_ROUTE_BYTES}] bytes "
                f"(the single-shot upload endpoint is disproportionately expensive server-side for "
                f"large transfers; route larger files through multipart), got {small_file_route_max_bytes}"
            )

        logger.debug(
            "creating uploader: max_storage_workers=%d, max_small_file_workers=%d, max_files_in_flight=%d, "
            "max_nominal_concurrency=%d, small_file_route_max_bytes=%s, per_request_retry_timeout=%.0fs",
            storage_workers,
            small_file_workers,
            files_in_flight,
            max_nominal_concurrency,
            small_file_route_max_bytes,
            per_request_retry_timeout,
        )
        clients = client._clients
        return cls(
            timeout,
            max_part_retries,
            # The shared client's transport-level status retries are the LOCAL layer of the
            # two-layer backoff; the gate's lane + damper are the global layer.
            _upload_client=clients.upload,
            _auth_header=clients.auth_header,
            _workspace_rid=clients.resolve_default_workspace_rid(),
            _session=create_multipart_request_session(
                pool_size=storage_workers, header_provider=clients.header_provider
            ),
            _small_pool=ThreadPoolExecutor(small_file_workers, thread_name_prefix="nominal-upload-small"),
            _driver_pool=ThreadPoolExecutor(files_in_flight, thread_name_prefix="nominal-upload-file"),
            _part_pool=ThreadPoolExecutor(storage_workers, thread_name_prefix="nominal-upload-part"),
            _gate=_ThrottleGate(
                max_concurrency=max_nominal_concurrency,
                deadline_seconds=per_request_retry_timeout,
                backoff=_GlobalBackoff(cap=max_backoff_duration),
            ),
            _small_file_route_max_bytes=small_file_route_max_bytes,
        )

    # ---- lifecycle ----

    def close(self, *, cancel_pending: bool = False) -> None:
        """Shut the uploader down; afterwards `enqueue_file` raises.

        Either way, every future `enqueue_file` has returned is settled by the time this
        returns — dropped files with `CancelledError` — and is safe to pass to
        `concurrent.futures.wait` / `as_completed` from any thread.

        Args:
            cancel_pending: If false (the default), every enqueued file runs to completion before
                this returns. If true, queued files are dropped and in-flight multipart files are
                cut short at their next part boundary, so the wait is bounded by one round of
                in-flight uploads plus the capped abort pass — not by the rest of the batch.
        """
        with self._issue_lock:
            if self._closed:
                return
            self._closed = True
        logger.debug("closing uploader (cancel_pending=%s)", cancel_pending)
        try:
            if cancel_pending:
                # Revoke the part lane FIRST, so a running driver unblocks at its next part
                # boundary, aborts, and settles instead of finishing a possibly-huge file.
                # The queued parts still RUN — each short-circuits on the drain flag in
                # microseconds. Draining them with `cancel_futures=True` instead would strand
                # their futures CANCELLED-but-never-notified (nothing ever calls
                # `set_running_or_notify_cancel` on a drained work item), a state
                # `concurrent.futures.wait` neither counts as done nor is ever woken by — and
                # a driver waiting on a queued sibling would wait forever, this close with it.
                self._draining.set()
                self._part_pool.shutdown(wait=False)
                self._small_pool.shutdown(wait=False)
                self._driver_pool.shutdown(wait=False)
                # Drop queued files by cancelling their futures ourselves — never with the
                # executors' `cancel_futures=True`, whose queue drain strands caller-held
                # futures in that same never-notified state. A future cancelled here is
                # dequeued by its pool's workers as a properly notified no-op, so the joins
                # below double as a settle barrier: by the time close returns, every future
                # this uploader ever issued is done AND visible to `wait`/`as_completed`.
                #
                # Cancelling before the driver join matters: that join lasts a whole round of
                # in-flight part PUTs plus the capped abort pass, and queued files left alive
                # across it would keep uploading after the caller was told they were dropped —
                # and outbid the aborts for lane slots (a refused abort leaves a multipart
                # upload open server-side).
                for future in self._issued_futures:
                    future.cancel()  # queued files drop; running or settled ones are unaffected
            self._driver_pool.shutdown(wait=True)  # drivers submit parts: part pool must outlive them
            self._part_pool.shutdown(wait=True)
            self._small_pool.shutdown(wait=True)
        finally:
            self._session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close(cancel_pending=exc_type is not None)

    # ---- public API ----

    def enqueue_file(
        self,
        path: pathlib.Path,
        *,
        file_type: FileType | None = None,
        name: str | None = None,
        part_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Future[str]:
        """Schedule a file upload and return a future for its storage location.

        Never blocks: the file is validated on the calling thread and then queued on its route's
        pool. The returned future is the executor's own, so cancelling a file that has not started
        yet genuinely dequeues it (and it never initiates anything); cancelling a running file
        returns False. The future stays safe to wait on across a cancelling close: `close`
        settles every future it drops (see `close`).

        Args:
            path: File to upload.
            file_type: Override the file type inferred from `path`.
            name: Override the object name derived from `path` (the file type's extension is
                still appended).
            part_size: Bytes per multipart part. Ignored on the small-file route.

        Returns:
            A future resolving to the uploaded object's location.

        Raises:
            RuntimeError: The uploader is closed.
            FileNotFoundError: `path` does not exist.
            ValueError: `name` is unsafe for storage, `part_size` is not positive, `part_size`
                would need more parts than the storage provider allows, or a multi-part
                `part_size` is below the provider's minimum for non-final parts.
        """
        if self._closed:
            raise RuntimeError("uploader is closed")
        file_type = file_type if file_type is not None else FileType.from_path(path)
        name = name if name is not None else path_upload_name(path, file_type)
        validate_upload_filename(name)
        total_size = path.stat().st_size  # raises FileNotFoundError synchronously if missing

        if part_size <= 0:
            raise ValueError(f"part_size must be positive, got {part_size}")
        num_parts = max(1, math.ceil(total_size / part_size))
        if num_parts > _STORAGE_MAX_PARTS:
            raise ValueError(
                f"'{path}' at part_size={part_size} would need {num_parts} parts, "
                f"above the storage provider's limit of {_STORAGE_MAX_PARTS}; use a larger part_size"
            )
        if num_parts > 1 and part_size < _STORAGE_MIN_PART_SIZE_BYTES:
            raise ValueError(
                f"part_size={part_size} is below the storage provider's 5 MiB minimum for "
                f"non-final parts; uploading '{path}' in {num_parts} parts would be rejected "
                f"at completion, after all bytes are uploaded. Use a larger part_size."
            )

        pending = _PendingUpload(path=path, file_type=file_type, name=name, part_size=part_size, total_size=total_size)
        # A declared size of zero is rejected by the single-shot endpoint, so empty files always
        # take multipart, where a single zero-byte part completes normally.
        small_route = (
            self._small_file_route_max_bytes is not None and 0 < total_size <= self._small_file_route_max_bytes
        )
        if small_route:
            logger.debug("enqueued %s (%d bytes) on the single-shot route", name, total_size)
        else:
            logger.debug("enqueued %s (%d bytes) on the multipart route, %d part(s)", name, total_size, num_parts)
        # Issued under the lock so a future can never slip past close's cancel pass: once close
        # holds the lock, enqueueing raises here instead of racing the executors' own shutdown.
        with self._issue_lock:
            if self._closed:
                raise RuntimeError("uploader is closed")
            if small_route:
                future = self._small_pool.submit(self._run_small_file_upload, pending)
            else:
                future = self._driver_pool.submit(self._upload_one, pending)
            self._issued_futures.append(future)
        return future

    # ---- small route (small-pool thread) ----

    def _run_small_file_upload(self, pending: _PendingUpload) -> str:
        """Upload a small file in one request via the backend `upload_file` endpoint.

        Always passes `size_bytes` so the server streams to storage (and doesn't silently cap at
        its in-memory limit). Reads the whole (small) file into memory.
        """
        safe_filename = f"{pending.name}{pending.file_type.extension}"
        body = pending.path.read_bytes()
        started = time.monotonic()
        location = self._gate.call(
            lambda: self._upload_client.upload_file(
                self._auth_header, body, safe_filename, size_bytes=pending.total_size, workspace=self._workspace_rid
            )
        )
        logger.debug(
            "uploaded %s single-shot (%d bytes) in %.2fs", safe_filename, pending.total_size, time.monotonic() - started
        )
        return location

    # ---- multipart route (driver-pool thread) ----

    def _upload_one(self, pending: _PendingUpload) -> str:
        """Run one file's whole multipart lifecycle: initiate, fan out parts, complete or abort."""
        safe_filename = f"{pending.name}{pending.file_type.extension}"
        started = time.monotonic()
        key, upload_id = self._gate.call(
            lambda: _initiate_multipart_upload(
                self._upload_client, self._auth_header, safe_filename, pending.file_type.mimetype, self._workspace_rid
            )
        )
        logger.debug("initiated multipart upload for %s: key=%s upload_id=%s", safe_filename, key, upload_id)
        plan = _PlannedUpload(
            path=pending.path,
            key=key,
            upload_id=upload_id,
            total_size=pending.total_size,
            part_size=pending.part_size,
        )
        futs: list[Future[_PartResult]] = []
        try:
            for bounds in plan.parts():
                futs.append(self._part_pool.submit(self._upload_part, plan, bounds))
            done, _unfinished = wait(futs, return_when=FIRST_EXCEPTION)
            # Surface a failure before collecting results. The collection below blocks on parts
            # in index order, so a high-numbered part's failure would otherwise wait out every
            # lower-numbered part's upload — minutes, on a large file — before cancelling its
            # siblings and aborting. Reaching past this loop means `wait` returned because every
            # part finished, and finished successfully.
            for part_future in (fut for fut in futs if fut in done):
                failure = part_future.exception()  # raises CancelledError for a revoked part
                if failure is not None:
                    raise failure
            results = [f.result() for f in futs]
            etags = {r.part_number: r.etag for r in results}
            location = self._gate.call(
                lambda: _complete_multipart_upload(self._upload_client, self._auth_header, key, upload_id, etags)
            )
            logger.debug(
                "completed multipart upload for %s (%d bytes, %d parts) in %.2fs",
                safe_filename,
                pending.total_size,
                len(etags),
                time.monotonic() - started,
            )
            return location
        except BaseException as e:
            logger.debug(
                "multipart upload for %s failed with %s after %.2fs; cancelling sibling parts and aborting",
                safe_filename,
                type(e).__name__,
                time.monotonic() - started,
            )
            for f in futs:
                f.cancel()  # queued siblings dequeue; running ones finish and are ignored
            self._safe_abort(key, upload_id, e)
            raise

    def _upload_part(self, plan: _PlannedUpload, bounds: _PartBounds) -> _PartResult:
        """Sign (gated) and PUT (ungated) one part, re-signing on a failed PUT.

        The sign call consumes API request budget so it goes through the gate; the PUT goes
        straight to the storage provider and does not, so it keeps its own transport-level retry
        and full pool concurrency. Re-signing on each attempt is what makes an expired signature
        self-healing.

        Returns the part's number and the ETag from the PUT response. A missing ETag fails the
        part immediately (see below) rather than being retried.

        Raises CancelledError the moment a cancelling close is in progress -- checked before the
        slice read, so a revoked part costs microseconds instead of a `part_size` read, and again
        before each attempt, so a retry never outlives the close.
        """
        if self._draining.is_set():
            raise CancelledError("uploader is closing")
        with plan.path.open("rb") as f:
            f.seek(bounds.offset)
            data = f.read(bounds.size)

        attempt_errors: list[Exception] = []
        for attempt in range(self.max_part_retries):
            if self._draining.is_set():
                raise CancelledError("uploader is closing")
            try:
                sign_response = self._gate.call(
                    lambda: _sign_part(
                        self._upload_client, self._auth_header, plan.key, plan.upload_id, bounds.part_number
                    )
                )
                put_response = _put_part(
                    self._session,
                    sign_response,
                    data,
                    verify=self._upload_client._verify,
                    timeout=self.timeout,
                )
            except NominalRequestThrottledError:
                raise  # the gate already spent the full budget; a fresh one would only re-herd
            except Exception as ex:
                logger.warning(
                    "Failed to sign or PUT part %d: %s",
                    bounds.part_number,
                    ex,
                    extra={"key": plan.key, "upload_id": plan.upload_id, "attempt": attempt + 1},
                )
                attempt_errors.append(
                    _wrap_multipart_retry_exception(
                        ex=ex,
                        key=plan.key,
                        part=bounds.part_number,
                        upload_id=plan.upload_id,
                        attempt=attempt + 1,
                    )
                )
                continue

            etag = put_response.headers.get("ETag")
            if not etag:
                # Completing with a missing etag would produce a corrupt object, and no retry can
                # supply one — fail the part immediately rather than re-uploading its bytes.
                raise NominalMultipartUploadError(
                    f"storage provider returned no ETag for part {bounds.part_number} "
                    f"(key={plan.key}, upload_id={plan.upload_id})"
                )
            return _PartResult(part_number=bounds.part_number, etag=etag)

        raise NominalMultipartUploadFailed(
            f"Multipart upload failed for key={plan.key}, upload_id={plan.upload_id}, "
            f"part={bounds.part_number} after {self.max_part_retries} attempts",
            attempt_errors,
        )

    def _safe_abort(self, key: str, upload_id: str, exc: BaseException) -> None:
        # Short budget on purpose: best-effort rollback must not compete for request budget with
        # the uploads still trying to succeed, and must not delay the caller's failure.
        try:
            self._gate.call(
                lambda: _abort(self._upload_client, self._auth_header, key, upload_id, exc),
                deadline_seconds=_ABORT_THROTTLE_DEADLINE_S,
            )
        except Exception:
            logger.warning("best-effort multipart abort failed", exc_info=True)
