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
from concurrent.futures import FIRST_EXCEPTION, CancelledError, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Iterable, Type

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
    _put_part,
    _sign_part,
    _wrap_multipart_retry_exception,
    path_upload_name,
)
from nominal.core._utils.networking import (
    create_conjure_service_client_with_session,
    create_multipart_request_session,
)
from nominal.core.exceptions import (
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalRequestThrottledError,
)
from nominal.core.filetype import FileType
from nominal.experimental.ingest._upload_pacing import (
    _ABORT_THROTTLE_DEADLINE_S,
    DEFAULT_THROTTLE_DEADLINE_S,
    _AdaptivePacer,
    _ThrottleGate,
)

logger = logging.getLogger(__name__)

# Files at or below the uploader's `small_file_route_max_bytes` are uploaded single-shot via the
# backend `upload_file` endpoint (one request) instead of the multipart flow. That endpoint holds
# a server request thread for the whole transfer, so the opt-in threshold is hard-capped: measured
# throughput of the two routes reaches parity around this size, above which multipart is strictly
# better (its bytes stream directly to storage and its parts parallelize). Experimental.
MAX_SMALL_FILE_ROUTE_BYTES = 4 * 1024 * 1024  # 4 MiB

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

    max_workers: int
    timeout: float
    max_part_retries: int

    _upload_client: upload_api.UploadService = field(repr=False)
    _auth_header: str = field(repr=False)
    _workspace_rid: str | None = field(repr=False)
    # The session used for direct-to-storage PUTs; always owned (and closed) by this uploader.
    _session: requests.Session = field(repr=False)
    # The dedicated upload client's session, or None when the client was injected. An injected
    # client's lifecycle belongs to its owner, so the uploader never closes it.
    _owned_client_session: requests.Session | None = field(repr=False)
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

    @classmethod
    def create(
        cls,
        clients: Any,
        *,
        workspace_rid: str | None = None,
        max_workers: int | None = None,
        small_route_workers: int | None = None,
        max_multipart_files_in_flight: int | None = None,
        small_file_route_max_bytes: int | None = None,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        throttle_deadline: float = DEFAULT_THROTTLE_DEADLINE_S,
        upload_client: upload_api.UploadService | None = None,
    ) -> Self:
        """Create a MultipartUploader sized for one batch of uploads.

        Args:
            clients: The client bundle to upload with. Duck-typed: `auth_header`,
                `header_provider`, `resolve_default_workspace_rid()`, `_user_agent`,
                `_service_config`.
            workspace_rid: Workspace to upload into. Defaults to the clients' default workspace.
            max_workers: Part-pool size, and the default for the other two pools.
            small_route_workers: Small-pool size. Defaults to `max_workers`.
            max_multipart_files_in_flight: Driver-pool size, which *is* the bound on concurrently
                open multipart uploads. Defaults to `max_workers`; there is no unbounded mode.
            small_file_route_max_bytes: EXPERIMENTAL. If set, files whose size is <= this many
                bytes are uploaded single-shot via the backend `upload_file` endpoint (one
                request) instead of multipart — avoiding the per-file metadata burst that
                rate-limits many-small-file batches. Guarded to `MAX_SMALL_FILE_ROUTE_BYTES`.
            timeout: Per-request timeout for direct-to-storage part PUTs.
            max_part_retries: Attempts per part before the file fails.
            throttle_deadline: Wall-clock budget, per request, for retrying while the server is
                throttling. Throttling is not a terminal error inside that budget; exceeding it
                raises `NominalRequestThrottledError`. Non-throttle errors always fail immediately.
            upload_client: Injection seam for tests and callers with their own client. When None
                the uploader builds (and owns) a dedicated one; an injected client is never closed.

        Returns:
            An uploader ready to accept files. Close it (or use it as a context manager) to
            release its threads and sessions.

        Raises:
            ValueError: A pool size is not positive, or `small_file_route_max_bytes` is outside
                `(0, MAX_SMALL_FILE_ROUTE_BYTES]`.
        """
        max_workers = DEFAULT_NUM_WORKERS if max_workers is None else max_workers
        small_route_workers = max_workers if small_route_workers is None else small_route_workers
        drivers = max_workers if max_multipart_files_in_flight is None else max_multipart_files_in_flight
        for label, value in (
            ("max_workers", max_workers),
            ("small_route_workers", small_route_workers),
            ("max_multipart_files_in_flight", drivers),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive, got {value}")
        if small_file_route_max_bytes is not None and not (
            0 < small_file_route_max_bytes <= MAX_SMALL_FILE_ROUTE_BYTES
        ):
            raise ValueError(
                f"small_file_route_max_bytes must be in (0, {MAX_SMALL_FILE_ROUTE_BYTES}] bytes "
                f"(the single-shot upload endpoint holds a server thread per upload; route larger "
                f"files through multipart), got {small_file_route_max_bytes}"
            )

        client: upload_api.UploadService
        owned_session: requests.Session | None
        if upload_client is None:
            # The uploader owns its client: transport status-retries reduced to redirects so
            # 429/503 reach the gate in one round trip; pool sized to this uploader's threads.
            client, owned_session = create_conjure_service_client_with_session(
                upload_api.UploadService,
                user_agent=clients._user_agent,
                service_config=clients._service_config,
                header_provider=clients.header_provider,
                retry_status_forcelist=(308,),
                pool_connections=small_route_workers + max_workers,
                pool_maxsize=small_route_workers + max_workers,
            )
        else:
            client, owned_session = upload_client, None  # injected: its lifecycle is not ours
        return cls(
            max_workers,
            timeout,
            max_part_retries,
            _upload_client=client,
            _auth_header=clients.auth_header,
            _workspace_rid=workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid(),
            _session=create_multipart_request_session(pool_size=max_workers, header_provider=clients.header_provider),
            _owned_client_session=owned_session,
            _small_pool=ThreadPoolExecutor(small_route_workers, thread_name_prefix="nominal-upload-small"),
            _driver_pool=ThreadPoolExecutor(drivers, thread_name_prefix="nominal-upload-file"),
            _part_pool=ThreadPoolExecutor(max_workers, thread_name_prefix="nominal-upload-part"),
            _gate=_ThrottleGate(_AdaptivePacer(), deadline_seconds=throttle_deadline),
            _small_file_route_max_bytes=small_file_route_max_bytes,
        )

    # ---- lifecycle ----

    def close(self, *, cancel_pending: bool = False) -> None:
        """Shut the uploader down; afterwards `enqueue_file` raises.

        Args:
            cancel_pending: If false (the default), every enqueued file runs to completion before
                this returns. If true, queued files are dropped and in-flight multipart files are
                cut short at their next part boundary, so the wait is bounded by one in-flight
                request rather than by the rest of the batch.
        """
        if self._closed:
            return
        self._closed = True
        try:
            if cancel_pending:
                # Revoke the part lane FIRST, so a running driver unblocks at its next part
                # boundary, aborts, and settles instead of finishing a possibly-huge file.
                #
                # Revoking it with `cancel_futures=True` would deadlock: that drains queued work
                # items out of the pool and cancels their futures WITHOUT anyone ever calling
                # `set_running_or_notify_cancel`, so the futures sit in CANCELLED-but-not-notified
                # — a state `concurrent.futures.wait` neither counts as done nor is ever woken by.
                # A driver waiting on a queued sibling would then wait forever, and this close
                # with it. So the queued parts must still RUN: the flag makes each one
                # short-circuit in microseconds, and its future settles (and notifies) normally.
                self._draining.set()
                self._part_pool.shutdown(wait=False)
                # Queued drivers never started, so plain cancellation is safe here: nothing inside
                # the uploader waits on a driver future, only the caller, and `result()`/`done()`/
                # `cancelled()` all read CANCELLED correctly.
                self._driver_pool.shutdown(wait=True, cancel_futures=True)
                self._small_pool.shutdown(wait=True, cancel_futures=True)
                self._part_pool.shutdown(wait=True)
            else:
                self._driver_pool.shutdown(wait=True)  # drivers submit parts: part pool must outlive them
                self._part_pool.shutdown(wait=True)
                self._small_pool.shutdown(wait=True)
        finally:
            self._session.close()
            if self._owned_client_session is not None:
                self._owned_client_session.close()

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
        returns False.

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
        if self._small_file_route_max_bytes is not None and 0 < total_size <= self._small_file_route_max_bytes:
            return self._small_pool.submit(self._run_small_file_upload, pending)
        return self._driver_pool.submit(self._upload_one, pending)

    # ---- small route (small-pool thread) ----

    def _run_small_file_upload(self, pending: _PendingUpload) -> str:
        """Upload a small file in one request via the backend `upload_file` endpoint.

        Always passes `size_bytes` so the server streams to storage (and doesn't silently cap at
        its in-memory limit). Reads the whole (small) file into memory.
        """
        safe_filename = f"{pending.name}{pending.file_type.extension}"
        body = pending.path.read_bytes()
        return self._gate.call(
            lambda: self._upload_client.upload_file(
                self._auth_header, body, safe_filename, size_bytes=pending.total_size, workspace=self._workspace_rid
            )
        )

    # ---- multipart route (driver-pool thread) ----

    def _upload_one(self, pending: _PendingUpload) -> str:
        """Run one file's whole multipart lifecycle: initiate, fan out parts, complete or abort."""
        safe_filename = f"{pending.name}{pending.file_type.extension}"
        key, upload_id = self._gate.call(
            lambda: _initiate_multipart_upload(
                self._upload_client, self._auth_header, safe_filename, pending.file_type.mimetype, self._workspace_rid
            )
        )
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
            return self._gate.call(
                lambda: _complete_multipart_upload(self._upload_client, self._auth_header, key, upload_id, etags)
            )
        except BaseException as e:
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
