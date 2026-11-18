"""Schedule local files on the shared stream uploader."""

from __future__ import annotations

import logging
import math
import pathlib
import random
import threading
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Callable, Self, Type
from weakref import WeakSet

import requests

from nominal.core._utils.filenames import resolve_upload_filename
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE, StreamUploader
from nominal.core._utils.upload_pacing import (
    DEFAULT_MAX_BACKOFF_DURATION_S,
    DEFAULT_THROTTLE_DEADLINE_S,
    NOMINAL_MAX_CONCURRENCY,
    _ThrottleGate,
)
from nominal.core.filetype import FileType
from nominal.exceptions import NominalMultipartUploadFailed, NominalRequestThrottledError

if TYPE_CHECKING:
    from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

# Tiny files avoid the initiate/sign/complete round trips. Larger bodies go directly to
# storage so they do not occupy the Nominal API connection for the whole transfer.
DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES = 1024 * 1024
MAX_SMALL_FILE_ROUTE_BYTES = 4 * 1024 * 1024
DEFAULT_MAX_STORAGE_WORKERS = 10
DEFAULT_FILE_RETRY_TIMEOUT_S = 3600.0

_STORAGE_MIN_PART_SIZE_BYTES = 5 * 1024 * 1024  # provider minimum for every part but the last
_STORAGE_MAX_PARTS = 10_000  # provider maximum parts per multipart upload

_FILE_RETRY_BASE_S = 1.0  # first retry lands quickly: brief blips resolve in seconds
_FILE_RETRY_CAP_S = 60.0  # a long outage is probed about once a minute, not hammered


def _is_transient_upload_error(exc: BaseException) -> bool:
    """True if `exc` is network weather that retrying the whole file can outlast.

    Transient: connection failures, timeouts, transport retry exhaustion, throttle-budget
    exhaustion, and HTTP 408/429/5xx — including any of those as the underlying causes inside
    a part-failure group. Everything else (local I/O errors, other 4xx, corruption, unknown
    exceptions) is treated as permanent: retrying cannot help, so the failure surfaces
    immediately.
    """
    match exc:
        case (
            ConnectionError()  # raw socket-level errors
            | TimeoutError()
            | requests.exceptions.ConnectionError()
            | requests.exceptions.Timeout()
            | requests.exceptions.RetryError()
            | NominalRequestThrottledError()  # the gate spent one request budget; a retry gets a fresh one
        ):
            return True
        case requests.exceptions.HTTPError(response=response) if response is not None:
            return response.status_code in (408, 429) or response.status_code >= 500
        case NominalMultipartUploadFailed():
            # A part that exhausted its attempts: transient iff every attempt failed transiently
            # (each wrapped attempt error chains the original as its __cause__).
            causes = [wrapped.__cause__ for wrapped in exc.exceptions]
            return bool(causes) and all(cause is not None and _is_transient_upload_error(cause) for cause in causes)
        case _:
            return False


@dataclass
class MultipartUploader:
    """Upload a batch of local files and return a storage-location future for each file.

    Small files use a single Nominal request. Larger files are opened as binary streams and
    their numbered parts go directly to object storage. Both routes share one buffer budget and
    one gate for Nominal requests, including retries after throttling.

    File drivers can wait for memory or a storage PUT. They use their own executor so workers
    needed to finish those PUTs are always available. Single-request uploads also have their
    own executor so large multipart files cannot occupy every file driver.
    """

    _streams: StreamUploader = field(repr=False)
    _small_pool: ThreadPoolExecutor = field(repr=False)
    _driver_pool: ThreadPoolExecutor = field(repr=False)
    # If set, files with 0 < size <= this go single-shot via upload_file instead of multipart.
    _small_file_route_max_bytes: int | None = field(default=None, repr=False)
    # Per-file transient-retry budget in seconds; None disables file-level retry entirely.
    _file_retry_timeout: float | None = field(default=None, repr=False)
    # Time seams for the file-retry loop. `_retry_wait` sleeps AND watches the drain flag
    # (True = close in progress); None waits for a cancelling close. Tests inject fakes.
    _retry_clock: Callable[[], float] = field(default=time.monotonic, repr=False)
    _retry_wait: Callable[[float], bool] | None = field(default=None, repr=False)
    _retry_jitter: Callable[[float], float] = field(default=lambda delay: random.uniform(0.0, delay), repr=False)
    _accepting_files: bool = field(default=True, repr=False)
    _closed: bool = field(default=False, repr=False)
    # Executors keep pending jobs alive. Weak references let completed batches release their
    # results and exceptions as soon as callers release the futures.
    _issued_futures: WeakSet[Future[str]] = field(default_factory=WeakSet, repr=False)
    _issue_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def timeout(self) -> float | None:
        """Timeout in seconds applied to each storage PUT."""
        return self._streams.timeout

    @timeout.setter
    def timeout(self, value: float) -> None:
        self._streams.timeout = value

    @property
    def max_part_retries(self) -> int:
        """Number of attempts available to each multipart part."""
        return self._streams.max_part_retries

    @max_part_retries.setter
    def max_part_retries(self, value: int) -> None:
        self._streams.max_part_retries = value

    @classmethod
    def create(
        cls,
        client: NominalClient,
        *,
        max_storage_workers: int | None = None,
        max_small_file_workers: int | None = None,
        max_files_in_flight: int | None = None,
        max_buffer_bytes: int | None = None,
        small_file_route_max_bytes: int | None = DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES,
        max_nominal_concurrency: int = NOMINAL_MAX_CONCURRENCY,
        timeout: float = 30.0,
        max_part_retries: int = 3,
        per_request_retry_timeout: float = DEFAULT_THROTTLE_DEADLINE_S,
        max_backoff_duration: float = DEFAULT_MAX_BACKOFF_DURATION_S,
        file_retry_timeout: float | None = DEFAULT_FILE_RETRY_TIMEOUT_S,
    ) -> Self:
        """Create a MultipartUploader sized for one batch of uploads.

        Args:
            client: Client whose workspace, credentials and header provider to use.
            max_storage_workers: Concurrent PUTs to object storage. Defaults to 10. Larger
                values may help on high-bandwidth connections until the network saturates.
            max_buffer_bytes: Total bytes available for buffered file payloads across both upload
                routes. Defaults to one default-size part per storage worker plus one maximum
                single-request body per small-file worker. Each individual part or single-request
                body must fit this budget. HTTP buffers and bookkeeping take additional memory.
            max_small_file_workers: Concurrent single-request uploads. Defaults to
                `max_nominal_concurrency`.
            max_files_in_flight: Concurrent multipart files. Defaults to half the storage
                worker count, rounded up.
            small_file_route_max_bytes: Use one Nominal request for nonempty files up to this
                size. Defaults to 1 MiB and supports values up to 4 MiB. Pass None to send
                every file through multipart.
            max_nominal_concurrency: Maximum concurrent API calls across all files, including
                small uploads, signing and completion. Direct storage PUTs use their own pool.
            timeout: Per-request timeout for direct-to-storage part PUTs.
            max_part_retries: Attempts per part before the file fails.
            per_request_retry_timeout: Time in seconds available to start or retry a Nominal
                request. An exhausted budget raises `NominalRequestThrottledError`. An
                in-flight transport request can finish after this deadline.
            max_backoff_duration: Maximum local backoff in seconds. When Nominal supplies
                a longer Retry-After delay, the batch waits until that delay expires.
            file_retry_timeout: Budget in seconds for retrying a local file from the beginning
                after a transient failure, such as a connection timeout. Defaults to one hour.
                Pass None to disable whole-file retries.

        Returns:
            An uploader ready to accept files. Close it (or use it as a context manager) to
            release its threads and sessions.

        Raises:
            ValueError: A worker count, part attempt count, or buffer budget is nonpositive, or
                the small-file threshold falls outside the supported range.
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
        if max_buffer_bytes is None:
            max_buffer_bytes = storage_workers * DEFAULT_CHUNK_SIZE + small_file_workers * (
                small_file_route_max_bytes or 0
            )
        streams = StreamUploader(
            clients.upload,
            clients.auth_header,
            clients.resolve_default_workspace_rid(),
            max_workers=storage_workers,
            max_buffer_bytes=max_buffer_bytes,
            header_provider=clients.header_provider,
            timeout=timeout,
            max_part_retries=max_part_retries,
            gate=_ThrottleGate(
                max_concurrency=max_nominal_concurrency,
                deadline_seconds=per_request_retry_timeout,
                max_backoff_duration=max_backoff_duration,
            ),
        )
        return cls(
            _streams=streams,
            _small_pool=ThreadPoolExecutor(small_file_workers, thread_name_prefix="nominal-upload-small"),
            _driver_pool=ThreadPoolExecutor(files_in_flight, thread_name_prefix="nominal-upload-file"),
            _file_retry_timeout=file_retry_timeout,
            _small_file_route_max_bytes=small_file_route_max_bytes,
        )

    def close(self, *, cancel_pending: bool = False) -> None:
        """Wait for enqueued files to settle and release the storage workers and session.

        Args:
            cancel_pending: Cancel queued files and stop running files at the next request
                boundary. In-flight requests finish before the remote upload is aborted.
                Pending file futures remain safe to pass to `wait` and `as_completed`.
        """
        with self._issue_lock:
            if self._closed:
                return
            self._accepting_files = False
        if cancel_pending:
            self._streams.cancel()
            # Explicit cancel lets the executor notify wait()/as_completed() when it dequeues
            # the cancelled future. Draining its queue with cancel_futures=True skips that step.
            for future in self._issued_futures:
                future.cancel()
        # Join outside the lock so another caller can cancel a batch that is already draining.
        # File drivers may still need storage workers to finish or abort their uploads.
        self._driver_pool.shutdown(wait=True)
        self._small_pool.shutdown(wait=True)
        with self._issue_lock:
            if not self._closed:
                self._streams.close()
                self._closed = True

    def __enter__(self) -> Self:
        """Use this uploader for the lifetime of a batch."""
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Finish queued files on success, or cancel pending work on exception."""
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

        Validation runs on the calling thread. Queuing returns immediately even when all
        workers are busy. You can cancel the future until its file starts uploading.

        Args:
            path: File to upload.
            file_type: Override the file type inferred from `path`.
            name: Override the filename derived from `path`. An existing extension is preserved;
                only names without one use the default extension from ``file_type``.
            part_size: Bytes per multipart part. Ignored on the small-file route. The largest
                part of this file must fit in the batch's `max_buffer_bytes` budget.

        Returns:
            A future resolving to the uploaded object's location.

        Raises:
            RuntimeError: The uploader is closed.
            FileNotFoundError: `path` does not exist.
            ValueError: The filename or default extension is invalid, a request body exceeds
                the batch buffer budget, or the multipart size is nonpositive or requires
                more parts than the storage provider allows.

        Note:
            Some providers require all non-final parts to be at least 5 MiB. Smaller values
            log a warning and may fail at completion.
        """
        if not self._accepting_files:
            raise RuntimeError("uploader is closed")
        file_type = file_type if file_type is not None else FileType.from_path(path)
        filename = resolve_upload_filename(name if name is not None else path.name, file_type.extension)
        total_size = path.stat().st_size  # raises FileNotFoundError synchronously if missing

        # A declared size of zero is rejected by the single-shot endpoint, so empty files always
        # take multipart, where a single zero-byte part completes normally.
        small_route = (
            self._small_file_route_max_bytes is not None and 0 < total_size <= self._small_file_route_max_bytes
        )
        if small_route:
            reservation = total_size
        else:
            if part_size <= 0:
                raise ValueError(f"part_size must be positive, got {part_size}")
            num_parts = max(1, math.ceil(total_size / part_size))
            if num_parts > _STORAGE_MAX_PARTS:
                raise ValueError(
                    f"'{path}' at part_size={part_size} would need {num_parts} parts, "
                    f"above the storage provider's limit of {_STORAGE_MAX_PARTS}; use a larger part_size"
                )
            if num_parts > 1 and part_size < _STORAGE_MIN_PART_SIZE_BYTES:
                # Deliberately small parts can be the right call on a lossy network, so this
                # warns rather than rejects — but the storage provider may still refuse
                # non-final parts below its minimum when the upload completes.
                logger.warning(
                    "part_size=%d is below the storage provider's 5 MiB minimum for non-final "
                    "parts; the provider may reject uploading '%s' in %d parts at completion",
                    part_size,
                    path,
                    num_parts,
                )
            reservation = min(total_size, part_size)
        if reservation > self._streams.max_buffer_bytes:
            raise ValueError(
                f"upload of {filename!r} needs {reservation} buffered bytes; "
                f"max_buffer_bytes is {self._streams.max_buffer_bytes}"
            )

        def upload_once() -> str:
            # A whole-file retry reopens the source; part retries reuse the buffered bytes.
            if self._streams.draining.is_set():
                raise CancelledError("uploader is closing")
            with path.open("rb") as stream:
                if small_route:
                    return self._streams.upload_small_file(stream, filename, size=total_size)
                return self._streams.upload_stream(
                    stream,
                    filename,
                    file_type.mimetype,
                    chunk_size=part_size,
                    expected_size=total_size,
                    use_put_etags=True,
                ).location

        # Issued under the lock so a future can never slip past close's cancel pass: once close
        # holds the lock, enqueueing raises here instead of racing the executors' own shutdown.
        with self._issue_lock:
            if not self._accepting_files:
                raise RuntimeError("uploader is closed")
            pool = self._small_pool if small_route else self._driver_pool
            future = pool.submit(self._run_with_file_retry, upload_once, filename)
            self._issued_futures.add(future)
        return future

    # ---- file-level transient retry (runs on the task's own pool thread) ----

    def _run_with_file_retry(self, upload_once: Callable[[], str], name: str) -> str:
        """Run one file's upload, retrying transient failures until the file's retry budget expires.

        Transient failures (see `_is_transient_upload_error`) re-run the whole upload after a
        jittered exponential backoff; permanent or unknown failures raise immediately. The
        backoff waits ON the drain flag, so a cancelling close interrupts the wait instantly
        and the file settles as cancelled — close latency never grows with the retry budget.
        """
        if self._file_retry_timeout is None:
            return upload_once()
        wait_for_drain = self._retry_wait if self._retry_wait is not None else self._streams.draining.wait
        deadline = self._retry_clock() + self._file_retry_timeout
        delay = _FILE_RETRY_BASE_S
        while True:
            try:
                return upload_once()
            except Exception as exc:
                if not _is_transient_upload_error(exc):
                    raise
                remaining = deadline - self._retry_clock()
                if remaining <= 0:
                    raise
                wait_s = min(self._retry_jitter(delay), remaining)
                logger.warning(
                    "Transient %s uploading %s; retrying in %.1fs (%.0fs of retry budget left)",
                    type(exc).__name__,
                    name,
                    wait_s,
                    remaining,
                )
                if wait_for_drain(wait_s):
                    raise CancelledError("uploader is closing") from exc
                if self._retry_clock() >= deadline:
                    raise
                delay = min(delay * 2.0, _FILE_RETRY_CAP_S)
