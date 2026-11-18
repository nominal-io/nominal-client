from __future__ import annotations

import io
import logging
import pathlib
import threading
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import dataclass, field
from types import TracebackType
from typing import BinaryIO, Callable, Iterator, Mapping, Self, TypeVar

import requests
from nominal_api import ingest_api, upload_api

from nominal.core._utils.filenames import resolve_upload_filename
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.core._utils.upload_pacing import (
    _ABORT_THROTTLE_DEADLINE_S,
    DEFAULT_THROTTLE_DEADLINE_S,
    NOMINAL_MAX_CONCURRENCY,
    _ThrottleGate,
)
from nominal.core.filetype import FileType
from nominal.exceptions import NominalMultipartUploadError, NominalMultipartUploadFailed, NominalRequestThrottledError

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 64_000_000
DEFAULT_NUM_WORKERS = 8

_T = TypeVar("_T")


@dataclass(frozen=True)
class _CompletedUpload:
    key: str
    """Key the upload was opened under, which a File Store change commits."""
    location: str


def _wrap_multipart_retry_exception(
    *,
    ex: Exception,
    key: str,
    part: int,
    upload_id: str,
    attempt: int,
) -> NominalMultipartUploadError:
    """Identify the failed part and provider request without copying a signed URL into the summary."""
    message = (
        f"Multipart upload failed for key={key}, upload_id={upload_id}, part={part}, "
        f"attempt={attempt}: {type(ex).__name__}"
    )
    if isinstance(ex, requests.RequestException) and ex.response is not None:
        message += f", HTTP {ex.response.status_code}"
        for header in ("x-amz-request-id", "x-amz-id-2", "x-ms-request-id"):
            if value := ex.response.headers.get(header):
                message += f", {header}={value!r}"
    wrapped = NominalMultipartUploadError(message)
    wrapped.__cause__ = ex
    return wrapped


@dataclass(frozen=True)
class _SegmentedBody:
    """A replayable requests body with a Content-Length, assembled from short reads."""

    blocks: list[bytes]
    length: int

    def __iter__(self) -> Iterator[bytes]:
        return iter(self.blocks)

    def __len__(self) -> int:
        return self.length


def _read_bytes(stream: BinaryIO, size: int) -> bytes:
    data = stream.read(size)
    if data is None:  # RawIOBase returns None when a nonblocking read would block.
        raise BlockingIOError("upload requires a blocking binary stream")
    return data


def _read_part(stream: BinaryIO, size: int) -> bytes | _SegmentedBody:
    """Fill a part even when a pipe or raw stream returns short reads before EOF."""
    first = _read_bytes(stream, size)
    if not first or len(first) == size:
        return first

    # A short read may just be the final part of a file. Keep that fast path as bytes.
    # For repeated short reads, combine tiny fragments in bounded 64 KiB blocks;
    # requests can send and retry those blocks without a second part-sized allocation.
    block_size = 64 * 1024
    blocks = [first]
    total = len(first)
    block = io.BytesIO()
    while total < size:
        piece = _read_bytes(stream, min(size - total, block_size - block.tell()))
        if not piece:
            break
        total += len(piece)
        block.write(piece)
        if block.tell() == block_size:
            blocks.append(block.getvalue())
            block = io.BytesIO()
    if block.tell():
        blocks.append(block.getvalue())
    return first if len(blocks) == 1 else _SegmentedBody(blocks, total)


def _put_part(
    multipart_session: requests.Session,
    sign_response: ingest_api.SignPartResponse,
    data: bytes | _SegmentedBody,
    *,
    verify: bool | str | None,
    timeout: float | None = None,
) -> requests.Response:
    """PUT one already-signed part to the storage provider.

    Exactly one request; retrying is the caller's job. The session's own transport-level retry
    policy still applies underneath.
    """
    put_response = multipart_session.put(
        sign_response.url,
        data=data,
        headers=sign_response.headers,
        verify=verify,
        timeout=timeout,
    )
    put_response.raise_for_status()
    return put_response


def _complete_multipart_upload(
    upload_client: upload_api.UploadService,
    auth_header: str,
    key: str,
    upload_id: str,
    etags: Mapping[int, str],
    bucket: str | None = None,
) -> str:
    """Complete an upload from a caller-supplied {part_number: etag} mapping.

    Parts are sent in ascending part-number order, which the storage provider requires.
    Returns the object location, or raises if the response carries none.
    """
    parts = [ingest_api.Part(etag=etags[number], part_number=number) for number in sorted(etags)]
    response = upload_client.complete_multipart_upload(auth_header, key, upload_id, parts, bucket=bucket)
    if response.location is None:
        raise NominalMultipartUploadFailed(
            "completing multipart upload failed: no location on response",
            [RuntimeError("Multipart upload completion returned no location")],
        )
    return response.location


def _release_error_payloads(error: BaseException) -> None:
    """Keep diagnostic stacks and HTTP status without retaining a failed upload's bytes.

    Batch callers may keep failed futures indefinitely. Both traceback locals and the
    prepared request attached to a requests exception can otherwise pin the body.
    """
    pending = [error]
    seen: set[int] = set()
    while pending:
        exc = pending.pop()
        if id(exc) in seen:
            continue
        seen.add(id(exc))
        if exc.__traceback__ is not None:
            traceback.clear_frames(exc.__traceback__)
        if isinstance(exc, requests.RequestException):
            if isinstance(exc.request, requests.PreparedRequest):
                exc.request.body = None
            if exc.response is not None:
                for response in (*exc.response.history, exc.response):
                    if response.request is not None:
                        response.request.body = None
        if isinstance(exc, BaseExceptionGroup):
            pending.extend(exc.exceptions)
        if exc.__cause__ is not None:
            pending.append(exc.__cause__)
        if exc.__context__ is not None:
            pending.append(exc.__context__)


@dataclass
class _Parts:
    """Track the handoff from one stream reader to its storage workers."""

    max_pending: int
    # The reader reserves a slot with b"" before reading, then stores the payload there.
    # None means a worker has claimed it. Removing the entry means the part has settled.
    pending: dict[int, bytes | _SegmentedBody | None] = field(default_factory=dict)
    etags: dict[int, str] = field(default_factory=dict)
    error: BaseException | None = None

    def check(self) -> None:
        if self.error is not None:
            raise self.error

    def can_read(self) -> bool:
        self.check()
        return len(self.pending) < self.max_pending


class StreamUploader:
    """Upload independent streams through one storage pool and one Nominal request gate.

    Each caller drives its own stream. Storage workers only sign and send parts, so a
    reader waiting for buffer capacity never takes a worker away from an active PUT.
    Close after the callers have returned, or cancel first to stop accepting more parts.
    """

    def __init__(
        self,
        upload_client: upload_api.UploadService,
        auth_header: str,
        workspace_rid: str | None,
        *,
        max_workers: int,
        max_buffer_bytes: int,
        header_provider: HeaderProvider | None = None,
        gate: _ThrottleGate | None = None,
        timeout: float | None = None,
        max_part_retries: int = 3,
    ) -> None:
        if max_workers <= 0 or max_buffer_bytes <= 0 or max_part_retries <= 0:
            raise ValueError("max_workers, max_buffer_bytes and max_part_retries must be positive")
        self._client = upload_client
        self._auth = auth_header
        self._workspace = workspace_rid
        self.max_buffer_bytes = max_buffer_bytes
        self.max_part_retries = max_part_retries
        self.timeout = timeout
        self.gate = gate or _ThrottleGate(
            max_concurrency=NOMINAL_MAX_CONCURRENCY, deadline_seconds=DEFAULT_THROTTLE_DEADLINE_S
        )
        self.draining = threading.Event()
        self._available = max_buffer_bytes
        self._capacity = threading.Condition()
        self._max_pending = 2 * max_workers
        self.session = create_multipart_request_session(pool_size=max_workers, header_provider=header_provider)
        self._pool = ThreadPoolExecutor(max_workers, thread_name_prefix="nominal-upload-part")

    def cancel(self) -> None:
        with self._capacity:
            self.draining.set()
            self._capacity.notify_all()

    def close(self) -> None:
        self.cancel()
        self._pool.shutdown(wait=True)
        self.session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def _check_open(self) -> None:
        if self.draining.is_set():
            raise CancelledError("uploader is closing")

    def _call(self, operation: Callable[[], _T]) -> _T:
        # Skip waiting for admission when already closing. The gate rechecks after admission.
        self._check_open()
        return self.gate.call(operation, cancel_event=self.draining)

    def _reserve_bytes(self, size: int, can_read: Callable[[], bool] = lambda: True) -> None:
        """Reserve capacity before reading. The caller holds the condition lock."""
        if size < 0 or size > self.max_buffer_bytes:
            raise ValueError(f"cannot reserve {size} bytes with max_buffer_bytes={self.max_buffer_bytes}")
        while True:
            self._check_open()
            if can_read() and size <= self._available:
                self._available -= size
                return
            self._capacity.wait()

    def _release_bytes(self, size: int) -> None:
        """Return capacity after dropping the payload, under the condition lock."""
        self._available += size
        self._capacity.notify_all()

    def upload_small_file(self, stream: BinaryIO, filename: str, *, size: int) -> str:
        """Send a size-known file in a single Nominal request under the same byte budget."""
        with self._capacity:
            self._reserve_bytes(size)
        try:
            data = _read_bytes(stream, size)
            if len(data) != size or _read_bytes(stream, 1):
                raise ValueError(f"size of {filename!r} changed since enqueue")

            return self._call(
                lambda: self._client.upload_file(self._auth, data, filename, size_bytes=size, workspace=self._workspace)
            )
        except BaseException as exc:
            _release_error_payloads(exc)
            raise
        finally:
            data = b""  # An escaping exception can keep this frame, including its local variables.
            with self._capacity:
                self._release_bytes(size)

    def upload_stream(
        self,
        stream: BinaryIO,
        filename: str,
        mimetype: str,
        *,
        chunk_size: int,
        expected_size: int | None = None,
        destination: ingest_api.UploadDestination | None = None,
        use_put_etags: bool = False,
    ) -> _CompletedUpload:
        """Upload from the current position. A known file size bounds the final read.

        The driver reads and numbers each chunk together, then hands it to one worker.
        Once every worker settles, complete using PUT receipts or list the stored parts.
        On failure stop siblings, drain active PUTs, and only then abort the remote upload.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must be nonnegative")
        reservation = chunk_size if expected_size is None else min(expected_size, chunk_size)
        if reservation > self.max_buffer_bytes:
            raise ValueError(f"part requires {reservation} bytes, exceeding max_buffer_bytes={self.max_buffer_bytes}")
        request = ingest_api.InitiateMultipartUploadRequest(
            filename=filename, filetype=mimetype, workspace=self._workspace, destination=destination
        )
        upload = self._call(lambda: self._client.initiate_multipart_upload(self._auth, request))
        parts = _Parts(self._max_pending)
        try:
            sent = self._send_stream(stream, chunk_size, expected_size, upload, parts, use_put_etags)
            with self._capacity:
                self._capacity.wait_for(lambda: not parts.pending)
                parts.check()
            etags = parts.etags
            if not use_put_etags:
                listed = self._call(
                    lambda: self._client.list_parts(self._auth, upload.key, upload.upload_id, bucket=upload.bucket)
                )
                etags = {part.part_number: part.etag for part in listed}
                if len(etags) != sent or any(n < 1 or n > sent for n in etags):
                    raise NominalMultipartUploadError(
                        f"Incomplete part listing for {upload.key!r}: expected parts 1–{sent}"
                    )
            location = self._call(
                lambda: _complete_multipart_upload(
                    self._client, self._auth, upload.key, upload.upload_id, etags, upload.bucket
                )
            )
            return _CompletedUpload(upload.key, location)
        except BaseException as exc:
            with self._capacity:
                if parts.error is None:
                    parts.error = exc
                # A stream read, submit, or the success-path wait itself can be interrupted.
                # Mark queued siblings first; let running PUTs finish before touching the upload ID.
                self._capacity.wait_for(lambda: not parts.pending)
            _release_error_payloads(exc)
            try:
                self.gate.call(
                    lambda: self._client.abort_multipart_upload(
                        self._auth, upload.key, upload.upload_id, bucket=upload.bucket
                    ),
                    deadline_seconds=_ABORT_THROTTLE_DEADLINE_S,
                )
            except Exception as abort_error:
                logger.warning("Could not abort multipart upload %s: %s", upload.upload_id, type(abort_error).__name__)
            raise

    def _send_stream(
        self,
        stream: BinaryIO,
        chunk_size: int,
        remaining: int | None,
        upload: ingest_api.InitiateMultipartUploadResponse,
        parts: _Parts,
        use_put_etags: bool,
    ) -> int:
        number = 1
        while number == 1 or remaining != 0:
            size = chunk_size if remaining is None else min(chunk_size, remaining)
            with self._capacity:
                self._reserve_bytes(size, parts.can_read)
                parts.pending[number] = b""
            try:
                data = _read_part(stream, size)
                count = len(data)
                parts.pending[number] = data
                del data
                if remaining is not None and count != size:
                    raise ValueError(f"File ended early during upload of {upload.key!r}")
                if not count and number > 1:
                    with self._capacity:
                        del parts.pending[number]
                        self._release_bytes(size)
                    break
                self._pool.submit(self._run_part, upload, parts, number, size, use_put_etags)
            except BaseException as exc:
                data = b""
                _release_error_payloads(exc)
                with self._capacity:
                    # An interrupted submit may already have handed the job to a worker.
                    if parts.pending.get(number) is not None:
                        del parts.pending[number]
                        self._release_bytes(size)
                raise
            if remaining is not None:
                remaining -= count
            number += 1
            if count < size or not count:
                break
        if remaining == 0 and _read_bytes(stream, 1):
            raise ValueError(f"File grew during upload of {upload.key!r}")
        return number - 1

    def _run_part(
        self,
        upload: ingest_api.InitiateMultipartUploadResponse,
        parts: _Parts,
        number: int,
        size: int,
        use_put_etags: bool,
    ) -> None:
        with self._capacity:
            data = parts.pending.get(number)
            if data is None:
                return
            parts.pending[number] = None
        try:
            etag = self._send_part(upload, parts, number, data, use_put_etags)
            if etag is not None:
                with self._capacity:
                    parts.etags[number] = etag
        except BaseException as exc:
            _release_error_payloads(exc)
            with self._capacity:
                if parts.error is None:
                    parts.error = exc
        finally:
            del data
            with self._capacity:
                del parts.pending[number]
                self._release_bytes(size)

    def _send_part(
        self,
        upload: ingest_api.InitiateMultipartUploadResponse,
        parts: _Parts,
        number: int,
        data: bytes | _SegmentedBody,
        use_put_etags: bool,
    ) -> str | None:
        def sign() -> ingest_api.SignPartResponse:
            if parts.error is not None:
                raise CancelledError("sibling part failed")
            return self._client.sign_part(self._auth, upload.key, number, upload.upload_id, bucket=upload.bucket)

        errors = []
        for attempt in range(1, self.max_part_retries + 1):
            try:
                signed = self._call(sign)
                self._check_open()
                if parts.error is not None:
                    raise CancelledError("sibling part failed")
                response = _put_part(self.session, signed, data, verify=self._client._verify, timeout=self.timeout)
            except (CancelledError, NominalRequestThrottledError):
                raise
            except Exception as exc:
                errors.append(
                    _wrap_multipart_retry_exception(
                        ex=exc, key=upload.key, part=number, upload_id=upload.upload_id, attempt=attempt
                    )
                )
                continue
            # A previous failed attempt can hold a traceback pointing back into this
            # frame. Break the cycle so a successful retry frees its payload immediately.
            errors.clear()
            if not use_put_etags:
                return None
            if etag := response.headers.get("ETag"):
                return etag
            raise NominalMultipartUploadError(f"storage provider returned no ETag for part {number} (key={upload.key})")
        raise NominalMultipartUploadFailed(
            f"Multipart upload failed for key={upload.key}, upload_id={upload.upload_id}, "
            f"part={number} after {len(errors)} attempts",
            errors,
        )


def put_multipart_upload(
    auth_header: str,
    workspace_rid: str | None,
    f: BinaryIO,
    filename: str,
    mimetype: str,
    upload_client: upload_api.UploadService,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
) -> str:
    """Execute a multipart upload to S3.

    All metadata-style requests (init, sign, complete) proxy through Nominal servers, while the upload PUT requests for
    each part go to a pre-signed URL to the storage provider.

    Args:
        auth_header: Nominal authorization token
        workspace_rid: Nominal workspace rid
        f: Binary stream to upload. An empty stream creates a zero-byte object.
        filename: filename to use when uploading; sent literally as the object name
        mimetype: Type of data contained within binary stream
        upload_client: Conjure upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html

    """
    return _put_multipart_upload_to(
        auth_header,
        workspace_rid,
        f,
        filename,
        mimetype,
        upload_client,
        chunk_size=chunk_size,
        max_workers=max_workers,
        header_provider=header_provider,
    ).location


def _put_multipart_upload_to(
    auth_header: str,
    workspace_rid: str | None,
    f: BinaryIO,
    filename: str,
    mimetype: str,
    upload_client: upload_api.UploadService,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
    destination: ingest_api.UploadDestination | None = None,
) -> _CompletedUpload:
    """Run `put_multipart_upload` against ``destination``, returning the upload's key as well as its location.

    Every follow-up request passes back the ``bucket`` handle initiate returned, which is how
    the server finds an upload outside the default destination.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    with StreamUploader(
        upload_client,
        auth_header,
        workspace_rid,
        max_workers=max_workers,
        max_buffer_bytes=2 * max_workers * chunk_size,
        header_provider=header_provider,
    ) as uploader:
        return uploader.upload_stream(f, filename, mimetype, chunk_size=chunk_size, destination=destination)


def upload_multipart_io(
    auth_header: str,
    workspace_rid: str | None,
    f: BinaryIO,
    name: str,
    file_type: FileType,
    upload_client: upload_api.UploadService,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
) -> str:
    """Execute a multipart upload to S3 proxied via Nominal servers

    Args:
        auth_header: Nominal authorization token
        workspace_rid: Nominal workspace rid
        f: Binary stream to upload. An empty stream creates a zero-byte object.
        name: Upload filename. Existing extensions are preserved; only names without one use the default.
            Sent literally (no URL-encoding); unsafe storage characters raise ValueError.
        file_type: MIME type and default extension to use when the filename has none.
        upload_client: Conjure upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    Note:
        see put_multipart_upload for more details

    """
    safe_filename = resolve_upload_filename(name, file_type.extension)
    return put_multipart_upload(
        auth_header,
        workspace_rid,
        f,
        safe_filename,
        file_type.mimetype,
        upload_client,
        chunk_size=chunk_size,
        max_workers=max_workers,
        header_provider=header_provider,
    )


def upload_multipart_file(
    auth_header: str,
    workspace_rid: str | None,
    file: pathlib.Path,
    upload_client: upload_api.UploadService,
    file_type: FileType | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
) -> str:
    """Execute a multipart upload to S3 proxied via Nominal servers.

    Args:
        auth_header: Nominal authorization token
        workspace_rid: Nominal workspace rid
        file: File to upload to S3
        upload_client: Conjure upload client
        file_type: Manually override inferred file type for the given file
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    Note:
        see put_multipart_upload for more details

    """
    if file_type is None:
        file_type = FileType.from_path(file)

    with file.open("rb") as file_handle:
        return upload_multipart_io(
            auth_header,
            workspace_rid,
            file_handle,
            file.name,
            file_type,
            upload_client,
            chunk_size=chunk_size,
            max_workers=max_workers,
            header_provider=header_provider,
        )
