from __future__ import annotations

import concurrent.futures
import logging
import pathlib
from functools import partial
from queue import Queue
from typing import BinaryIO, Iterable, Mapping

import requests

from nominal.core._utils.filenames import resolve_upload_filename
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.core._utils.pagination_tools import paginate_grpc
from nominal.core.filetype import FileType
from nominal.exceptions import NominalMultipartUploadError, NominalMultipartUploadFailed
from nominal.protos.uploads.v1 import uploads_pb2, uploads_pb2_grpc

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 64_000_000
DEFAULT_NUM_WORKERS = 8


def _wrap_multipart_retry_exception(
    *,
    ex: Exception,
    key: str,
    part: int,
    upload_id: str,
    attempt: int,
) -> NominalMultipartUploadError:
    """Wrap a failed multipart upload attempt so that we can:
    - add contextual information, especially response headers so that we can debug S3 errors
    - AWS will want x-amz-request-id and x-amz-id-2 headers for debugging
    """
    response = getattr(ex, "response", None)
    response_headers = (
        ", ".join(f"{header}={value!r}" for header, value in sorted(response.headers.items()))
        if response is not None and response.headers is not None
        else "none"
    )
    wrapped = NominalMultipartUploadError(
        f"Multipart upload failed for key={key}, upload_id={upload_id}, part={part}, "
        f"attempt={attempt}: {type(ex).__name__}: {ex}. Response headers: {response_headers}"
    )
    wrapped.__cause__ = ex
    return wrapped


def _put_part(
    multipart_session: requests.Session,
    sign_response: uploads_pb2.UploadPartUrl,
    data: bytes,
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
        verify=verify,
        timeout=timeout,
    )
    put_response.raise_for_status()
    return put_response


def _sign_and_put_part(
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    multipart_session: requests.Session,
    key: str,
    upload_id: str,
    part: int,
    data: bytes,
    num_retries: int = 3,
    verify: bool | str | None = None,
) -> requests.Response:
    """Sign and PUT a single in-memory part to S3, retrying transient failures.

    The retry wrapper for the legacy single-stream path; the driver-model uploader runs its own
    gated, drainable retry loop over the same signing RPC and `_put_part` primitive.
    """
    attempt_errors: list[NominalMultipartUploadError] = []
    for attempt in range(num_retries):
        log_extras = {"key": key, "part": part, "upload_id": upload_id, "attempt": attempt + 1}
        try:
            logger.debug("Signing part %d for upload", part, extra=log_extras)
            with translate_grpc_errors():
                sign_response = upload_client.CreateUploadPartUrl(
                    uploads_pb2.CreateUploadPartUrlRequest(upload_id=upload_id, part_number=part)
                ).upload_part_url
            logger.debug(
                "Successfully signed part %d for upload",
                part,
                extra=log_extras,
            )

            logger.debug("Pushing part %d for multipart upload", part, extra=log_extras)
            put_response = _put_part(multipart_session, sign_response, data, verify=verify)
            logger.debug(
                "Finished pushing part %d for multipart upload with status %d",
                part,
                put_response.status_code,
                extra=log_extras,
            )
            return put_response
        except Exception as ex:
            logger.warning("Failed to upload part %d: %s", part, ex, extra=log_extras)
            attempt_errors.append(
                _wrap_multipart_retry_exception(ex=ex, key=key, part=part, upload_id=upload_id, attempt=attempt + 1)
            )

    if attempt_errors:
        raise NominalMultipartUploadFailed(
            f"Multipart upload failed for key={key}, upload_id={upload_id}, part={part} after {num_retries} attempts",
            attempt_errors,
        )

    raise RuntimeError(f"Unknown error uploading part {part} for upload_id={upload_id} and key={key}")


def _sign_and_upload_part_job(
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    multipart_session: requests.Session,
    key: str,
    upload_id: str,
    q: Queue[bytes],
    part: int,
    num_retries: int = 3,
    verify: bool | str | None = None,
) -> requests.Response:
    data = q.get()
    try:
        return _sign_and_put_part(upload_client, multipart_session, key, upload_id, part, data, num_retries, verify)
    finally:
        q.task_done()


def _initiate_multipart_upload(
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    filename: str,
    mimetype: str,
    workspace_rid: str | None,
) -> tuple[str, str]:
    """Initiate a multipart upload. Returns (key, upload_id)."""
    request = uploads_pb2.CreateMultipartUploadRequest(
        multipart_upload=uploads_pb2.MultipartUpload(
            file_name=filename, content_type=mimetype, workspace_rid=workspace_rid
        )
    )
    with translate_grpc_errors():
        response = upload_client.CreateMultipartUpload(request).multipart_upload
    return response.object_key, response.upload_id


def _complete_multipart_upload(
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    upload_id: str,
    etags: Mapping[int, str],
) -> str:
    """Complete an upload from a caller-supplied {part_number: etag} mapping.

    Parts are sent in ascending part-number order, which the storage provider requires.
    Returns the object location, or raises if the response carries none.
    """
    parts = [uploads_pb2.UploadPart(etag=etags[number], part_number=number) for number in sorted(etags)]
    with translate_grpc_errors():
        response = upload_client.CompleteMultipartUpload(
            uploads_pb2.CompleteMultipartUploadRequest(upload_id=upload_id, parts=parts)
        )
    if not response.object_path:
        raise NominalMultipartUploadFailed(
            "completing multipart upload failed: no location on response",
            [RuntimeError("Multipart upload completion returned no location")],
        )
    return response.object_path


def _iter_chunks(f: BinaryIO, chunk_size: int) -> Iterable[bytes]:
    while (data := f.read(chunk_size)) != b"":
        yield data


def put_multipart_upload(
    workspace_rid: str | None,
    f: BinaryIO,
    filename: str,
    mimetype: str,
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
    verify: bool | str | None = None,
) -> str:
    """Execute a multipart upload to S3.

    All metadata-style requests (init, sign, complete) proxy through Nominal servers, while the upload PUT requests for
    each part go to a pre-signed URL to the storage provider.

    Args:
        workspace_rid: Nominal workspace rid
        f: Binary IO to upload
        filename: filename to use when uploading; sent literally as the object name
        mimetype: Type of data contained within binary stream
        upload_client: gRPC upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        verify: CA bundle or TLS verification setting for object-storage requests.
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html

    """
    # muiltithreaded multipart upload:
    # - create a worker thread pool and a queue for all threads to share
    # - initialize the upload, getting the object key and upload id
    # - the main thread will chunk the file up into "parts" and enqueue each part
    # - each task will take a "part" from the queue, and sign and upload it
    # - once all tasks are done, all parts will have been uploaded, so we "complete"
    #   the upload and get the final s3 location.
    # - if any error occurs after initializing, we abort the upload.

    q: Queue[bytes] = Queue(maxsize=2 * max_workers)  # allow for look-ahead
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    key, upload_id = _initiate_multipart_upload(upload_client, filename, mimetype, workspace_rid)

    # One session shared across all part jobs for this upload.
    session = create_multipart_request_session(pool_size=max_workers, header_provider=header_provider)

    # Prefill arguments for helper function
    _sign_and_upload_part = partial(_sign_and_upload_part_job, upload_client, session, key, upload_id, q, verify=verify)

    jobs: list[concurrent.futures.Future[requests.Response]] = []

    try:
        for part, chunk in enumerate(_iter_chunks(f, chunk_size), start=1):
            q.put(chunk)
            fut = pool.submit(_sign_and_upload_part, part)
            jobs.append(fut)
            logger.debug("submitted sign and upload job", extra={"part": part})

        # block until all upload jobs are complete
        done, not_done = concurrent.futures.wait(jobs, return_when="FIRST_EXCEPTION")

        # if there was an error, not all jobs completed, so cancel any remaining tasks
        for fut in not_done:
            fut.cancel()

        # re-raise any exception encountered to abort the upload
        for fut in done:
            maybe_exc = fut.exception()
            if maybe_exc is not None:
                raise maybe_exc

        # if all tasks have successfully completed, the queue should be empty too
        q.join()

        # Mark the upload as completed. This single-stream path tracks no ETags of its own, so it
        # asks the server which parts landed, following every page before completing.
        parts_with_size = (
            part
            for page in paginate_grpc(
                upload_client.ListUploadParts,
                request_factory=lambda token: uploads_pb2.ListUploadPartsRequest(
                    upload_id=upload_id, page_token=token or "", page_size=1000
                ),
            )
            for part in page.parts
        )
        return _complete_multipart_upload(upload_client, upload_id, {p.part_number: p.etag for p in parts_with_size})
    except Exception as e:
        _abort(upload_client, key, upload_id, e)
        raise e


def upload_multipart_io(
    workspace_rid: str | None,
    f: BinaryIO,
    name: str,
    file_type: FileType,
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
    verify: bool | str | None = None,
) -> str:
    """Execute a multipart upload to S3 proxied via Nominal servers

    Args:
        workspace_rid: Nominal workspace rid
        f: Binary IO to upload
        name: Upload filename. Existing extensions are preserved; only names without one use the default.
            Sent literally (no URL-encoding); unsafe storage characters raise ValueError.
        file_type: MIME type and default extension to use when the filename has none.
        upload_client: gRPC upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        verify: CA bundle or TLS verification setting for object-storage requests.
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    Note:
        see put_multipart_upload for more details

    """
    safe_filename = resolve_upload_filename(name, file_type.extension)
    return put_multipart_upload(
        workspace_rid,
        f,
        safe_filename,
        file_type.mimetype,
        upload_client,
        chunk_size=chunk_size,
        max_workers=max_workers,
        header_provider=header_provider,
        verify=verify,
    )


def upload_multipart_file(
    workspace_rid: str | None,
    file: pathlib.Path,
    upload_client: uploads_pb2_grpc.UploadServiceStub,
    file_type: FileType | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
    header_provider: HeaderProvider | None = None,
    verify: bool | str | None = None,
) -> str:
    """Execute a multipart upload to S3 proxied via Nominal servers.

    Args:
        workspace_rid: Nominal workspace rid
        file: File to upload to S3
        upload_client: gRPC upload client
        file_type: Manually override inferred file type for the given file
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data
        verify: CA bundle or TLS verification setting for object-storage requests.
        header_provider: Optional provider for headers to include in all requests to object store

    Returns: Path to the uploaded object in S3

    Note:
        see put_multipart_upload for more details

    """
    if file_type is None:
        file_type = FileType.from_path(file)

    with file.open("rb") as file_handle:
        return upload_multipart_io(
            workspace_rid,
            file_handle,
            file.name,
            file_type,
            upload_client,
            chunk_size=chunk_size,
            max_workers=max_workers,
            header_provider=header_provider,
            verify=verify,
        )


def _abort(upload_client: uploads_pb2_grpc.UploadServiceStub, key: str, upload_id: str, e: BaseException) -> None:
    logger.error(
        "aborting multipart upload due to an exception", exc_info=e, extra={"key": key, "upload_id": upload_id}
    )
    try:
        with translate_grpc_errors():
            upload_client.DeleteMultipartUpload(uploads_pb2.DeleteMultipartUploadRequest(upload_id=upload_id))
    except Exception as exc:
        logger.critical("multipart upload abort failed", exc_info=exc, extra={"key": key, "upload_id": upload_id})
        raise
