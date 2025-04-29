from __future__ import annotations

import concurrent.futures
import logging
import pathlib
import urllib.parse
from functools import partial
from queue import Queue
from typing import BinaryIO, Iterable

import requests
from nominal_api import ingest_api, upload_api

from nominal.core.filetype import FileType
from nominal.exceptions import NominalMultipartUploadFailed

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 64_000_000
DEFAULT_NUM_WORKERS = 8


def _sign_and_upload_part_job(
    upload_client: upload_api.UploadService,
    auth_header: str,
    key: str,
    upload_id: str,
    q: Queue[bytes],
    part: int,
) -> requests.Response:
    data = q.get()
    try:
        response = upload_client.sign_part(auth_header, key, part, upload_id)
        logger.debug(
            "successfully signed multipart upload part",
            extra={"key": key, "part": part, "upload_id": upload_id, "response.url": response.url},
        )
        put_response = requests.put(
            response.url,
            data=data,
            headers=response.headers,
            verify=upload_client._verify,
        )
        logger.debug(
            "put multipart upload part",
            extra={"url": response.url, "size": len(data), "status_code": put_response.status_code},
        )
        put_response.raise_for_status()
        return put_response
    except Exception as e:
        logger.exception("error uploading part", exc_info=e, extra={"key": key, "upload_id": upload_id, "part": part})
        raise e
    finally:
        q.task_done()


def _iter_chunks(f: BinaryIO, chunk_size: int) -> Iterable[bytes]:
    while (data := f.read(chunk_size)) != b"":
        yield data


def path_upload_name(path: pathlib.Path, file_type: FileType) -> str:
    """Extract the name of a file without any extension suffixes associated with the file_type for use in uploads"""
    filename = path.name

    # If the file type has an extension associated, and the path ends in that extension,
    # remove exactly the extenion
    if file_type.extension and filename.endswith(file_type.extension):
        return filename[: -len(file_type.extension)]

    return path.stem.split(".")[0]


def put_multipart_upload(
    auth_header: str,
    workspace_rid: str | None,
    f: BinaryIO,
    filename: str,
    mimetype: str,
    upload_client: upload_api.UploadService,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
) -> str:
    """Execute a multipart upload to S3.

    All metadata-style requests (init, sign, complete) proxy through Nominal servers, while the upload PUT requests for
    each part go to a pre-signed URL to the storage provider.

    Args:
        auth_header: Nominal authorization token
        workspace_rid: Nominal workspace rid
        f: Binary IO to upload
        filename: URL-safe filename to use when uploading to S3
        mimetype: Type of data contained within binary stream
        upload_client: Conjure upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data

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
    initiate_request = ingest_api.InitiateMultipartUploadRequest(
        filename=filename, filetype=mimetype, workspace=workspace_rid
    )
    initiate_response = upload_client.initiate_multipart_upload(auth_header, initiate_request)
    key, upload_id = initiate_response.key, initiate_response.upload_id
    _sign_and_upload_part = partial(_sign_and_upload_part_job, upload_client, auth_header, key, upload_id, q)

    jobs: list[concurrent.futures.Future[requests.Response]] = []

    try:
        # S3 requires parts to start at 1
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

        # mark the upload as completed
        parts_with_size = upload_client.list_parts(auth_header, key, upload_id)
        parts = [ingest_api.Part(etag=p.etag, part_number=p.part_number) for p in parts_with_size]
        complete_response = upload_client.complete_multipart_upload(auth_header, key, upload_id, parts)
        if complete_response.location is None:
            raise NominalMultipartUploadFailed("completing multipart upload failed: no location on response")
        return complete_response.location
    except Exception as e:
        _abort(upload_client, auth_header, key, upload_id, e)
        raise e


def upload_multipart_io(
    auth_header: str,
    workspace_rid: str | None,
    f: BinaryIO,
    name: str,
    file_type: FileType,
    upload_client: upload_api.UploadService,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
) -> str:
    """Execute a multipart upload to S3 proxied via Nominal servers

    Args:
        auth_header: Nominal authorization token
        workspace_rid: Nominal workspace rid
        f: Binary IO to upload
        name: Name of the file to create in S3
            NOTE: does not need to be URL Safe
        file_type: Type of data being uploaded
        upload_client: Conjure upload client
        chunk_size: Maximum size of chunk to upload to S3 at once
        max_workers: Number of worker threads to use when processing and uploading data

    Returns: Path to the uploaded object in S3

    Note: see put_multipart_upload for more details

    """
    urlsafe_name = urllib.parse.quote_plus(name)
    safe_filename = f"{urlsafe_name}{file_type.extension}"
    return put_multipart_upload(
        auth_header,
        workspace_rid,
        f,
        safe_filename,
        file_type.mimetype,
        upload_client,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )


def upload_multipart_file(
    auth_header: str,
    workspace_rid: str | None,
    file: pathlib.Path,
    upload_client: upload_api.UploadService,
    file_type: FileType | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = DEFAULT_NUM_WORKERS,
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

    Returns: Path to the uploaded object in S3

    Note: see put_multipart_upload for more details

    """
    if file_type is None:
        file_type = FileType.from_path(file)

    file_name = path_upload_name(file, file_type)
    with file.open("rb") as file_handle:
        return upload_multipart_io(
            auth_header,
            workspace_rid,
            file_handle,
            file_name,
            file_type,
            upload_client,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )


def _abort(upload_client: upload_api.UploadService, auth_header: str, key: str, upload_id: str, e: Exception) -> None:
    logger.error(
        "aborting multipart upload due to an exception", exc_info=e, extra={"key": key, "upload_id": upload_id}
    )
    try:
        upload_client.abort_multipart_upload(auth_header, key, upload_id)
    except Exception as exc:
        logger.critical("multipart upload abort failed", exc_info=exc, extra={"key": key, "upload_id": upload_id})
        raise exc from e
