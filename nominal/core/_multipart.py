from __future__ import annotations

import concurrent.futures
import logging
from functools import partial
from queue import Queue
from typing import BinaryIO, Iterable

import requests

from .._api.combined import ingest_api, upload_api
from ..exceptions import NominalMultipartUploadFailed

logger = logging.getLogger(__name__)


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
        put_response = requests.put(response.url, data=data, headers=response.headers)
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


def put_multipart_upload(
    auth_header: str,
    f: BinaryIO,
    filename: str,
    mimetype: str,
    upload_client: upload_api.UploadService,
    chunk_size: int = 64_000_000,
    max_workers: int = 8,
) -> str:
    """Execute a multipart upload to S3.

    All metadata-style requests (init, sign, complete) proxy through Nominal servers, while the upload PUT requests for
    each part go to a pre-signed URL to the storage provider.

    Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
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
    initiate_request = ingest_api.InitiateMultipartUploadRequest(filename=filename, filetype=mimetype)
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


def _abort(upload_client: upload_api.UploadService, auth_header: str, key: str, upload_id: str, e: Exception) -> None:
    logger.error(
        "aborting multipart upload due to an exception", exc_info=e, extra={"key": key, "upload_id": upload_id}
    )
    try:
        upload_client.abort_multipart_upload(auth_header, key, upload_id)
    except Exception as e:
        logger.critical("multipart upload abort failed", exc_info=e, extra={"key": key, "upload_id": upload_id})
        raise e
