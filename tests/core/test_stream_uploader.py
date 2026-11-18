"""Payload ownership across independent callers sharing a core uploader."""

from __future__ import annotations

import gc
import io
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import MagicMock

import pytest
import requests
from nominal_api import ingest_api

from nominal.core._utils.multipart import StreamUploader
from nominal.exceptions import NominalMultipartUploadFailed


def test_small_file_and_stream_share_read_capacity() -> None:
    """A held single-shot body leaves room for only one multipart chunk across the batch."""
    small_started = Event()
    release_small = Event()
    first_put = Event()
    allow_put = Event()
    received: list[bytes] = []
    client = MagicMock()
    client.initiate_multipart_upload.return_value = ingest_api.InitiateMultipartUploadResponse(
        key="video", upload_id="upload", bucket="uploads"
    )
    client.complete_multipart_upload.return_value = ingest_api.CompleteMultipartUploadResponse(location="s3://video")

    def send_small(*args: object, **kwargs: object) -> str:
        small_started.set()
        assert release_small.wait(timeout=5)
        return "s3://manifest"

    def put(url: str, *, data: bytes, **kwargs: object) -> MagicMock:
        first_put.set()
        assert allow_put.wait(timeout=5)
        received.append(data)
        return MagicMock(headers={"ETag": "etag"})

    client.upload_file.side_effect = send_small
    with (
        StreamUploader(client, "token", None, max_workers=1, max_buffer_bytes=8) as uploader,
        ThreadPoolExecutor(2) as drivers,
    ):
        # Use the HTTP boundary; the real driver, storage executor and byte budget run together.
        uploader.session.put = MagicMock(side_effect=put)
        small = drivers.submit(uploader.upload_small_file, io.BytesIO(b"info"), "manifest", size=4)
        source = io.BytesIO(b"abcdefghijkl")
        try:
            assert small_started.wait(timeout=5)
            large = drivers.submit(
                uploader.upload_stream, source, "video", "video/mp4", chunk_size=4, use_put_etags=True
            )
            assert first_put.wait(timeout=5)
            assert source.tell() == 4
        finally:
            release_small.set()
            allow_put.set()

        assert small.result(timeout=5) == "s3://manifest"
        assert large.result(timeout=5).location == "s3://video"
    assert received == [b"abcd", b"efgh", b"ijkl"]


@pytest.mark.parametrize("single_request", [True, False], ids=["small-file", "multipart"])
def test_keeping_an_upload_error_releases_the_buffered_payload(single_request: bool) -> None:
    """Callers can retain failed outcomes without retaining the original body via a request or traceback."""
    released = []

    class Payload(bytes):
        def __del__(self) -> None:
            released.append(True)

    class Source(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            data = super().read(size)
            return Payload(data) if data else data

    url = "https://storage.example.test/file"

    def refuse(data: bytes) -> requests.Response:
        # A redirect preserves the upload body on both prepared requests.
        redirect = requests.Response()
        redirect.status_code = 307
        redirect.request = requests.Request("PUT", url, data=data).prepare()
        response = requests.Response()
        response.history = [redirect]
        response.request = requests.Request("PUT", url, data=data).prepare()
        response.status_code = 403
        response.raise_for_status()
        return response

    client = MagicMock()
    client.initiate_multipart_upload.return_value = MagicMock(key="key", upload_id="id", bucket="bucket")
    client.sign_part.return_value = MagicMock(url=url, headers={})
    # A plain callable avoids a mock call history retaining the test's payload.
    client.upload_file = lambda auth, body, name, **kwargs: refuse(body)
    with StreamUploader(client, "auth", None, max_workers=1, max_buffer_bytes=4, max_part_retries=1) as uploader:
        uploader.session.put = lambda url, *, data, **kwargs: refuse(data)
        with pytest.raises(requests.HTTPError if single_request else NominalMultipartUploadFailed) as failure:
            if single_request:
                uploader.upload_small_file(Source(b"data"), "test.bin", size=4)
            else:
                uploader.upload_stream(Source(b"data"), "test.bin", "application/octet-stream", chunk_size=4)

    gc.collect()
    assert failure.value is not None
    assert released == [True]


def test_partial_read_failure_releases_payload_before_another_file_reads() -> None:
    """An unfinished PUT can delay the abort, while the failed read's bytes are reusable immediately."""
    put_started, finish_put = Event(), Event()
    read_failed, partial_released = Event(), Event()

    class Partial(bytes):
        def __del__(self) -> None:
            partial_released.set()

    class FailingSource(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            data = super().read(size)
            if data == b"x":
                assert put_started.wait(timeout=5)
                return Partial(data)
            if not data:
                read_failed.set()
                raise OSError("read failed")
            return data

    def put(*args: object, **kwargs: object) -> MagicMock:
        put_started.set()
        assert finish_put.wait(timeout=5)
        return MagicMock()

    client = MagicMock()
    with (
        StreamUploader(client, "auth", None, max_workers=1, max_buffer_bytes=8) as uploader,
        ThreadPoolExecutor(1) as drivers,
    ):
        uploader.session.put = put
        failed = drivers.submit(uploader.upload_stream, FailingSource(b"datax"), "large", "text/plain", chunk_size=4)
        try:
            assert read_failed.wait(timeout=5)
            uploader.upload_small_file(io.BytesIO(b"more"), "small", size=4)
            assert partial_released.is_set()
        finally:
            finish_put.set()
        with pytest.raises(OSError, match="read failed"):
            failed.result(timeout=5)
