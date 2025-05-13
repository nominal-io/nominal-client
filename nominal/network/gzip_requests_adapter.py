from __future__ import annotations

import gzip
import io
from typing import Any, Mapping, MutableMapping

import requests
from conjure_python_client._http.requests_client import TransportAdapter


class GzipRequestsAdapter(TransportAdapter):
    """Adapter used with `requests` library for sending gzip-compressed data.

    Based on: https://github.com/psf/requests/issues/1753#issuecomment-417806737
    """

    ACCEPT_ENCODING = "Accept-Encoding"
    CONTENT_ENCODING = "Content-Encoding"
    CONTENT_LENGTH = "Content-Length"

    COMPRESSION_LEVEL = 1

    def add_headers(self, request: requests.PreparedRequest, **kwargs: Any) -> None:
        """Tell the server that we support compression."""
        super().add_headers(request, **kwargs)  # type: ignore[no-untyped-call]

        body = request.body
        if body is None:
            return

        if isinstance(body, (bytes, str)):
            content_length = len(body)
        else:
            content_length = body.seek(0, 2)
            body.seek(0, 0)

        headers: MutableMapping[str, str] = {
            self.ACCEPT_ENCODING: "gzip",
            self.CONTENT_ENCODING: "gzip",
            self.CONTENT_LENGTH: str(content_length),
        }
        request.headers.update(headers)

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str = True,
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: Mapping[str, str] | None = None,
    ) -> requests.Response:
        """Compress data before sending."""
        if stream:
            # Typechecking
            if isinstance(request.body, (bytes, str)):
                raise ValueError("Expected request body to not be bytes or string if stream")
            elif request.body is None:
                raise ValueError("Expected request body to be non-null whin streaming")

            # Having a file-like object, therefore we need to stream the
            # content into a new one through the compressor.
            compressed_body = io.BytesIO()
            compressed_body.name = request.url
            compressor = gzip.open(compressed_body, mode="wb", compresslevel=self.COMPRESSION_LEVEL)

            # Read, write and compress the content at the same time.
            compressor.write(request.body.read())
            compressor.flush()
            compressor.close()

            # Seek to beginning of stream to make it readable
            compressed_body.seek(0, 0)
            request.body = compressed_body
        elif request.body is not None:
            body = request.body if isinstance(request.body, bytes) else request.body.encode("utf-8")
            request.body = gzip.compress(body, compresslevel=self.COMPRESSION_LEVEL)

        return super().send(request, stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)
