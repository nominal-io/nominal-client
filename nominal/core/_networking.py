from __future__ import annotations

import gzip
import os
from typing import Any, Callable, Mapping, Type, TypeVar

import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import RetryWithJitter, TransportAdapter
from requests.adapters import CaseInsensitiveDict

T = TypeVar("T")

GZIP_COMPRESSION_LEVEL = 1


class GzipRequestsAdapter(TransportAdapter):
    """Adapter used with `requests` library for sending gzip-compressed data.

    Based on: https://github.com/psf/requests/issues/1753#issuecomment-417806737
    """

    ACCEPT_ENCODING = "Accept-Encoding"
    CONTENT_ENCODING = "Content-Encoding"
    CONTENT_LENGTH = "Content-Length"

    def add_headers(self, request: requests.PreparedRequest, **kwargs: Any) -> None:
        """Tell the server that we support compression."""
        super().add_headers(request, **kwargs)  # type: ignore[no-untyped-call]

        body = request.body
        if body is None:
            return
        elif kwargs.get("stream", False):
            return

        if isinstance(body, (bytes, str)):
            content_length = len(body)
        else:
            content_length = body.seek(0, os.SEEK_END)
            body.seek(0, os.SEEK_SET)

        headers = {
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
            # We don't need to gzip streamed data via requests-- any such api endpoint today has a
            # multi-part upload based mechanism for uploading large requests.
            return super().send(request, stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)
        elif request.body is not None:
            # If there is data being posted to the API, gzip-encode it to save network bandwidth
            body = request.body if isinstance(request.body, bytes) else request.body.encode("utf-8")
            request.body = gzip.compress(body, compresslevel=GZIP_COMPRESSION_LEVEL)

        return super().send(request, stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)


def create_gzip_service_client(
    service_class: Type[T],
    user_agent: str,
    service_config: ServiceConfiguration,
    return_none_for_unknown_union_types: bool = False,
) -> T:
    """Wrapper around logic found in the conjure_python_client for creating conjure clients
    that automatically gzip data being sent to services.

    In bandwidth constrained scenarios, this has been measured to have up to 5x speedups in time to
    send data to backend services, depending on the compressability of the data.

    See: https://github.com/palantir/conjure-python-client/blob/60d6d7639502a3b0fe18fad388ce84cbc54eb613/conjure_python_client/_http/requests_client.py#L181

    Args:
        service_class: Conjure class of the service to create a client for
        user_agent: User agent string to add as a header to all requests
        service_config: Configuration for the service containing metadata such as the base URL of the api, security
            settings, and timeout settings.
        return_none_for_unknown_union_types: If true, returns None instead of raising an exception when an unknown
            union type is encountered during decoding API responses.
        enable_keep_alive: If true, enable keep alive in connections with the service.

    Returns:
        Instantiated conjure client object to hit the API with
    """
    # setup retry to match java remoting
    # https://github.com/palantir/http-remoting/tree/3.12.0#quality-of-service-retry-failover-throttling
    retry = RetryWithJitter(
        total=service_config.max_num_retries,
        connect=service_config.max_num_retries,  # Allow connection error retries
        read=service_config.max_num_retries,  # Allow read error retries (e.g., RemoteDisconnected)
        status_forcelist=[308, 429, 503],
        backoff_factor=float(service_config.backoff_slot_size) / 1000,
    )
    transport_adapter = GzipRequestsAdapter(max_retries=retry)
    # create a session, for shared connection polling, user agent, etc
    session = requests.Session()
    session.headers = CaseInsensitiveDict({"User-Agent": user_agent})
    if service_config.security is not None:
        verify = service_config.security.trust_store_path
    else:
        verify = None
    for uri in service_config.uris:
        session.mount(uri, transport_adapter)
    return service_class(  # type: ignore
        session,
        service_config.uris,
        service_config.connect_timeout,
        service_config.read_timeout,
        verify,
        return_none_for_unknown_union_types,
    )


def create_conjure_client_factory(
    user_agent: str,
    service_config: ServiceConfiguration,
    return_none_for_unknown_union_types: bool = False,
) -> Callable[[Type[T]], T]:
    """Create factory method for creating conjure clients given the respective conjure service type

    See `create_gzip_service_client` for documentation on parameters.
    """

    def factory(service_class: Type[T]) -> T:
        return create_gzip_service_client(
            service_class,
            user_agent=user_agent,
            service_config=service_config,
            return_none_for_unknown_union_types=return_none_for_unknown_union_types,
        )

    return factory
