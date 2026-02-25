from __future__ import annotations

import gzip
import logging
import os
import ssl
from typing import Any, Callable, Mapping, Type, TypeVar

import requests
import truststore
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import KEEP_ALIVE_SOCKET_OPTIONS, RetryWithJitter
from requests.adapters import DEFAULT_POOLSIZE, CaseInsensitiveDict, HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

T = TypeVar("T")

GZIP_COMPRESSION_LEVEL = 1


class SslBypassRequestsAdapter(HTTPAdapter):
    """Transport adapter that allows customizing SSL options and forwarding host truststore.

    NOTE: based on a combination of injecting `truststore.SSLContext` into
        `conjure_python_client._http.requests_client.TransportAdapter`.
    """

    ENABLE_KEEP_ALIVE_ATTR = "_enable_keep_alive"
    __attrs__ = [*HTTPAdapter.__attrs__, ENABLE_KEEP_ALIVE_ATTR]

    def __init__(self, *args: Any, enable_keep_alive: bool = False, **kwargs: Any):
        self._enable_keep_alive = enable_keep_alive
        super().__init__(*args, **kwargs)

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Mapping[str, Any],
    ) -> None:
        """Wrapper around the standard init_poolmanager from HTTPAdapter with modifications
        to support keep-alive settings and injecting SSL context.
        """
        if self._enable_keep_alive:
            keep_alive_kwargs: dict[str, Any] = {
                "socket_options": [
                    *HTTPConnection.default_socket_options,
                    *KEEP_ALIVE_SOCKET_OPTIONS,
                ]
            }
            pool_kwargs = {**pool_kwargs, **keep_alive_kwargs}

        pool_kwargs["ssl_context"] = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)  # type: ignore[no-untyped-call]

    def __setstate__(self, state: dict[str, Any]) -> None:
        state[self.ENABLE_KEEP_ALIVE_ATTR] = state.get(self.ENABLE_KEEP_ALIVE_ATTR, False)
        super().__setstate__(state)  # type: ignore[misc]


class NominalRequestsAdapter(SslBypassRequestsAdapter):
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


def create_conjure_service_client(
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
    transport_adapter = NominalRequestsAdapter(max_retries=retry)
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

    See `create_conjure_service_client` for documentation on parameters.
    """

    def factory(service_class: Type[T]) -> T:
        return create_conjure_service_client(
            service_class,
            user_agent=user_agent,
            service_config=service_config,
            return_none_for_unknown_union_types=return_none_for_unknown_union_types,
        )

    return factory


def create_multipart_request_session(
    *,
    pool_size: int = DEFAULT_POOLSIZE,
    num_retries: int = 5,
) -> requests.Session:
    retries = Retry(
        total=num_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
    )
    session = requests.Session()
    adapter = SslBypassRequestsAdapter(max_retries=retries, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    return session
