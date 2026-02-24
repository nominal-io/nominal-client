from __future__ import annotations

import gzip
import logging
import os
import ssl
import threading
from typing import Any, Callable, Mapping, Type, TypeVar

import requests
import truststore
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import KEEP_ALIVE_SOCKET_OPTIONS, RetryWithJitter
from requests.adapters import DEFAULT_POOLSIZE, CaseInsensitiveDict, HTTPAdapter
from typing_extensions import Buffer
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

T = TypeVar("T")

GZIP_COMPRESSION_LEVEL = 1


class VerificationEnforcingSSLContext(truststore.SSLContext):
    """A truststore.SSLContext that coordinates truststore's _configure_context +
    wrap_socket with urllib3's load_verify_locations so they never interleave.

    truststore intentionally sets verify_mode=CERT_NONE and check_hostname=False
    before calling wrap_socket (deferring verification to the OS trust store after
    the handshake). urllib3 calls load_verify_locations on the same context when
    recycling connections, and can snapshot the temporarily-disabled state,
    corrupting it for subsequent connections.

    The fix is to hold an RLock across the entire wrap_socket call (which internally
    also acquires the lock around _configure_context), so load_verify_locations
    cannot interleave. RLock is required because wrap_socket -> super().wrap_socket
    -> _configure_context all re-acquire the same lock from the same thread.

    This class should only be used for shared contexts (i.e. those held inside
    ClientsBunch sessions which are accessed from multiple threads). Ephemeral
    per-thread sessions should use a plain truststore.SSLContext instead.
    """

    def __init__(self, protocol: int = None) -> None:  # type: ignore[assignment]
        super().__init__(protocol)
        # Replace the plain Lock truststore creates with an RLock so our
        # wrap_socket override can hold it while super().wrap_socket() also
        # tries to acquire it.
        self._ctx_lock = threading.RLock()

    def load_verify_locations(
        self,
        cafile: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None = None,
        capath: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None = None,
        cadata: str | Buffer | None = None,
    ) -> None:
        with self._ctx_lock:
            verify_mode = self._ctx.verify_mode
            check_hostname = self._ctx.check_hostname
            super().load_verify_locations(cafile=cafile, capath=capath, cadata=cadata)
            clobbered = []
            if self._ctx.verify_mode != verify_mode:
                self._ctx.verify_mode = verify_mode
                clobbered.append("verify_mode")
            if self._ctx.check_hostname != check_hostname:
                self._ctx.check_hostname = check_hostname
                clobbered.append("check_hostname")
            if clobbered:
                logger.debug(
                    "VerificationEnforcingSSLContext: load_verify_locations clobbered %s and they were restored. "
                    "(thread=%s)",
                    " and ".join(clobbered),
                    threading.get_ident(),
                )

    def wrap_socket(
        self,
        sock: Any,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLSocket:
        with self._ctx_lock:
            return super().wrap_socket(
                sock,
                server_side=server_side,
                do_handshake_on_connect=do_handshake_on_connect,
                suppress_ragged_eofs=suppress_ragged_eofs,
                server_hostname=server_hostname,
                session=session,
            )


class SslBypassRequestsAdapter(HTTPAdapter):
    """Transport adapter that uses the OS trust store via truststore.

    When shared_context=True (the default), uses a VerificationEnforcingSSLContext
    which is safe to share across threads. When shared_context=False, uses a plain
    truststore.SSLContext — suitable for ephemeral per-thread sessions where no
    sharing occurs and the locking overhead is unnecessary.
    """

    ENABLE_KEEP_ALIVE_ATTR = "_enable_keep_alive"
    SHARED_CONTEXT_ATTR = "_shared_context"
    __attrs__ = [*HTTPAdapter.__attrs__, ENABLE_KEEP_ALIVE_ATTR, SHARED_CONTEXT_ATTR]

    def __init__(self, *args: Any, enable_keep_alive: bool = False, shared_context: bool = True, **kwargs: Any):
        self._enable_keep_alive = enable_keep_alive
        self._shared_context = shared_context
        if shared_context:
            self._ssl_context: ssl.SSLContext = VerificationEnforcingSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        else:
            self._ssl_context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        super().__init__(*args, **kwargs)

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Mapping[str, Any],
    ) -> None:
        if self._enable_keep_alive:
            pool_kwargs = {
                **pool_kwargs,
                "socket_options": [
                    *HTTPConnection.default_socket_options,
                    *KEEP_ALIVE_SOCKET_OPTIONS,
                ],
            }
        pool_kwargs["ssl_context"] = self._ssl_context
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)  # type: ignore[no-untyped-call]

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        proxy_kwargs["ssl_context"] = self._ssl_context
        return super().proxy_manager_for(proxy, **proxy_kwargs)

    def __setstate__(self, state: dict[str, Any]) -> None:
        state[self.ENABLE_KEEP_ALIVE_ATTR] = state.get(self.ENABLE_KEEP_ALIVE_ATTR, False)
        state[self.SHARED_CONTEXT_ATTR] = state.get(self.SHARED_CONTEXT_ATTR, True)
        if "_ssl_context" not in state:
            shared = state[self.SHARED_CONTEXT_ATTR]
            state["_ssl_context"] = (
                VerificationEnforcingSSLContext(ssl.PROTOCOL_TLS_CLIENT)
                if shared
                else truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            )
        super().__setstate__(state)  # type: ignore[misc]


class NominalRequestsAdapter(SslBypassRequestsAdapter):
    """Adapter used with `requests` library for sending gzip-compressed data."""

    ACCEPT_ENCODING = "Accept-Encoding"
    CONTENT_ENCODING = "Content-Encoding"
    CONTENT_LENGTH = "Content-Length"

    def add_headers(self, request: requests.PreparedRequest, **kwargs: Any) -> None:
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
        if stream:
            return super().send(request, stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)
        elif request.body is not None:
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

    # shared_context=True: this session is shared across threads via ClientsBunch
    transport_adapter = NominalRequestsAdapter(max_retries=retry, shared_context=True)
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
    shared_context: bool = False,
) -> requests.Session:
    """Create a requests Session configured for multipart uploads to S3.

    Each call produces an independent session with its own SSLContext and
    connection pool.

    Args:
        pool_size: Maximum number of connections to keep in the pool.
        num_retries: Number of times to retry failed requests.
        shared_context: Whether the session will be shared across threads.
            Pass True when the session is held on a long-lived object and
            called from multiple threads concurrently (e.g. MultipartFileDownloader).
            Pass False (the default) when the session is used exclusively by a
            single thread, which avoids unnecessary locking overhead.
    """
    retries = Retry(
        total=num_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
    )
    session = requests.Session()
    adapter = SslBypassRequestsAdapter(max_retries=retries, pool_maxsize=pool_size, shared_context=shared_context)
    session.mount("https://", adapter)
    return session
