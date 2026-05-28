from __future__ import annotations

import gzip
import logging
import ssl
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping, Type, TypeVar

if TYPE_CHECKING:
    import grpc

import requests
import truststore
import typing_extensions
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import KEEP_ALIVE_SOCKET_OPTIONS, RetryWithJitter
from requests.adapters import DEFAULT_POOLSIZE, CaseInsensitiveDict, HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

from nominal.core.exceptions import HeaderConflictError

logger = logging.getLogger(__name__)

T = TypeVar("T")

GZIP_COMPRESSION_LEVEL = 1


class HeaderProvider(ABC):
    @abstractmethod
    def headers(self) -> Mapping[str, str]: ...


class TransportProvider(ABC):
    """Controls transport-level authentication for Nominal API connections.

    Implementations supply credentials for both the HTTP (requests) and gRPC transports.
    The default HTTP path calls ``create_ssl_context()`` to build an ``ssl.SSLContext``
    that is injected into the standard requests adapter. Implementations that need a
    different TLS stack for API calls (e.g. Windows Schannel for CAC card auth) can
    override ``create_http_adapter()`` to return a fully custom ``HTTPAdapter`` instead,
    which is then mounted for the Nominal API URIs only.
    """

    @abstractmethod
    def create_ssl_context(self) -> ssl.SSLContext:
        """Return an ``ssl.SSLContext`` for the requests HTTP adapter.

        Used by the default HTTP path and for non-API connections such as S3 multipart
        uploads. Implementations that replace the API adapter via ``create_http_adapter()``
        still need this for those non-API uses.
        """
        ...

    @abstractmethod
    def create_grpc_channel_credentials(
        self,
        *,
        root_certificates: bytes | None = None,
        certificate_chain_pem: bytes | None = None,
    ) -> grpc.ChannelCredentials:
        """Return ``grpc.ChannelCredentials`` for gRPC channel creation."""
        ...

    def create_http_adapter(
        self,
        *,
        max_retries: Retry,
    ) -> HTTPAdapter | None:
        """Return a custom ``HTTPAdapter`` for Nominal API calls, or ``None`` to use the default.

        Override this when the TLS stack for API traffic must be replaced entirely — for
        example, on Windows where smartcard auth routes through Schannel rather than
        OpenSSL + pkcs11-provider. The returned adapter is mounted for each service URI;
        ``create_ssl_context()`` is not called for those URIs.

        Note: ``HTTPAdapter(max_retries=...)`` only retries when urllib3 performs the send.
        Implementations that bypass urllib3 must implement their own retry logic or
        accept a single attempt per call.
        """
        return None


@dataclass(frozen=True)
class StaticHeaderProvider(HeaderProvider):
    _headers: Mapping[str, str]

    def headers(self) -> Mapping[str, str]:
        return self._headers


def normalize_header_provider(headers: HeaderProvider | Mapping[str, str] | None) -> HeaderProvider | None:
    if headers is None:
        return None
    if isinstance(headers, HeaderProvider):
        return headers
    return StaticHeaderProvider(headers)


class HeaderProviderSession(requests.Session):
    def __init__(self, header_provider: HeaderProvider | None = None) -> None:
        super().__init__()
        self._header_provider = header_provider

    @typing_extensions.override
    def prepare_request(self, request: requests.Request) -> requests.PreparedRequest:
        prepared = super().prepare_request(request)
        if self._header_provider is None:
            return prepared

        request_headers = CaseInsensitiveDict(request.headers or {})
        for key, value in self._header_provider.headers().items():
            if key in request_headers:
                raise HeaderConflictError(
                    f"HeaderProvider returned header {key!r}, but the request already set that header; "
                    "HeaderProvider cannot override explicit request headers."
                )
            prepared.headers[key] = value
        return prepared


class ThreadSafeSSLContext(truststore.SSLContext):
    """A truststore.SSLContext that is safe to share across threads.

    truststore's wrap_socket temporarily sets verify_mode=CERT_NONE while the
    TLS handshake is in progress, releasing _ctx_lock before the handshake runs.
    This creates a window where urllib3's load_verify_locations can snapshot the
    disabled state and corrupt the context for subsequent connections.

    This subclass replaces _ctx_lock with an RLock and holds it across the full
    wrap_socket call, making load_verify_locations and wrap_socket mutually
    exclusive. RLock is required because truststore's wrap_socket re-acquires
    _ctx_lock internally around _configure_context.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        # Intentionally expanding type from lock -> rlock
        self._ctx_lock: threading.RLock = threading.RLock()  # type: ignore[assignment]

    @typing_extensions.override
    def wrap_socket(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> ssl.SSLSocket:
        with self._ctx_lock:
            return super().wrap_socket(*args, **kwargs)


class NominalSslRequestsAdapter(HTTPAdapter):
    """Transport adapter that uses the OS trust store via truststore.

    If a SSL context is not provided, sessions use a ThreadSafeSSLContext, which is safe to share across threads.
    """

    ENABLE_KEEP_ALIVE_ATTR = "_enable_keep_alive"
    __attrs__ = [*HTTPAdapter.__attrs__, ENABLE_KEEP_ALIVE_ATTR]

    def __init__(
        self,
        *args: Any,
        enable_keep_alive: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        **kwargs: Any,
    ):
        self._enable_keep_alive = enable_keep_alive
        self._ssl_context = ssl_context if ssl_context is not None else ThreadSafeSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        super().__init__(*args, **kwargs)

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Mapping[str, Any],
    ) -> None:
        kwargs: dict[str, Any] = {**pool_kwargs}
        if self._enable_keep_alive:
            kwargs["socket_options"] = [
                *HTTPConnection.default_socket_options,
                *KEEP_ALIVE_SOCKET_OPTIONS,
            ]
        super().init_poolmanager(connections, maxsize, block, **kwargs, ssl_context=self._ssl_context)  # type: ignore[no-untyped-call]

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        proxy_kwargs.pop("ssl_context", None)
        return super().proxy_manager_for(proxy, **proxy_kwargs, ssl_context=self._ssl_context)  # type: ignore[no-untyped-call]

    def __setstate__(self, state: dict[str, Any]) -> None:
        state[self.ENABLE_KEEP_ALIVE_ATTR] = state.get(self.ENABLE_KEEP_ALIVE_ATTR, False)
        if "_ssl_context" not in state:
            state["_ssl_context"] = ThreadSafeSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        super().__setstate__(state)  # type: ignore[misc]


class NominalRequestsAdapter(NominalSslRequestsAdapter):
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

        content_length = len(body)
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
            body = request.body
            raw_body = body if isinstance(body, bytes) else body.encode("utf-8")
            request.body = gzip.compress(raw_body, compresslevel=GZIP_COMPRESSION_LEVEL)
            request.headers[self.CONTENT_LENGTH] = str(len(request.body))

        return super().send(request, stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)


def create_conjure_service_client(
    service_class: Type[T],
    user_agent: str,
    service_config: ServiceConfiguration,
    return_none_for_unknown_union_types: bool = False,
    header_provider: HeaderProvider | None = None,
    transport_provider: TransportProvider | None = None,
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
        header_provider: Additional default headers to attach to each request.
        transport_provider: Optional transport provider for authentication. When its
            ``create_requests_session()`` returns a non-None session it is used directly;
            otherwise ``create_ssl_context()`` is called and the default adapter path is used.

    Returns:
        Instantiated conjure client object to hit the API with
    """
    verify = service_config.security.trust_store_path if service_config.security is not None else None

    # setup retry to match java remoting
    # https://github.com/palantir/http-remoting/tree/3.12.0#quality-of-service-retry-failover-throttling
    retry = RetryWithJitter(
        total=service_config.max_num_retries,
        connect=service_config.max_num_retries,  # Allow connection error retries
        read=service_config.max_num_retries,  # Allow read error retries (e.g., RemoteDisconnected)
        status_forcelist=[308, 429, 503],
        backoff_factor=float(service_config.backoff_slot_size) / 1000,
    )
    # Let the provider substitute a custom adapter when its TLS stack differs from the default
    # (e.g. Windows Schannel for CAC card auth). If it returns None, fall through to the standard
    # NominalRequestsAdapter backed by the provider's ssl_context (or a ThreadSafeSSLContext when
    # no provider is given). The adapter is mounted only for the service URIs so that non-API
    # traffic (e.g. S3 multipart uploads) is unaffected.
    transport_adapter: HTTPAdapter = (
        transport_provider.create_http_adapter(max_retries=retry)
        if transport_provider is not None
        else None
    )
    if transport_adapter is None:
        ssl_context = transport_provider.create_ssl_context() if transport_provider is not None else None
        transport_adapter = NominalRequestsAdapter(max_retries=retry, ssl_context=ssl_context)

    session = HeaderProviderSession(header_provider)
    for uri in service_config.uris:
        session.mount(uri, transport_adapter)

    session.headers = CaseInsensitiveDict({"User-Agent": user_agent})
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
    header_provider: HeaderProvider | None = None,
    transport_provider: TransportProvider | None = None,
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
            header_provider=header_provider,
            transport_provider=transport_provider,
        )

    return factory


def create_multipart_request_session(
    *,
    pool_size: int = DEFAULT_POOLSIZE,
    num_retries: int = 5,
    header_provider: HeaderProvider | None = None,
    transport_provider: TransportProvider | None = None,
) -> requests.Session:
    """Create a requests Session configured for multipart uploads to S3.

    Each call produces an independent session safe for concurrent use across threads.
    S3 pre-signed URLs use AWS auth and do not require client certificates, so this
    session always uses the SSL context path regardless of the transport provider.

    Args:
        pool_size: Number of concurrent workers. Controls the number of cached host pools
            and the per-host connection limit (2 * pool_size).
        num_retries: Number of times to retry failed requests.
        header_provider: Additional default headers to attach to every request issued by the session.
        transport_provider: Optional transport provider for authentication. Its
            ``create_ssl_context()`` is called to supply the TLS context for S3 connections.
    """
    if pool_size <= 0:
        raise ValueError(f"pool_size must be positive, got {pool_size}")

    retries = Retry(
        total=num_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
    )
    ssl_context = transport_provider.create_ssl_context() if transport_provider is not None else None
    session = HeaderProviderSession(header_provider)
    adapter = NominalSslRequestsAdapter(
        max_retries=retries,
        # Match the number of cached host pools to the thread count to avoid LRU eviction.
        pool_connections=pool_size,
        # Double the per-host connection limit so retries/redirects don't discard connections.
        pool_maxsize=pool_size * 2,
        ssl_context=ssl_context,
    )
    session.mount("https://", adapter)
    return session
