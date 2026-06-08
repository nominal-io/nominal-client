from __future__ import annotations

import gzip
import io
import os
import threading
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

import certifi
import requests
from requests.adapters import CaseInsensitiveDict, HTTPAdapter
from requests.utils import get_auth_from_url, select_proxy
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, ReadTimeoutError, ResponseError
from urllib3.util.retry import Retry

from nominal.smartcard._errors import SmartcardConfigurationError

_GZIP_COMPRESSION_LEVEL = 1

# Headers that .NET HttpClient manages internally; must not be forwarded by Python.
_SKIP_HEADERS: frozenset[str] = frozenset(
    {"content-length", "host", "connection", "transfer-encoding", "accept-encoding"}
)


class _RawResponseBody(io.BytesIO):
    """A ``BytesIO`` that tolerates the extra keyword arguments urllib3 passes to ``read``.

    Streaming endpoints set ``raw.decode_content`` and may call ``raw.read(decode_content=...)``,
    but plain ``BytesIO.read`` rejects that keyword. The body is already decompressed by .NET, so we
    accept and ignore ``decode_content``/``cache_content`` to match urllib3's ``read`` signature.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.decode_content: bool = False

    def read(self, amt: int | None = None, decode_content: bool | None = None, cache_content: bool = False) -> bytes:
        return super().read(amt)


class _RetryResponse:
    """Minimal urllib3 response shape consumed by Retry."""

    def __init__(self, *, status: int, headers: Mapping[str, str]) -> None:
        self.status = status
        self.headers = headers

    def get_redirect_location(self) -> str | None:
        return None


def _timeout_to_seconds(timeout: float | tuple[float | None, float | None] | None) -> float:
    """Convert a requests-style timeout to a float number of seconds.

    ``None`` maps to the 300-second default for this adapter. Explicit values
    are forwarded as-is so callers can set short timeouts for fast failure.
    For tuple ``(connect, read)`` timeouts the larger value is used since the
    underlying .NET HttpClient does not support per-phase timeouts.
    """
    if isinstance(timeout, tuple):
        values = [v for v in timeout if v is not None]
        return float(max(values)) if values else 300.0
    if timeout is None:
        return 300.0
    return float(timeout)


def _body_bytes_for_request(
    request: requests.PreparedRequest, *, stream: bool = False
) -> tuple[bytes | None, dict[str, str]]:
    """Return ``(body_bytes, extra_headers)`` for the outgoing request.

    ``extra_headers`` contains any compression headers that must be merged into
    the forwarded header dict. This function never mutates ``request.headers``
    so that the same ``PreparedRequest`` can be re-sent without corruption.
    Skips compression when the caller already set a ``Content-Encoding`` header.
    """
    body = request.body
    if body is None:
        return None, {}
    if stream:
        return body if isinstance(body, bytes) else body.encode("utf-8"), {}

    raw: bytes = body if isinstance(body, bytes) else body.encode("utf-8")

    # Don't double-compress if the caller already set Content-Encoding.
    if request.headers.get("Content-Encoding"):
        return raw, {}

    compressed = gzip.compress(raw, compresslevel=_GZIP_COMPRESSION_LEVEL)
    return compressed, {"Content-Encoding": "gzip"}


def _forwardable_headers(request: requests.PreparedRequest) -> dict[str, str]:
    headers: dict[str, str] = {}
    for k, v in request.headers.items():
        key = k.decode("ascii", errors="replace") if isinstance(k, bytes) else str(k)
        val = v.decode("ascii", errors="replace") if isinstance(v, bytes) else str(v)
        if key.lower() not in _SKIP_HEADERS:
            headers[key] = val
    return headers


def _assert_supported_verify(verify: bool | str | os.PathLike[str] | None) -> None:
    if verify is None or verify is True or verify is False:
        return

    try:
        verify_path = os.path.abspath(os.fspath(verify))
    except TypeError as exc:
        raise SmartcardConfigurationError(
            f"Windows smartcard transport only supports Schannel's Windows trust store. Unsupported verify value: {verify!r}."
        ) from exc

    try:
        default_certifi_path = os.path.abspath(certifi.where())
    except Exception:
        default_certifi_path = None
    if verify_path == default_certifi_path:
        return

    raise SmartcardConfigurationError(
        "Windows smartcard transport uses Schannel and cannot honor a custom Python CA bundle "
        f"({verify_path}). Import that CA into the Windows certificate store, or omit trust_store_path "
        "so Schannel validates against Windows trust."
    )


def _enable_modern_tls() -> None:
    r"""Ensure TLS 1.2 (and 1.3 when available) is enabled for the process."""
    from System.Net import SecurityProtocolType, ServicePointManager  # type: ignore[import-not-found]

    protocol = SecurityProtocolType.Tls12
    tls13 = getattr(SecurityProtocolType, "Tls13", None)
    if tls13 is not None:
        protocol |= tls13
    # OR the modern protocols onto whatever is already enabled rather than replacing it.
    ServicePointManager.SecurityProtocol |= protocol


def _build_web_proxy(proxy_url: str, *, WebProxy: Any, NetworkCredential: Any) -> Any:
    r"""Build a .NET ``WebProxy`` from a requests-style proxy URL, forwarding embedded credentials."""
    username, password = get_auth_from_url(proxy_url)
    parts = urlsplit(proxy_url)

    # Strip any "userinfo@" prefix so WebProxy receives only scheme://host:port.
    address = urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, parts.query, parts.fragment))
    proxy = WebProxy(address)
    if username or password:
        proxy.Credentials = NetworkCredential(username, password)
    return proxy


def _build_http_client(*, client_certificate: Any, proxy_url: str | None = None) -> Any:
    r"""Create and return a .NET ``System.Net.Http.HttpClient`` for Schannel smartcard auth.

    Args:
        client_certificate: Selected .NET ``X509Certificate2`` client certificate.
        proxy_url: Optional proxy URL selected from the ``proxies`` mapping.
    """
    import clr  # type: ignore[import-untyped]  # noqa: PLC0415

    clr.AddReference("System.Net.Http")

    from System import TimeSpan  # type: ignore[import-not-found]
    from System.Net import DecompressionMethods, NetworkCredential, WebProxy
    from System.Net.Http import ClientCertificateOption, HttpClient, HttpClientHandler  # type: ignore[import-not-found]

    _enable_modern_tls()

    handler = HttpClientHandler()
    handler.AllowAutoRedirect = False
    handler.UseProxy = proxy_url is not None
    if proxy_url is not None:
        handler.Proxy = _build_web_proxy(proxy_url, WebProxy=WebProxy, NetworkCredential=NetworkCredential)
    # AutomaticDecompression causes .NET to add its own Accept-Encoding header and
    # decompress the response transparently. Accept-Encoding must not be forwarded
    # from Python (it is listed in _SKIP_HEADERS).
    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate

    handler.ClientCertificateOptions = ClientCertificateOption.Manual
    handler.ClientCertificates.Add(client_certificate)

    client = HttpClient(handler)

    # TimeSpan.FromMilliseconds(-1) is the cross-framework sentinel for "no timeout"
    # (TimeSpan.InfiniteTimeSpan was only added in .NET 6).
    client.Timeout = TimeSpan.FromMilliseconds(-1)
    return client


def _dotnet_send(
    client: Any,
    method: str,
    url: str,
    headers: dict[str, str],
    body_bytes: bytes | None,
    timeout_seconds: float,
) -> tuple[int, str, dict[str, str], bytes, str]:
    """Send an HTTP request through the provided .NET ``HttpClient``.

    Returns ``(status_code, reason, response_headers, body, final_url)``.

    Raises:
        requests.exceptions.Timeout: when the per-request timeout elapses.
        requests.exceptions.ConnectionError: on any other transport failure.
    """
    from System import Array, Byte, Uri
    from System.Net.Http import ByteArrayContent, HttpMethod, HttpRequestMessage
    from System.Threading import CancellationTokenSource  # type: ignore[import-not-found]

    message = HttpRequestMessage(HttpMethod(method), Uri(url))
    if body_bytes is not None:
        message.Content = ByteArrayContent(Array[Byte](body_bytes))

    for k, v in headers.items():
        if not message.Headers.TryAddWithoutValidation(k, v):
            if message.Content is not None:
                message.Content.Headers.TryAddWithoutValidation(k, v)

    cts = CancellationTokenSource()
    net_response = None
    try:
        cts.CancelAfter(int(timeout_seconds * 1000))
        net_response = client.SendAsync(message, cts.Token).GetAwaiter().GetResult()

        body = bytes(net_response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult())

        resp_headers: dict[str, str] = {}
        for header in net_response.Headers:
            resp_headers[header.Key] = ", ".join(str(v) for v in header.Value)
        for header in net_response.Content.Headers:
            resp_headers[header.Key] = ", ".join(str(v) for v in header.Value)

        return (
            int(net_response.StatusCode),
            net_response.ReasonPhrase or "",
            resp_headers,
            body,
            url,
        )
    except Exception as exc:
        if cts.IsCancellationRequested:
            raise requests.exceptions.Timeout(
                f"Windows smartcard request timed out after {timeout_seconds:g}s: {method} {url}"
            ) from exc
        raise requests.exceptions.ConnectionError(f"Windows smartcard request failed: {method} {url}: {exc}") from exc
    finally:
        cts.Dispose()
        if net_response is not None:
            net_response.Dispose()
        message.Dispose()


def _dotnet_send_with_retries(
    *,
    client: Any,
    request: requests.PreparedRequest,
    headers: dict[str, str],
    body_bytes: bytes | None,
    timeout_seconds: float,
    retries: Retry,
) -> tuple[int, str, dict[str, str], bytes, str]:
    method = str(request.method)
    url = str(request.url)

    while True:
        try:
            status_code, reason, resp_headers, resp_body, final_url = _dotnet_send(
                client,
                method,
                url,
                headers,
                body_bytes,
                timeout_seconds,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            retry_error = (
                ReadTimeoutError(None, url, str(exc))  # type: ignore[arg-type]
                if isinstance(exc, requests.exceptions.Timeout)
                else ConnectTimeoutError(None, str(exc))
            )
            try:
                retries = retries.increment(method=method, url=url, error=retry_error)
            except MaxRetryError as max_exc:
                # Retry budget exhausted: attach the PreparedRequest and re-raise the
                # original exception so callers can inspect exc.request and the message.
                exc.request = request
                raise exc from max_exc
            except Exception:
                # Retry.increment may reraise the urllib3 error directly (e.g.
                # ReadTimeoutError when read=False). Fall back to re-raising the
                # original requests exception.
                raise exc
            retries.sleep()
            continue

        retry_headers = CaseInsensitiveDict(resp_headers)
        retry_response = _RetryResponse(status=status_code, headers=retry_headers)
        has_retry_after = "retry-after" in retry_headers
        if retries.is_retry(method, status_code, has_retry_after=has_retry_after):
            try:
                retries = retries.increment(method=method, url=url, response=retry_response)  # type: ignore[arg-type]
            except MaxRetryError as exc:
                if isinstance(exc.reason, ResponseError):
                    raise requests.exceptions.RetryError(exc, request=request) from exc
                raise requests.exceptions.ConnectionError(exc, request=request) from exc
            retries.sleep(retry_response)  # type: ignore[arg-type]
            continue
        return status_code, reason, resp_headers, resp_body, final_url


class WindowsHttpAdapter(HTTPAdapter):
    r"""requests HTTPAdapter backed by the Windows .NET HttpClient + Schannel smartcard transport.

    Uses pythonnet to call ``System.Net.Http.HttpClient`` directly. The transport
    provider selects one Windows client-auth certificate and passes it here; Schannel
    performs the TLS handshake and PIN prompting through the Windows credential UI.
    """

    def __init__(self, *args: Any, client_certificate: Any, **kwargs: Any) -> None:
        self._client_certificate = client_certificate
        super().__init__(*args, **kwargs)
        self._net_clients: dict[Any, Any] = {}
        self._net_clients_lock = threading.Lock()
        self._closed = False
        self._tls_warmed = False
        self._tls_warm_lock = threading.Lock()

    def _get_http_client(self, *, proxy_url: str | None) -> Any:
        with self._net_clients_lock:
            client = self._net_clients.get(proxy_url)
            if client is None:
                client = _build_http_client(client_certificate=self._client_certificate, proxy_url=proxy_url)
                self._net_clients[proxy_url] = client
            return client

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str | os.PathLike[str] | None = True,
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: Mapping[str, str] | None = None,
    ) -> requests.Response:
        if self._closed:
            raise RuntimeError("WindowsHttpAdapter is closed and cannot send requests.")
        _assert_supported_verify(verify)
        proxy_url = select_proxy(str(request.url), proxies)
        client = self._get_http_client(proxy_url=proxy_url)
        body_bytes, body_headers = _body_bytes_for_request(request, stream=stream)
        forwarded_headers = _forwardable_headers(request)
        forwarded_headers.update(body_headers)
        timeout_seconds = _timeout_to_seconds(timeout)

        # Serialize the first request so Schannel prompts for and caches the PIN exactly once.
        # We mark the adapter warmed only after a request *succeeds*: a failed or cancelled first
        # attempt (e.g. the user dismisses the PIN dialog) must not open the gate, or the next
        # burst of threads would each trigger a fresh prompt. The lock holds other callers until
        # the warming request finishes; if it raises, the next caller becomes the new warmer.
        if not self._tls_warmed:
            with self._tls_warm_lock:
                if not self._tls_warmed:
                    response = self._send_once(client, request, forwarded_headers, body_bytes, timeout_seconds)
                    self._tls_warmed = True
                    return response

        return self._send_once(client, request, forwarded_headers, body_bytes, timeout_seconds)

    def _send_once(
        self,
        client: Any,
        request: requests.PreparedRequest,
        headers: dict[str, str],
        body_bytes: bytes | None,
        timeout_seconds: float,
    ) -> requests.Response:
        status_code, reason, resp_headers, resp_body, final_url = _dotnet_send_with_retries(
            client=client,
            request=request,
            headers=headers,
            body_bytes=body_bytes,
            timeout_seconds=timeout_seconds,
            retries=self.max_retries,
        )
        return self._build_response(request, status_code, reason, resp_headers, resp_body, final_url)

    def _build_response(
        self,
        request: requests.PreparedRequest,
        status_code: int,
        reason: str,
        resp_headers: dict[str, str],
        resp_body: bytes,
        final_url: str,
    ) -> requests.Response:
        response = requests.Response()
        response.status_code = status_code
        response.reason = reason
        response.headers = CaseInsensitiveDict(resp_headers)
        response._content = resp_body
        response.raw = _RawResponseBody(resp_body)
        response.url = final_url
        response.request = request
        response.encoding = requests.utils.get_encoding_from_headers(response.headers)
        return response

    def close(self) -> None:
        """Dispose the underlying .NET ``HttpClient`` and release its connections."""
        with self._net_clients_lock:
            self._closed = True
            clients = list(self._net_clients.values())
            self._net_clients.clear()
        for client in clients:
            client.Dispose()
        super().close()  # type: ignore[no-untyped-call]
