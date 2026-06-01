from __future__ import annotations

import gzip
import io
import os
import threading
from typing import Any, Mapping

import requests
from requests.adapters import CaseInsensitiveDict, HTTPAdapter
from requests.utils import select_proxy
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, ReadTimeoutError, ResponseError
from urllib3.util.retry import Retry

_GZIP_COMPRESSION_LEVEL = 1

# Headers that .NET HttpClient manages internally; must not be forwarded by Python.
_SKIP_HEADERS: frozenset[str] = frozenset(
    {"content-length", "host", "connection", "transfer-encoding", "accept-encoding"}
)


class _RawResponseBody(io.BytesIO):
    """BytesIO that duck-types as a urllib3 response body for ``response.raw`` consumers.

    Streaming endpoints set ``raw.decode_content`` and may call ``raw.read(decode_content=...)``;
    plain ``BytesIO.read`` rejects that keyword. The body is already decompressed by .NET, so we
    accept and ignore ``decode_content``/``cache_content`` to match urllib3's ``read`` signature.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.decode_content: bool = False

    def read(self, amt: int | None = None, decode_content: bool | None = None, cache_content: bool = False) -> bytes:  # type: ignore[override]
        return super().read(amt)


class _RetryResponse:
    """Minimal urllib3 response shape consumed by Retry."""

    def __init__(self, *, status: int, headers: Mapping[str, str]) -> None:
        self.status = status
        self.headers = headers

    def get_redirect_location(self) -> str | None:
        return None


def _timeout_to_seconds(timeout: object) -> float:
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


def _body_bytes_for_request(request: requests.PreparedRequest) -> tuple[bytes | None, dict[str, str]]:
    """Return ``(body_bytes, extra_headers)`` for the outgoing request.

    ``extra_headers`` contains any compression headers that must be merged into
    the forwarded header dict. This function never mutates ``request.headers``
    so that the same ``PreparedRequest`` can be re-sent without corruption.
    Skips compression when the caller already set a ``Content-Encoding`` header.
    """
    body = request.body
    if body is None:
        return None, {}

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


def _build_http_client(*, proxy_url: str | None = None) -> Any:
    r"""Create and return a .NET ``System.Net.Http.HttpClient`` for Schannel CAC auth.

    Args:
        proxy_url: Optional proxy URL selected from the ``proxies`` mapping.
    """
    import clr  # noqa: PLC0415

    clr.AddReference("System.Net.Http")

    from System import TimeSpan  # type: ignore[import]
    from System.Net import DecompressionMethods, SecurityProtocolType, ServicePointManager, WebProxy  # type: ignore[import]
    from System.Net.Http import HttpClient, HttpClientHandler  # type: ignore[import]

    # .NET Framework's HttpClient delegates to HttpWebRequest internally, and its default
    # security protocol is TLS 1.0/1.1 on older framework versions, which modern servers
    # reject. Explicitly opt in to TLS 1.2 (and 1.3 when available) globally.
    _tls_flags = int(SecurityProtocolType.Tls12)
    try:
        _tls_flags |= int(SecurityProtocolType.Tls13)
    except AttributeError:
        pass
    ServicePointManager.SecurityProtocol = SecurityProtocolType(_tls_flags)

    handler = HttpClientHandler()
    handler.AllowAutoRedirect = False
    handler.UseProxy = proxy_url is not None
    if proxy_url is not None:
        handler.Proxy = WebProxy(proxy_url)
    # AutomaticDecompression causes .NET to add its own Accept-Encoding header and
    # decompress the response transparently. Accept-Encoding must not be forwarded
    # from Python (it is listed in _SKIP_HEADERS).
    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate

    from System.Net.Http import ClientCertificateOption  # type: ignore[import]

    handler.ClientCertificateOptions = ClientCertificateOption.Automatic

    client = HttpClient(handler)
    # Infinite timeout on the shared client; per-request timeouts are enforced via
    # CancellationTokenSource in _dotnet_send so the client can be reused safely.
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
    from System import Array, Byte, Uri  # type: ignore[import]
    from System.Net.Http import ByteArrayContent, HttpMethod, HttpRequestMessage  # type: ignore[import]
    from System.Threading import CancellationTokenSource  # type: ignore[import]

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

        # Redirects are disabled (AllowAutoRedirect=False), so the final URL is always the
        # URL we sent; return it directly rather than reaching through RequestMessage.
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
                f"Windows CAC request timed out after {timeout_seconds:g}s: {method} {url}"
            ) from exc
        raise requests.exceptions.ConnectionError(f"Windows CAC request failed: {method} {url}: {exc}") from exc
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
                ReadTimeoutError(None, url, str(exc))
                if isinstance(exc, requests.exceptions.Timeout)
                else ConnectTimeoutError(None, str(exc))
            )
            try:
                retries = retries.increment(method=method, url=url, error=retry_error)
            except MaxRetryError as max_exc:
                # Retry budget exhausted: attach the PreparedRequest and re-raise the
                # original exception so callers can inspect exc.request and the message.
                exc.request = request  # type: ignore[attr-defined]
                raise exc from max_exc
            except Exception:
                # Retry.increment may reraise the urllib3 error directly (e.g.
                # ReadTimeoutError when read=False). Fall back to re-raising the
                # original requests exception.
                raise exc
            retries.sleep()
            continue

        # urllib3's get_retry_after does a case-sensitive headers.get("Retry-After"); a plain
        # dict keyed by the server's casing (lowercase under HTTP/2) would drop the delay and
        # silently fall back to backoff. CaseInsensitiveDict makes the lookup case-insensitive.
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


class WindowsCacAdapter(HTTPAdapter):
    r"""requests HTTPAdapter backed by the Windows .NET HttpClient + Schannel CAC transport.

    Uses pythonnet to call ``System.Net.Http.HttpClient`` directly — no subprocess
    overhead, no PowerShell bridge, no per-request process spawning. The Windows
    certificate store (``CurrentUser\My``) supplies the CAC certificate through
    ``ClientCertificateOption.Automatic``; Schannel handles PIN prompting natively
    via the Windows credential UI so no PIN handling is required in Python.

    Certificate selection is delegated to Schannel by design: it filters
    ``CurrentUser\My`` by the server's acceptable-CA list and client-auth usage. This
    differs from the non-Windows path, which explicitly pins PIV slot 9A. On a card
    exposing multiple client-auth certs Schannel's choice is opaque, but ``.Automatic``
    is the standard CAC approach and correct for the common single-auth-cert case.

    Server certificate validation is always performed by Schannel using the Windows
    trust store. DoD root CAs are already present there on a standard CAC install.
    The ``verify`` argument is ignored: there is no validation-bypass path, so
    ``verify=False`` does not weaken the connection.

    Lifecycle:
        The underlying ``HttpClient`` is created once at construction time and
        reused across all requests, following .NET best practices for connection
        pooling. Call ``close()`` to dispose it when the adapter is no longer needed.

    Compression:
        Request bodies are gzip-compressed before sending, mirroring the behaviour
        of ``NominalRequestsAdapter`` on non-Windows platforms.

    Retries:
        The adapter honors the ``Retry`` instance passed as ``max_retries`` for
        transport failures and retryable HTTP status codes.

    Limitations:
        - Responses are fully buffered into memory; ``stream=True`` does not yield a true
          streaming body, so very large downloads are materialized in full. (This adapter
          carries Nominal API traffic; S3 presigned transfers use a different adapter.)
        - ``Set-Cookie`` is not extracted into the session cookie jar. The Nominal API uses
          header-based auth, so no cookie state is relied upon.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._net_clients: dict[Any, Any] = {}
        self._net_clients_lock = threading.Lock()
        self._closed = False
        # Schannel prompts for PIN on the first TLS handshake. If multiple threads race to send
        # their first request concurrently, each in-flight handshake triggers a separate prompt.
        # Serialize the first request so the PIN is cached once before threads run freely.
        self._tls_warmed = False
        self._tls_warm_lock = threading.Lock()

    def _get_http_client(self, *, proxy_url: str | None) -> Any:
        with self._net_clients_lock:
            client = self._net_clients.get(proxy_url)
            if client is None:
                client = _build_http_client(proxy_url=proxy_url)
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
            raise RuntimeError("WindowsCacAdapter is closed and cannot send requests.")
        proxy_url = select_proxy(str(request.url), proxies)
        client = self._get_http_client(proxy_url=proxy_url)
        body_bytes, body_headers = _body_bytes_for_request(request)
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
        response._content = resp_body  # type: ignore[attr-defined]
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
        super().close()
