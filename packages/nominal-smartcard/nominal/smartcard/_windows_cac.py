from __future__ import annotations

import gzip
import io
import ipaddress
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import requests
from requests.adapters import CaseInsensitiveDict, HTTPAdapter
from requests.utils import select_proxy
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, ReadTimeoutError, ResponseError
from urllib3.util.retry import Retry

from nominal.core._utils.networking import GZIP_COMPRESSION_LEVEL

NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR = "NOMINAL_WINDOWS_CERT_THUMBPRINT"

# Headers that .NET HttpClient manages internally; must not be forwarded by Python.
_SKIP_HEADERS: frozenset[str] = frozenset(
    {"content-length", "host", "connection", "transfer-encoding", "accept-encoding"}
)


@dataclass(frozen=True)
class _TrustConfig:
    cache_key: tuple[Any, ...]
    ca_certificates: tuple[bytes, ...] | None = None
    disable_server_certificate_validation: bool = False


class _RawResponseBody(io.BytesIO):
    """BytesIO variant that supports urllib3's decode_content attribute."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.decode_content: bool = False


class _RetryResponse:
    """Minimal urllib3 response shape consumed by Retry."""

    def __init__(self, *, status: int, headers: Mapping[str, str]) -> None:
        self.status = status
        self.headers = headers

    def get_redirect_location(self) -> str | None:
        return None


_PEM_CERTIFICATE_PATTERN = re.compile(
    rb"-----BEGIN CERTIFICATE-----\s*(?P<body>.*?)\s*-----END CERTIFICATE-----",
    re.DOTALL,
)


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


def _certificate_blobs_from_file(path: Path) -> tuple[bytes, ...]:
    import base64
    import binascii

    data = path.read_bytes()
    matches = list(_PEM_CERTIFICATE_PATTERN.finditer(data))
    if not matches:
        return (data,) if data else ()

    blobs: list[bytes] = []
    for match in matches:
        b64 = re.sub(rb"\s+", b"", match.group("body"))
        try:
            blobs.append(base64.b64decode(b64, validate=True))
        except binascii.Error as exc:
            raise OSError(f"Could not parse TLS CA certificate bundle: {path}") from exc
    return tuple(blobs)


def _certificate_blobs_from_path(path: Path) -> tuple[bytes, ...]:
    if path.is_dir():
        blobs: list[bytes] = []
        for child in sorted(path.iterdir()):
            if child.is_file():
                try:
                    blobs.extend(_certificate_blobs_from_file(child))
                except OSError:
                    continue
        if not blobs:
            raise OSError(f"Could not find any TLS CA certificates in directory: {path}")
        return tuple(blobs)
    return _certificate_blobs_from_file(path)


def _trust_config_from_verify(verify: bool | str | os.PathLike[str] | None) -> _TrustConfig:
    if not verify:
        return _TrustConfig(cache_key=("insecure",), disable_server_certificate_validation=True)

    if verify is True:
        # Let Schannel validate the server certificate using the Windows certificate store.
        # On a standard CAC install the DoD root and intermediate CAs are already trusted
        # there, so no custom Python-level callback is needed or desired.
        return _TrustConfig(cache_key=("system",))

    # Explicit custom CA bundle path — load it and install a Python validation callback.
    cert_path = Path(os.fspath(verify))
    if not cert_path.exists():
        raise OSError(f"Could not find a suitable TLS CA certificate bundle, invalid path: {verify}")

    stat = cert_path.stat()
    return _TrustConfig(
        cache_key=("ca-bundle", str(cert_path.resolve()), stat.st_mtime_ns, stat.st_size),
        ca_certificates=_certificate_blobs_from_path(cert_path),
    )


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

    compressed = gzip.compress(raw, compresslevel=GZIP_COMPRESSION_LEVEL)
    return compressed, {"Content-Encoding": "gzip"}


def _forwardable_headers(request: requests.PreparedRequest) -> dict[str, str]:
    headers: dict[str, str] = {}
    for k, v in request.headers.items():
        key = k.decode("ascii", errors="replace") if isinstance(k, bytes) else str(k)
        val = v.decode("ascii", errors="replace") if isinstance(v, bytes) else str(v)
        if key.lower() not in _SKIP_HEADERS:
            headers[key] = val
    return headers


def _configure_server_certificate_validation(
    *,
    clr_module: Any,
    handler: Any,
    ca_certificates: tuple[bytes, ...] | None,
    disable_server_certificate_validation: bool,
) -> None:
    if disable_server_certificate_validation:
        handler.ServerCertificateCustomValidationCallback = lambda *_args: True
        return
    if ca_certificates is None:
        return

    clr_module.AddReference("System.Security")
    from cryptography import x509
    from cryptography.x509 import verification
    from System.Net.Security import SslPolicyErrors  # type: ignore[import]
    from System.Security.Cryptography.X509Certificates import X509ContentType  # type: ignore[import]

    roots = [x509.load_der_x509_certificate(blob) for blob in ca_certificates]
    store = verification.Store(roots)
    certificate_missing = int(SslPolicyErrors.RemoteCertificateNotAvailable)

    def certificate_from_dotnet(certificate: Any) -> x509.Certificate:
        return x509.load_der_x509_certificate(bytes(certificate.Export(X509ContentType.Cert)))

    def subject_for_request(request: Any) -> x509.DNSName | x509.IPAddress:
        host = str(request.RequestUri.Host)
        try:
            return x509.IPAddress(ipaddress.ip_address(host))
        except ValueError:
            return x509.DNSName(host)

    def validate_server_certificate(_request: Any, certificate: Any, chain: Any, errors: Any) -> bool:
        if int(errors) & certificate_missing:
            return False
        try:
            if chain is not None:
                intermediates = [certificate_from_dotnet(element.Certificate) for element in chain.ChainElements]
            else:
                intermediates = []
            leaf = certificate_from_dotnet(certificate)
            intermediates = [intermediate for intermediate in intermediates if intermediate != leaf]
            verifier = verification.PolicyBuilder().store(store).build_server_verifier(subject_for_request(_request))
            verifier.verify(leaf, intermediates)
            return True
        except Exception:
            return False

    handler.ServerCertificateCustomValidationCallback = validate_server_certificate


def _configure_client_certificate(*, clr_module: Any, handler: Any, cert_thumbprint: str | None) -> None:
    from System.Net.Http import ClientCertificateOption  # type: ignore[import]

    if not cert_thumbprint:
        handler.ClientCertificateOptions = ClientCertificateOption.Automatic
        return

    clr_module.AddReference("System.Security")
    from System.Security.Cryptography.X509Certificates import (  # type: ignore[import]
        OpenFlags,
        StoreLocation,
        StoreName,
        X509FindType,
        X509Store,
    )

    store = X509Store(StoreName.My, StoreLocation.CurrentUser)
    store.Open(OpenFlags.ReadOnly)
    try:
        thumbprint = cert_thumbprint.replace(" ", "").upper()
        matches = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, False)
        if matches.Count < 1:
            raise RuntimeError(f"Could not find certificate thumbprint {thumbprint!r} in CurrentUser\\My")
        handler.ClientCertificateOptions = ClientCertificateOption.Manual
        handler.ClientCertificates.Add(matches[0])
    finally:
        store.Close()


def _build_http_client(
    *,
    cert_thumbprint: str | None = None,
    ca_certificates: tuple[bytes, ...] | None = None,
    disable_server_certificate_validation: bool = False,
    proxy_url: str | None = None,
) -> Any:
    r"""Create and return a .NET ``System.Net.Http.HttpClient`` for Schannel CAC auth.

    All pythonnet / .NET imports happen here so this module can be imported on
    non-Windows platforms without raising ``ImportError``.

    Args:
        cert_thumbprint: Optional thumbprint of the certificate to use (spaces
            ignored, case-insensitive). When supplied the matching certificate in
            ``CurrentUser\My`` is attached manually to the handler. When omitted,
            ``ClientCertificateOption.Automatic`` lets Schannel select the correct
            CAC certificate for each TLS handshake automatically.
        ca_certificates: Optional DER-encoded CA certificates loaded from the
            caller's ``verify`` bundle.
        disable_server_certificate_validation: Whether to accept any server
            certificate, matching ``requests`` when ``verify=False``.
        proxy_url: Optional proxy URL selected from the ``proxies`` mapping.
    """
    import clr  # noqa: PLC0415

    clr.AddReference("System.Net.Http")

    from System import TimeSpan  # type: ignore[import]
    from System.Net import DecompressionMethods, WebProxy  # type: ignore[import]
    from System.Net.Http import HttpClient, HttpClientHandler  # type: ignore[import]

    handler = HttpClientHandler()
    handler.AllowAutoRedirect = False
    handler.UseProxy = proxy_url is not None
    if proxy_url is not None:
        handler.Proxy = WebProxy(proxy_url)
    # AutomaticDecompression causes .NET to add its own Accept-Encoding header and
    # decompress the response transparently. Accept-Encoding must not be forwarded
    # from Python (it is listed in _SKIP_HEADERS).
    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate

    _configure_server_certificate_validation(
        clr_module=clr,
        handler=handler,
        ca_certificates=ca_certificates,
        disable_server_certificate_validation=disable_server_certificate_validation,
    )
    _configure_client_certificate(clr_module=clr, handler=handler, cert_thumbprint=cert_thumbprint)

    client = HttpClient(handler)
    # Infinite timeout on the shared client; per-request timeouts are enforced via
    # CancellationTokenSource in _dotnet_send so the client can be reused safely.
    client.Timeout = TimeSpan.InfiniteTimeSpan
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

        return (
            int(net_response.StatusCode),
            net_response.ReasonPhrase or "",
            resp_headers,
            body,
            str(net_response.RequestMessage.RequestUri.AbsoluteUri),
        )
    except requests.exceptions.RequestException:
        raise
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

        retry_response = _RetryResponse(status=status_code, headers=resp_headers)
        has_retry_after = any(k.lower() == "retry-after" for k in resp_headers)
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

    Certificate selection:
        By default Schannel automatically selects the appropriate client-auth
        certificate for each TLS handshake. To pin a specific certificate, set the
        ``NOMINAL_WINDOWS_CERT_THUMBPRINT`` environment variable to its thumbprint
        (spaces ignored, case-insensitive) before constructing the adapter.

    Lifecycle:
        The underlying ``HttpClient`` is created once at construction time and
        reused across all requests, following .NET best practices for connection
        pooling. Call ``close()`` to dispose it when the adapter is no longer needed.

    Compression:
        Non-streaming request bodies are gzip-compressed before sending, mirroring
        the behaviour of ``NominalRequestsAdapter`` on non-Windows platforms.

    Retries:
        The adapter honors the ``Retry`` instance passed as ``max_retries`` for
        transport failures and retryable HTTP status codes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cert_thumbprint = os.environ.get(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR) or None
        self._net_clients: dict[tuple[Any, ...], Any] = {}
        self._net_clients_lock = threading.Lock()

    def _get_http_client(
        self,
        *,
        verify: bool | str | os.PathLike[str] | None,
        proxy_url: str | None,
    ) -> Any:
        trust_config = _trust_config_from_verify(verify)
        cache_key = (*trust_config.cache_key, proxy_url)
        with self._net_clients_lock:
            client = self._net_clients.get(cache_key)
            if client is None:
                client = _build_http_client(
                    cert_thumbprint=self._cert_thumbprint,
                    ca_certificates=trust_config.ca_certificates,
                    disable_server_certificate_validation=trust_config.disable_server_certificate_validation,
                    proxy_url=proxy_url,
                )
                self._net_clients[cache_key] = client
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
        proxy_url = select_proxy(str(request.url), proxies)
        client = self._get_http_client(verify=verify, proxy_url=proxy_url)
        body_bytes, body_headers = _body_bytes_for_request(request)
        forwarded_headers = _forwardable_headers(request)
        forwarded_headers.update(body_headers)
        status_code, reason, resp_headers, resp_body, final_url = _dotnet_send_with_retries(
            client=client,
            request=request,
            headers=forwarded_headers,
            body_bytes=body_bytes,
            timeout_seconds=_timeout_to_seconds(timeout),
            retries=self.max_retries,
        )

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
            clients = list(self._net_clients.values())
            self._net_clients.clear()
        for client in clients:
            client.Dispose()
        super().close()
