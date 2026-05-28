from __future__ import annotations

import gzip
import os
from typing import Any, Mapping

import requests
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from nominal.core._utils.networking import GZIP_COMPRESSION_LEVEL

NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR = "NOMINAL_WINDOWS_CERT_THUMBPRINT"

# Headers that .NET HttpClient manages internally; must not be forwarded by Python.
_SKIP_HEADERS: frozenset[str] = frozenset(
    {"content-length", "host", "connection", "transfer-encoding", "accept-encoding"}
)


def _timeout_to_seconds(timeout: object) -> float:
    if isinstance(timeout, tuple):
        values = [v for v in timeout if v is not None]
        return max(float(max(values)) if values else 300.0, 300.0)
    if timeout is None:
        return 300.0
    return max(float(timeout), 300.0)


def _build_http_client(cert_thumbprint: str | None = None) -> Any:
    r"""Create and return a .NET ``System.Net.Http.HttpClient`` for Schannel CAC auth.

    All pythonnet / .NET imports happen here so this module can be imported on
    non-Windows platforms without raising ``ImportError``.

    Args:
        cert_thumbprint: Optional thumbprint of the certificate to use (spaces
            ignored, case-insensitive). When supplied the matching certificate in
            ``CurrentUser\My`` is attached manually to the handler. When omitted,
            ``ClientCertificateOption.Automatic`` lets Schannel select the correct
            CAC certificate for each TLS handshake automatically.
    """
    import clr  # noqa: PLC0415

    clr.AddReference("System.Net.Http")

    from System import TimeSpan  # type: ignore[import]
    from System.Net import DecompressionMethods  # type: ignore[import]
    from System.Net.Http import ClientCertificateOption, HttpClient, HttpClientHandler  # type: ignore[import]

    handler = HttpClientHandler()
    handler.UseProxy = True
    # AutomaticDecompression causes .NET to add its own Accept-Encoding header and
    # decompress the response transparently. Accept-Encoding must not be forwarded
    # from Python (it is listed in _SKIP_HEADERS).
    handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate

    if cert_thumbprint:
        clr.AddReference("System.Security")
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
                raise RuntimeError(
                    f"Could not find certificate thumbprint {thumbprint!r} in CurrentUser\\My"
                )
            handler.ClientCertificateOptions = ClientCertificateOption.Manual
            handler.ClientCertificates.Add(matches[0])
        finally:
            store.Close()
    else:
        handler.ClientCertificateOptions = ClientCertificateOption.Automatic

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
        raise requests.exceptions.ConnectionError(
            f"Windows CAC request failed: {method} {url}: {exc}"
        ) from exc
    finally:
        cts.Dispose()
        if net_response is not None:
            net_response.Dispose()


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
        Request bodies are gzip-compressed before sending, mirroring the behaviour
        of ``NominalRequestsAdapter`` on non-Windows platforms.

    Retries:
        Not supported — each call to ``send()`` is a single attempt regardless of
        the ``max_retries`` value passed at construction time.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        thumbprint = os.environ.get(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR) or None
        self._net_client = _build_http_client(cert_thumbprint=thumbprint)

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str = True,
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: Mapping[str, str] | None = None,
    ) -> requests.Response:
        # Gzip-compress the request body before sending.
        body = request.body
        if body is not None and not stream:
            raw: bytes = body if isinstance(body, bytes) else body.encode("utf-8")
            compressed = gzip.compress(raw, compresslevel=GZIP_COMPRESSION_LEVEL)
            request.headers["Content-Encoding"] = "gzip"
            request.headers["Content-Length"] = str(len(compressed))
            body_bytes: bytes | None = compressed
        else:
            body_bytes = None

        # Forward headers to .NET, skipping those managed by the transport layer.
        forward_headers: dict[str, str] = {}
        for k, v in request.headers.items():
            key = k.decode("ascii", errors="replace") if isinstance(k, bytes) else str(k)
            val = v.decode("ascii", errors="replace") if isinstance(v, bytes) else str(v)
            if key.lower() not in _SKIP_HEADERS:
                forward_headers[key] = val

        timeout_seconds = _timeout_to_seconds(timeout)
        status_code, reason, resp_headers, resp_body, final_url = _dotnet_send(
            self._net_client,
            str(request.method),
            str(request.url),
            forward_headers,
            body_bytes,
            timeout_seconds,
        )

        response = requests.Response()
        response.status_code = status_code
        response.reason = reason
        response.headers = CaseInsensitiveDict(resp_headers)
        response._content = resp_body  # type: ignore[attr-defined]
        response.url = final_url
        response.request = request
        response.encoding = requests.utils.get_encoding_from_headers(response.headers)
        return response

    def close(self) -> None:
        """Dispose the underlying .NET ``HttpClient`` and release its connections."""
        self._net_client.Dispose()
        super().close()
