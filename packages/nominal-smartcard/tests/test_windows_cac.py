from __future__ import annotations

import gzip
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.util.retry import Retry

from nominal.smartcard._windows_cac import (
    WindowsCacAdapter,
    _timeout_to_seconds,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dotnet_result(
    *,
    status_code: int = 200,
    reason: str = "OK",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
    url: str = "https://api.example.com/",
) -> tuple[int, str, dict[str, str], bytes, str]:
    """Build the (status_code, reason, headers, body, url) tuple that _dotnet_send returns."""
    return status_code, reason, headers or {}, body, url


def _prepared(
    method: str = "GET",
    url: str = "https://api.example.com/",
    body: bytes | str | None = None,
    headers: dict[str, str] | None = None,
) -> requests.PreparedRequest:
    return requests.Request(method, url, data=body, headers=headers or {}).prepare()


@pytest.fixture
def adapter() -> WindowsCacAdapter:
    """A WindowsCacAdapter with the .NET client creation mocked out."""
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        yield WindowsCacAdapter()


def _send_args(mock_dotnet_send: MagicMock) -> tuple[Any, str, str, dict, bytes | None, float]:
    """Extract (client, method, url, headers, body_bytes, timeout_seconds) from the last call."""
    return mock_dotnet_send.call_args.args


# ---------------------------------------------------------------------------
# _timeout_to_seconds
# ---------------------------------------------------------------------------


def test_timeout_none_returns_300() -> None:
    assert _timeout_to_seconds(None) == 300.0


def test_timeout_scalar_below_300_preserved() -> None:
    # Explicit timeouts must not be clamped upward; a caller that sets 10s wants 10s.
    assert _timeout_to_seconds(10) == 10.0


def test_timeout_scalar_above_300_returned() -> None:
    assert _timeout_to_seconds(600) == 600.0


def test_timeout_tuple_uses_max() -> None:
    assert _timeout_to_seconds((60, 400)) == 400.0


def test_timeout_tuple_with_none_ignores_none() -> None:
    assert _timeout_to_seconds((None, 500)) == 500.0


def test_timeout_tuple_small_uses_max() -> None:
    # (connect, read) tuple: use the larger value; both under 300 must not clamp.
    assert _timeout_to_seconds((5, 10)) == 10.0


# ---------------------------------------------------------------------------
# WindowsCacAdapter.send — basic request/response
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_successful_get_returns_parsed_response(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result(status_code=200, body=b'{"ok": true}', url="https://api.example.com/test")
    resp = adapter.send(_prepared("GET", "https://api.example.com/test"))
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_response_headers_are_parsed(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result(headers={"Content-Type": "application/json", "X-Custom": "value"})
    resp = adapter.send(_prepared())
    assert resp.headers["Content-Type"] == "application/json"
    assert resp.headers["X-Custom"] == "value"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_response_url_forwarded(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result(url="https://api.example.com/redirected")
    resp = adapter.send(_prepared())
    assert resp.url == "https://api.example.com/redirected"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_non_200_status_code_preserved(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result(status_code=404, reason="Not Found")
    resp = adapter.send(_prepared("GET", "https://api.example.com/missing"))
    assert resp.status_code == 404
    assert resp.reason == "Not Found"


# ---------------------------------------------------------------------------
# Request body compression
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_post_body_is_gzip_compressed(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared("POST", body=b"hello world" * 100))
    _, _, _, _, body_bytes, _ = _send_args(mock_send)
    assert isinstance(body_bytes, bytes)
    assert gzip.decompress(body_bytes) == b"hello world" * 100


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_post_string_body_is_utf8_then_compressed(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared("POST", body="héllo"))
    _, _, _, _, body_bytes, _ = _send_args(mock_send)
    assert gzip.decompress(body_bytes) == "héllo".encode("utf-8")


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_compression_headers_set_on_post(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    req = _prepared("POST", body=b"payload")
    adapter.send(req)
    _, _, _, forwarded_headers, _, _ = _send_args(mock_send)
    assert forwarded_headers.get("Content-Encoding") == "gzip"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_body_bytes_does_not_mutate_prepared_request_headers(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    # Sending the same PreparedRequest twice must produce the same compressed body both times.
    mock_send.return_value = _dotnet_result()
    original_payload = b"re-send me"
    req = _prepared("POST", body=original_payload)
    assert "Content-Encoding" not in req.headers

    adapter.send(req)
    # Headers on the original request object must not have been mutated.
    assert "Content-Encoding" not in req.headers

    # A second send must still produce compressed bytes (not raw bytes with a stale gzip header).
    mock_send.reset_mock()
    mock_send.return_value = _dotnet_result()
    adapter.send(req)
    _, _, _, forwarded_headers2, body_bytes2, _ = _send_args(mock_send)
    assert gzip.decompress(body_bytes2) == original_payload
    assert forwarded_headers2.get("Content-Encoding") == "gzip"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_get_with_no_body_sends_none_body_bytes(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared())
    _, _, _, _, body_bytes, _ = _send_args(mock_send)
    assert body_bytes is None


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_stream_flag_does_not_suppress_compression(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    # stream=True controls response buffering (which is always buffered in this adapter),
    # not request body compression. The body must still be gzip-compressed.
    mock_send.return_value = _dotnet_result()
    req = _prepared("POST", body=b'{"export": true}', headers={"Content-Type": "application/json"})

    adapter.send(req, stream=True)

    _, _, _, forwarded_headers, body_bytes, _ = _send_args(mock_send)
    assert isinstance(body_bytes, bytes)
    assert gzip.decompress(body_bytes) == b'{"export": true}'
    assert forwarded_headers.get("Content-Encoding") == "gzip"
    assert forwarded_headers["Content-Type"] == "application/json"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_response_raw_is_file_like_for_streaming_call(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result(body=b"streamed bytes")

    resp = adapter.send(_prepared(), stream=True)

    assert resp.raw.read() == b"streamed bytes"
    resp.raw.decode_content = True
    assert resp.raw.decode_content is True


# ---------------------------------------------------------------------------
# Header filtering
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_restricted_headers_not_forwarded(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    """Transport-managed headers must be stripped before reaching _dotnet_send."""
    mock_send.return_value = _dotnet_result()
    req = _prepared("POST", body=b"body", headers={"X-Custom": "keep"})
    # Inject transport-managed headers directly to simulate what the stack would add.
    req.headers["Content-Length"] = "999"
    req.headers["Host"] = "other.example.com"
    req.headers["Connection"] = "close"
    req.headers["Transfer-Encoding"] = "chunked"
    req.headers["Accept-Encoding"] = "br"
    adapter.send(req)
    _, _, _, forwarded_headers, _, _ = _send_args(mock_send)
    lower_keys = {k.lower() for k in forwarded_headers}
    assert "content-length" not in lower_keys
    assert "host" not in lower_keys
    assert "connection" not in lower_keys
    assert "transfer-encoding" not in lower_keys
    assert "accept-encoding" not in lower_keys
    assert "x-custom" in lower_keys


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_user_agent_forwarded(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared(headers={"User-Agent": "nominal-test/1.0"}))
    _, _, _, forwarded_headers, _, _ = _send_args(mock_send)
    assert forwarded_headers.get("User-Agent") == "nominal-test/1.0"


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_request_headers_forwarded_to_dotnet(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    """Headers already in the prepared request (e.g. injected by HeaderProviderSession) must reach .NET."""
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared(headers={"Authorization": "Bearer token123"}))
    _, _, _, forwarded_headers, _, _ = _send_args(mock_send)
    assert forwarded_headers.get("Authorization") == "Bearer token123"


# ---------------------------------------------------------------------------
# Timeout passthrough
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_scalar_timeout_passed_to_dotnet_send(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared(), timeout=600)
    _, _, _, _, _, timeout_seconds = _send_args(mock_send)
    assert timeout_seconds == 600.0


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_none_timeout_defaults_to_300(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared(), timeout=None)
    _, _, _, _, _, timeout_seconds = _send_args(mock_send)
    assert timeout_seconds == 300.0


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_short_explicit_timeout_respected(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    # Explicit short timeouts must not be silently inflated to 300s.
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared(), timeout=5)
    _, _, _, _, _, timeout_seconds = _send_args(mock_send)
    assert timeout_seconds == 5.0


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_timeout_from_dotnet_propagates(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.side_effect = requests.exceptions.Timeout("timed out")
    with pytest.raises(requests.exceptions.Timeout, match="timed out"):
        adapter.send(_prepared())


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_connection_error_from_dotnet_propagates(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.side_effect = requests.exceptions.ConnectionError("connection refused")
    with pytest.raises(requests.exceptions.ConnectionError, match="connection refused"):
        adapter.send(_prepared())


# ---------------------------------------------------------------------------
# Body compression guards
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac._dotnet_send")
def test_existing_content_encoding_skips_compression(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    raw = b"\x1f\x8b already-compressed"
    req = _prepared("POST", body=raw, headers={"Content-Encoding": "gzip"})
    adapter.send(req)
    _, _, _, forwarded_headers, body_bytes, _ = _send_args(mock_send)
    # Body must be forwarded as-is; Content-Encoding must not be changed.
    assert body_bytes == raw
    assert forwarded_headers.get("Content-Encoding") == "gzip"


# ---------------------------------------------------------------------------
# Retry-After header case-insensitivity
# ---------------------------------------------------------------------------


def test_retry_after_case_insensitive() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = WindowsCacAdapter(max_retries=Retry(total=1, status_forcelist=[429]))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            # Return lowercase "retry-after" to verify case-insensitive lookup.
            mock_send.side_effect = [
                _dotnet_result(status_code=429, headers={"retry-after": "1"}),
                _dotnet_result(status_code=200),
            ]
            response = adapter.send(_prepared("GET", "https://api.example.com/rate"))

    assert response.status_code == 200
    assert mock_send.call_count == 2
    adapter.close()


# ---------------------------------------------------------------------------
# Trust, proxy, redirect, and retry parity with requests adapters
# ---------------------------------------------------------------------------


def test_verify_true_uses_schannel_trust_store() -> None:
    # verify=True must NOT load certifi or install a Python callback — Schannel's
    # Windows trust store already contains DoD root CAs on a standard CAC install.
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared(), verify=True)

    assert build.call_args.kwargs["disable_server_certificate_validation"] is False
    adapter.close()


def test_verify_false_builds_insecure_client() -> None:
    build = MagicMock()
    with patch("nominal.smartcard._windows_cac._build_http_client", build):
        isolated = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            isolated.send(_prepared(), verify=False)

    assert build.call_args.kwargs["disable_server_certificate_validation"] is True
    isolated.close()


def test_verify_path_defers_to_schannel(tmp_path: Path) -> None:
    # On a standard CAC install the Windows trust store already has DoD root CAs.
    # Any truthy verify value (including a CA bundle path injected by
    # requests.Session.merge_environment_settings()) is treated identically to
    # verify=True — we always defer to Schannel and never load Python-level certs.
    ca_bundle = tmp_path / "ca.pem"
    ca_bundle.write_text("-----BEGIN CERTIFICATE-----\nY2VydA==\n-----END CERTIFICATE-----\n")
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared(), verify=str(ca_bundle))

    assert build.call_args.kwargs["disable_server_certificate_validation"] is False
    adapter.close()


def test_verify_nonexistent_path_still_defers_to_schannel(adapter: WindowsCacAdapter) -> None:
    # Even a non-existent path is truthy, so Schannel handles validation — no OSError.
    with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
        response = adapter.send(_prepared(), verify="/does/not/exist.pem")
    assert response.status_code == 200


def test_proxy_mapping_selects_proxy_for_request_url() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared("GET", "https://api.example.com/test"), proxies={"https": "http://proxy:8080"})

    assert build.call_args.kwargs["proxy_url"] == "http://proxy:8080"
    adapter.close()


def test_no_proxy_mapping_uses_direct_client() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())

    assert build.call_args.kwargs["proxy_url"] is None
    adapter.close()


def test_http_client_reused_for_same_trust_and_proxy() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())
            adapter.send(_prepared())

    build.assert_called_once()
    adapter.close()


def test_status_forcelist_retries_then_returns_success() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = WindowsCacAdapter(max_retries=Retry(total=1, status_forcelist=[503]))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            mock_send.side_effect = [
                _dotnet_result(status_code=503, reason="Unavailable"),
                _dotnet_result(status_code=200, reason="OK", body=b"done"),
            ]
            response = adapter.send(_prepared("GET", "https://api.example.com/retry"))

    assert response.status_code == 200
    assert response.content == b"done"
    assert mock_send.call_count == 2
    adapter.close()


def test_connection_error_retries_then_returns_success() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = WindowsCacAdapter(max_retries=Retry(total=1, connect=1))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            mock_send.side_effect = [
                requests.exceptions.ConnectionError("refused"),
                _dotnet_result(status_code=200, reason="OK"),
            ]
            response = adapter.send(_prepared("GET", "https://api.example.com/retry"))

    assert response.status_code == 200
    assert mock_send.call_count == 2
    adapter.close()


def test_connection_error_retry_exhaustion_attaches_request() -> None:
    prepared = _prepared("GET", "https://api.example.com/fail")
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = WindowsCacAdapter(max_retries=Retry(total=0))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            mock_send.side_effect = requests.exceptions.ConnectionError("refused")
            with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
                adapter.send(prepared)

    # The raised exception must carry the original PreparedRequest so callers
    # can inspect it (e.g. for logging or retry-after handling).
    assert exc_info.value.request is prepared
    adapter.close()


def test_timeout_retry_exhaustion_attaches_request() -> None:
    prepared = _prepared("GET", "https://api.example.com/slow")
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = WindowsCacAdapter(max_retries=Retry(total=0))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            mock_send.side_effect = requests.exceptions.Timeout("timed out")
            with pytest.raises(requests.exceptions.Timeout) as exc_info:
                adapter.send(prepared)

    assert exc_info.value.request is prepared
    adapter.close()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# close() disposes the .NET client
# ---------------------------------------------------------------------------


def test_close_disposes_net_client() -> None:
    mock_client = MagicMock()
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=mock_client):
        adapter = WindowsCacAdapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())
    adapter.close()
    mock_client.Dispose.assert_called_once()


# ---------------------------------------------------------------------------
# SmartcardTransportProvider.create_http_adapter platform routing
# ---------------------------------------------------------------------------


def test_create_http_adapter_returns_pkcs11_adapter_on_non_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On non-Windows, create_http_adapter returns a NominalRequestsAdapter backed by the pkcs11 SSL context."""
    pytest.importorskip("cryptography")
    import ssl as _ssl

    from _helpers import _candidate, _FakeBackend, _make_der_cert
    from urllib3.util.retry import Retry

    from nominal.core._utils.networking import NominalRequestsAdapter
    from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
    from nominal.smartcard._session import SmartcardSessionManager
    from nominal.smartcard._transport import SmartcardTransportProvider

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    fake_context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    fake_bridge = MagicMock()
    fake_bridge.build_ssl_context.return_value = fake_context
    provider = SmartcardTransportProvider(_session_manager=manager, _openssl_bridge=fake_bridge)

    with patch("nominal.smartcard._transport.platform") as mock_platform:
        mock_platform.system.return_value = "Linux"
        result = provider.create_http_adapter(max_retries=Retry(0))

    assert isinstance(result, NominalRequestsAdapter)
    assert result._ssl_context is fake_context


def test_create_http_adapter_returns_windows_cac_adapter_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("cryptography")
    from _helpers import _candidate, _FakeBackend, _make_der_cert
    from urllib3.util.retry import Retry

    from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
    from nominal.smartcard._session import SmartcardSessionManager
    from nominal.smartcard._transport import SmartcardTransportProvider

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    provider = SmartcardTransportProvider(_session_manager=manager)

    with (
        patch("nominal.smartcard._transport.platform") as mock_platform,
        patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()),
    ):
        mock_platform.system.return_value = "Windows"
        result = provider.create_http_adapter(max_retries=Retry(0))

    assert isinstance(result, WindowsCacAdapter)
    result.close()
