from __future__ import annotations

import gzip
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.smartcard._windows_cac import (
    NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR,
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


def test_timeout_scalar_below_300_clamps_to_300() -> None:
    assert _timeout_to_seconds(10) == 300.0


def test_timeout_scalar_above_300_returned() -> None:
    assert _timeout_to_seconds(600) == 600.0


def test_timeout_tuple_uses_max() -> None:
    assert _timeout_to_seconds((60, 400)) == 400.0


def test_timeout_tuple_with_none_ignores_none() -> None:
    assert _timeout_to_seconds((None, 500)) == 500.0


def test_timeout_tuple_all_small_clamps_to_300() -> None:
    assert _timeout_to_seconds((5, 10)) == 300.0


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
def test_get_with_no_body_sends_none_body_bytes(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    mock_send.return_value = _dotnet_result()
    adapter.send(_prepared())
    _, _, _, _, body_bytes, _ = _send_args(mock_send)
    assert body_bytes is None


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
# Certificate thumbprint env var
# ---------------------------------------------------------------------------


def test_cert_thumbprint_env_var_passed_to_build_http_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR, "AABBCCDD")
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as mock_build:
        WindowsCacAdapter()
    mock_build.assert_called_once_with(cert_thumbprint="AABBCCDD")


def test_no_thumbprint_env_var_passes_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR, raising=False)
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as mock_build:
        WindowsCacAdapter()
    mock_build.assert_called_once_with(cert_thumbprint=None)


# ---------------------------------------------------------------------------
# close() disposes the .NET client
# ---------------------------------------------------------------------------


def test_close_disposes_net_client() -> None:
    mock_client = MagicMock()
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=mock_client):
        adapter = WindowsCacAdapter()
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
