from __future__ import annotations

import gzip
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.util.retry import Retry

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._windows_cac import (
    WindowsCacAdapter,
    _build_web_proxy,
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
def adapter() -> Iterator[WindowsCacAdapter]:
    """A WindowsCacAdapter with the .NET client creation mocked out."""
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        yield _make_adapter()


def _make_adapter(*args: Any, **kwargs: Any) -> WindowsCacAdapter:
    kwargs.setdefault("client_certificate", MagicMock(name="client_certificate"))
    return WindowsCacAdapter(*args, **kwargs)


def _send_args(mock_dotnet_send: MagicMock) -> tuple[Any, str, str, dict, bytes | None, float]:
    """Extract (client, method, url, headers, body_bytes, timeout_seconds) from the last call."""
    return mock_dotnet_send.call_args.args


def test_smartcard_package_import_does_not_require_pkcs11_or_cffi() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "pkcs11" or fullname.startswith("pkcs11.") or fullname == "cffi":
                    raise ModuleNotFoundError(f"blocked optional dependency: {fullname}")
                return None

        for module_name in list(sys.modules):
            if module_name == "pkcs11" or module_name.startswith("pkcs11.") or module_name == "cffi":
                del sys.modules[module_name]
        sys.meta_path.insert(0, Blocker())

        import nominal.smartcard

        nominal.smartcard.SmartcardTransportProvider.create()
        """
    )
    subprocess.run([sys.executable, "-c", code], check=True)


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
def test_stream_flag_suppresses_request_compression(mock_send: MagicMock, adapter: WindowsCacAdapter) -> None:
    # Match NominalRequestsAdapter: stream=True leaves request bodies untouched.
    mock_send.return_value = _dotnet_result()
    req = _prepared("POST", body=b'{"export": true}', headers={"Content-Type": "application/json"})

    adapter.send(req, stream=True)

    _, _, _, forwarded_headers, body_bytes, _ = _send_args(mock_send)
    assert body_bytes == b'{"export": true}'
    assert "Content-Encoding" not in forwarded_headers
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
        adapter = _make_adapter(max_retries=Retry(total=1, status_forcelist=[429]))
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


def test_retry_after_duration_is_honored_for_lowercase_header() -> None:
    # urllib3 looks up "Retry-After" case-sensitively, so a lowercase (HTTP/2) header must be
    # surfaced through a case-insensitive mapping or the delay is silently dropped to backoff.
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = _make_adapter(max_retries=Retry(total=1, status_forcelist=[503], backoff_factor=0))
        with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
            mock_send.side_effect = [
                _dotnet_result(status_code=503, headers={"retry-after": "7"}),
                _dotnet_result(status_code=200),
            ]
            with patch("urllib3.util.retry.time.sleep") as mock_sleep:
                response = adapter.send(_prepared("GET", "https://api.example.com/rate"))

    assert response.status_code == 200
    # With backoff_factor=0, any non-zero sleep can only have come from honoring Retry-After: 7.
    assert mock_sleep.call_args.args[0] == 7.0
    adapter.close()


# ---------------------------------------------------------------------------
# Trust, proxy, redirect, and retry parity with requests adapters
# ---------------------------------------------------------------------------


def test_verify_true_uses_schannel_trust_store() -> None:
    # verify=True must NOT load certifi or install a Python callback — Schannel's
    # Windows trust store already contains DoD root CAs on a standard CAC install.
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared(), verify=True)

    assert "disable_server_certificate_validation" not in build.call_args.kwargs
    adapter.close()


def test_verify_false_does_not_disable_schannel_validation() -> None:
    # verify=False is intentionally ignored: there is no insecure-client path. Schannel
    # always validates against the Windows trust store. This locks in that security property
    # so a future change can't silently reintroduce a validation bypass.
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        isolated = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            isolated.send(_prepared(), verify=False)

    assert "disable_server_certificate_validation" not in build.call_args.kwargs
    isolated.close()


def test_default_certifi_verify_path_defers_to_schannel() -> None:
    import certifi

    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared(), verify=certifi.where())

    assert "disable_server_certificate_validation" not in build.call_args.kwargs
    adapter.close()


def test_custom_verify_path_raises_clear_error(tmp_path: Path, adapter: WindowsCacAdapter) -> None:
    ca_bundle = tmp_path / "ca.pem"
    ca_bundle.write_text("-----BEGIN CERTIFICATE-----\nY2VydA==\n-----END CERTIFICATE-----\n")
    with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
        with pytest.raises(SmartcardConfigurationError, match="cannot honor a custom Python CA bundle"):
            adapter.send(_prepared(), verify=str(ca_bundle))


def test_verify_nonexistent_path_raises_clear_error(adapter: WindowsCacAdapter) -> None:
    with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
        with pytest.raises(SmartcardConfigurationError, match="cannot honor a custom Python CA bundle"):
            adapter.send(_prepared(), verify="/does/not/exist.pem")


def test_proxy_mapping_selects_proxy_for_request_url() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared("GET", "https://api.example.com/test"), proxies={"https": "http://proxy:8080"})

    assert build.call_args.kwargs["proxy_url"] == "http://proxy:8080"
    adapter.close()


def test_no_proxy_mapping_uses_direct_client() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())

    assert build.call_args.kwargs["proxy_url"] is None
    adapter.close()


def test_http_client_reused_for_same_trust_and_proxy() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())
            adapter.send(_prepared())

    build.assert_called_once()
    adapter.close()


def test_http_client_build_receives_client_certificate() -> None:
    client_certificate = MagicMock(name="selected_certificate")
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()) as build:
        adapter = WindowsCacAdapter(client_certificate=client_certificate)
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())

    assert build.call_args.kwargs["client_certificate"] is client_certificate
    adapter.close()


def test_status_forcelist_retries_then_returns_success() -> None:
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = _make_adapter(max_retries=Retry(total=1, status_forcelist=[503]))
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
        adapter = _make_adapter(max_retries=Retry(total=1, connect=1))
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
        adapter = _make_adapter(max_retries=Retry(total=0))
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
        adapter = _make_adapter(max_retries=Retry(total=0))
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
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())
    adapter.close()
    mock_client.Dispose.assert_called_once()


def test_send_after_close_raises() -> None:
    # A closed adapter must not silently rebuild a client (which would re-prompt for PIN).
    with patch("nominal.smartcard._windows_cac._build_http_client", return_value=MagicMock()):
        adapter = _make_adapter()
        with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
            adapter.send(_prepared())
    adapter.close()
    with pytest.raises(RuntimeError, match="closed"):
        adapter.send(_prepared())


# ---------------------------------------------------------------------------
# TLS warm-up: only mark warmed on success (one PIN prompt, no re-prompt storm)
# ---------------------------------------------------------------------------


def test_warmup_marks_warmed_only_on_success(adapter: WindowsCacAdapter) -> None:
    assert adapter._tls_warmed is False
    with patch("nominal.smartcard._windows_cac._dotnet_send", return_value=_dotnet_result()):
        adapter.send(_prepared())
    assert adapter._tls_warmed is True


def test_warmup_failure_does_not_mark_warmed(adapter: WindowsCacAdapter) -> None:
    # If the first request fails (e.g. the user dismisses the PIN dialog), the gate must stay
    # shut so the next request re-serializes instead of letting a burst each re-prompt.
    with patch("nominal.smartcard._windows_cac._dotnet_send") as mock_send:
        mock_send.side_effect = [
            requests.exceptions.ConnectionError("first handshake failed"),
            _dotnet_result(status_code=200),
        ]
        with pytest.raises(requests.exceptions.ConnectionError):
            adapter.send(_prepared())
        assert adapter._tls_warmed is False

        # The next request must still go through the serialized warm-up and, on success, open the gate.
        response = adapter.send(_prepared())
    assert response.status_code == 200
    assert adapter._tls_warmed is True


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
    from nominal.smartcard._transport import _Pkcs11SmartcardTransportProvider

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    fake_context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    fake_bridge = MagicMock()
    fake_bridge.build_ssl_context.return_value = fake_context
    provider = _Pkcs11SmartcardTransportProvider(_session_manager=manager, _openssl_bridge=fake_bridge)

    result = provider.create_http_adapter(max_retries=Retry(0))

    assert isinstance(result, NominalRequestsAdapter)
    assert result._ssl_context is fake_context


def test_create_http_adapter_returns_windows_cac_adapter_on_windows() -> None:
    pytest.importorskip("cryptography")
    from urllib3.util.retry import Retry

    from nominal.smartcard._transport import _WindowsSmartcardTransportProvider
    from nominal.smartcard._windows_cert_store import WindowsCertificateIdentity
    from nominal.smartcard._windows_cng_signer import _OID_RSA

    selected_certificate = MagicMock(name="selected_windows_certificate")
    identity = WindowsCertificateIdentity(
        certificate=selected_certificate,
        der_certificate=b"unused",
        thumbprint="AABBCC",
        subject="CN=Test",
        issuer="CN=Issuer",
        not_after="2099-01-01",
        public_key_oid=_OID_RSA,
    )
    provider = _WindowsSmartcardTransportProvider(_windows_identity=identity)

    result = provider.create_http_adapter(max_retries=Retry(0))

    assert isinstance(result, WindowsCacAdapter)
    assert result._client_certificate is selected_certificate
    result.close()


# ---------------------------------------------------------------------------
# _build_web_proxy — embedded proxy credentials are forwarded to the .NET WebProxy
# ---------------------------------------------------------------------------


class _FakeWebProxy:
    def __init__(self, address: str) -> None:
        self.address = address
        self.Credentials: Any = None


class _FakeNetworkCredential:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password


def test_build_web_proxy_without_credentials_passes_address_through() -> None:
    proxy = _build_web_proxy("http://proxy.corp:8080", WebProxy=_FakeWebProxy, NetworkCredential=_FakeNetworkCredential)
    assert proxy.address == "http://proxy.corp:8080"
    assert proxy.Credentials is None


def test_build_web_proxy_forwards_embedded_credentials() -> None:
    # The stock requests adapter would emit Proxy-Authorization for this URL; the Windows adapter
    # must instead attach the (percent-decoded) credentials to the WebProxy.
    proxy = _build_web_proxy(
        "http://user:p%40ss@proxy.corp:8080", WebProxy=_FakeWebProxy, NetworkCredential=_FakeNetworkCredential
    )
    assert proxy.address == "http://proxy.corp:8080"  # userinfo stripped from the address
    assert isinstance(proxy.Credentials, _FakeNetworkCredential)
    assert proxy.Credentials.username == "user"
    assert proxy.Credentials.password == "p@ss"


def test_build_web_proxy_preserves_ipv6_host() -> None:
    proxy = _build_web_proxy(
        "http://user:pass@[::1]:3128", WebProxy=_FakeWebProxy, NetworkCredential=_FakeNetworkCredential
    )
    assert proxy.address == "http://[::1]:3128"
    assert proxy.Credentials.username == "user"
