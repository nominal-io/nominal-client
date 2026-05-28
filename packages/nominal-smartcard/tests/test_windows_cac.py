from __future__ import annotations

import base64
import gzip
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.smartcard._windows_cac import (
    NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR,
    NOMINAL_WINDOWS_REQUIRE_PRIVATE_KEY_PROOF_ENV_VAR,
    NOMINAL_WINDOWS_TEST_PIN_ENV_VAR,
    NOMINAL_WINDOWS_VERBOSE_CAC_LOG_ENV_VAR,
    WindowsCacSession,
    _timeout_to_seconds,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ps_payload(
    *,
    status_code: int = 200,
    reason: str = "OK",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
    url: str = "https://api.example.com/",
    cac_events: list[str] | None = None,
) -> MagicMock:
    """Build a mock subprocess.CompletedProcess that returns a valid PowerShell payload."""
    payload = {
        "status_code": status_code,
        "reason": reason,
        "headers": headers or {},
        "body_b64": base64.b64encode(body).decode("ascii"),
        "url": url,
        "cac_events": cac_events or [],
    }
    result = MagicMock()
    result.returncode = 0
    result.stdout = json.dumps(payload)
    result.stderr = ""
    return result


def _envelope_from_mock(mock_run: MagicMock) -> dict[str, Any]:
    """Extract the JSON envelope that was passed to subprocess.run via stdin."""
    return json.loads(mock_run.call_args.kwargs["input"])


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
# WindowsCacSession.send — basic request/response
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_successful_get_returns_parsed_response(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload(status_code=200, body=b'{"ok": true}', url="https://api.example.com/test")
    session = WindowsCacSession()
    resp = session.get("https://api.example.com/test")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_response_headers_are_parsed(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload(headers={"Content-Type": "application/json", "X-Custom": "value"})
    session = WindowsCacSession()
    resp = session.get("https://api.example.com/")
    assert resp.headers["Content-Type"] == "application/json"
    assert resp.headers["X-Custom"] == "value"


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_response_url_forwarded(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload(url="https://api.example.com/redirected")
    session = WindowsCacSession()
    resp = session.get("https://api.example.com/")
    assert resp.url == "https://api.example.com/redirected"


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_non_200_status_code_preserved(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload(status_code=404, reason="Not Found")
    session = WindowsCacSession()
    resp = session.get("https://api.example.com/missing")
    assert resp.status_code == 404
    assert resp.reason == "Not Found"


# ---------------------------------------------------------------------------
# Request body compression
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_post_body_is_gzip_compressed(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.post("https://api.example.com/", data=b"hello world" * 100)
    envelope = _envelope_from_mock(mock_run)
    raw = gzip.decompress(base64.b64decode(envelope["body_b64"]))
    assert raw == b"hello world" * 100


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_post_string_body_is_utf8_then_compressed(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.post("https://api.example.com/", data="héllo")
    envelope = _envelope_from_mock(mock_run)
    raw = gzip.decompress(base64.b64decode(envelope["body_b64"]))
    assert raw == "héllo".encode("utf-8")


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_compression_headers_set_on_post(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.post("https://api.example.com/", data=b"payload")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["headers"].get("Content-Encoding") == "gzip"


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_get_with_no_body_sends_empty_body_b64(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["body_b64"] == ""


# ---------------------------------------------------------------------------
# Header filtering
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_restricted_headers_not_forwarded(mock_run: MagicMock) -> None:
    """Headers managed by the transport layer must not be duplicated in the PowerShell envelope."""
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.post(
        "https://api.example.com/",
        headers={
            "Content-Length": "999",
            "Host": "other.example.com",
            "Connection": "close",
            "Transfer-Encoding": "chunked",
            "Accept-Encoding": "br",
            "X-Custom": "keep",
        },
        data=b"body",
    )
    envelope = _envelope_from_mock(mock_run)
    forwarded = {k.lower() for k in envelope["headers"]}
    assert "content-length" not in forwarded
    assert "host" not in forwarded
    assert "connection" not in forwarded
    assert "transfer-encoding" not in forwarded
    assert "accept-encoding" not in forwarded
    assert "x-custom" in forwarded


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_user_agent_forwarded(mock_run: MagicMock) -> None:
    mock_run.return_value = _ps_payload()
    session = WindowsCacSession()
    session.headers["User-Agent"] = "nominal-test/1.0"
    session.get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["headers"].get("User-Agent") == "nominal-test/1.0"


# ---------------------------------------------------------------------------
# HeaderProvider integration
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_header_provider_headers_included(mock_run: MagicMock) -> None:
    from nominal.core._utils.networking import StaticHeaderProvider

    mock_run.return_value = _ps_payload()
    provider = StaticHeaderProvider({"Authorization": "Bearer token123"})
    session = WindowsCacSession(provider)
    session.get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["headers"].get("Authorization") == "Bearer token123"


# ---------------------------------------------------------------------------
# Environment variable passthrough
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_cert_thumbprint_env_var_forwarded(mock_run: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR, "AABBCCDD")
    mock_run.return_value = _ps_payload()
    WindowsCacSession().get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["cert_thumbprint"] == "AABBCCDD"


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_require_private_key_proof_env_var_forwarded(mock_run: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_WINDOWS_REQUIRE_PRIVATE_KEY_PROOF_ENV_VAR, "1")
    mock_run.return_value = _ps_payload()
    WindowsCacSession().get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["require_private_key_proof"] is True


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_test_pin_env_var_forwarded(mock_run: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_WINDOWS_TEST_PIN_ENV_VAR, "123456")
    mock_run.return_value = _ps_payload()
    WindowsCacSession().get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["test_pin"] == "123456"


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_verbose_flag_not_set_by_default(mock_run: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(NOMINAL_WINDOWS_VERBOSE_CAC_LOG_ENV_VAR, raising=False)
    mock_run.return_value = _ps_payload()
    WindowsCacSession().get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["verbose_cac_log"] is False


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_verbose_flag_set_when_env_var_is_1(mock_run: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_WINDOWS_VERBOSE_CAC_LOG_ENV_VAR, "1")
    mock_run.return_value = _ps_payload(cac_events=["bridge started"])
    WindowsCacSession().get("https://api.example.com/")
    envelope = _envelope_from_mock(mock_run)
    assert envelope["verbose_cac_log"] is True


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_nonzero_exit_code_raises_ssl_error(mock_run: MagicMock) -> None:
    result = MagicMock()
    result.returncode = 1
    result.stdout = ""
    result.stderr = "certificate selection failed"
    mock_run.return_value = result
    with pytest.raises(requests.exceptions.SSLError, match="certificate selection failed"):
        WindowsCacSession().get("https://api.example.com/")


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_nonzero_exit_code_uses_stdout_when_stderr_empty(mock_run: MagicMock) -> None:
    result = MagicMock()
    result.returncode = 1
    result.stdout = "stdout error detail"
    result.stderr = ""
    mock_run.return_value = result
    with pytest.raises(requests.exceptions.SSLError, match="stdout error detail"):
        WindowsCacSession().get("https://api.example.com/")


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_invalid_json_response_raises_ssl_error(mock_run: MagicMock) -> None:
    result = MagicMock()
    result.returncode = 0
    result.stdout = "not json {"
    result.stderr = ""
    mock_run.return_value = result
    with pytest.raises(requests.exceptions.SSLError, match="invalid JSON"):
        WindowsCacSession().get("https://api.example.com/")


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_subprocess_timeout_raises_requests_timeout(mock_run: MagicMock) -> None:
    mock_run.side_effect = subprocess.TimeoutExpired(cmd=["powershell.exe"], timeout=360)
    with pytest.raises(requests.exceptions.Timeout, match="timed out"):
        WindowsCacSession().get("https://api.example.com/")


@patch("nominal.smartcard._windows_cac.subprocess.run")
def test_os_error_raises_ssl_error(mock_run: MagicMock) -> None:
    mock_run.side_effect = OSError("No such file: powershell.exe")
    with pytest.raises(requests.exceptions.SSLError, match="powershell.exe"):
        WindowsCacSession().get("https://api.example.com/")


# ---------------------------------------------------------------------------
# SmartcardTransportProvider.create_requests_session platform routing
# ---------------------------------------------------------------------------


def test_create_requests_session_returns_none_on_non_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    from _helpers import _candidate, _FakeBackend, _make_der_cert

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

    with patch("nominal.smartcard._transport.platform") as mock_platform:
        mock_platform.system.return_value = "Linux"
        result = provider.create_requests_session()

    assert result is None


def test_create_requests_session_returns_windows_cac_session_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("cryptography")
    from _helpers import _candidate, _FakeBackend, _make_der_cert

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

    with patch("nominal.smartcard._transport.platform") as mock_platform:
        mock_platform.system.return_value = "Windows"
        result = provider.create_requests_session()

    assert isinstance(result, WindowsCacSession)


def test_create_requests_session_forwards_header_provider_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("cryptography")
    from _helpers import _candidate, _FakeBackend, _make_der_cert

    from nominal.core._utils.networking import StaticHeaderProvider
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
    hp = StaticHeaderProvider({"X-Test": "value"})

    with patch("nominal.smartcard._transport.platform") as mock_platform:
        mock_platform.system.return_value = "Windows"
        session = provider.create_requests_session(header_provider=hp)

    assert isinstance(session, WindowsCacSession)
    assert session._header_provider is hp
