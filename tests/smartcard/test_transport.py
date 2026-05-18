from __future__ import annotations

import ssl
from pathlib import Path

import pytest

from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import SmartcardSslContextProvider
from tests.smartcard._helpers import _candidate, _FakeBackend, _make_der_cert


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[SmartcardSession, str]] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession, pin: str) -> ssl.SSLContext:
        self.calls.append((session, pin))
        return self.context


def _make_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, pin: str = "123456"
) -> tuple[SmartcardSslContextProvider, _FakeBridge]:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        pin_provider=lambda prompt: pin,
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    return provider, bridge


def test_ssl_context_provider_builds_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    ctx = provider.create_ssl_context()
    assert ctx is bridge.context
    assert len(bridge.calls) == 1


def test_ssl_context_provider_passes_pin_to_bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch, pin="secret")
    provider.create_ssl_context()
    assert bridge.calls[0][1] == "secret"


def test_ssl_context_provider_passes_session_to_bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    certificate = _candidate(der_certificate=_make_der_cert())
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [certificate]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        pin_provider=lambda prompt: "pin",
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    provider.create_ssl_context()
    assert bridge.calls[0][0].certificate is certificate


# SmartcardSslContextProvider property factory


def test_ssl_context_provider_session_manager_defaults_to_shared(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SmartcardSessionManager, "_shared_manager", None)
    provider = SmartcardSslContextProvider()
    assert provider.session_manager is SmartcardSessionManager.shared()
