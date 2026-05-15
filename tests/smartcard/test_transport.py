from __future__ import annotations

import ssl
from pathlib import Path

from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import SmartcardSslContextProvider
from tests.smartcard._helpers import _candidate, _FakeBackend


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[SmartcardSession, str]] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession, pin: str) -> ssl.SSLContext:
        self.calls.append((session, pin))
        return self.context


def _make_provider(tmp_path: Path, *, pin: str = "123456") -> tuple[SmartcardSslContextProvider, _FakeBridge]:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        backend_factory=lambda path: _FakeBackend(path, [_candidate()]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        config=SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: pin,
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    return provider, bridge


def test_ssl_context_provider_builds_ssl_context(tmp_path: Path) -> None:
    provider, bridge = _make_provider(tmp_path)
    ctx = provider.create_ssl_context()
    assert ctx is bridge.context
    assert len(bridge.calls) == 1


def test_ssl_context_provider_passes_pin_to_bridge(tmp_path: Path) -> None:
    provider, bridge = _make_provider(tmp_path, pin="secret")
    provider.create_ssl_context()
    assert bridge.calls[0][1] == "secret"


def test_ssl_context_provider_passes_session_to_bridge(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        backend_factory=lambda path: _FakeBackend(path, [certificate]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        config=SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "pin",
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    provider.create_ssl_context()
    assert bridge.calls[0][0].certificate is certificate
