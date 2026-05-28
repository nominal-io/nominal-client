from __future__ import annotations

import ssl
import threading
from pathlib import Path

import pytest
from _helpers import _candidate, _FakeBackend, _make_der_cert

from nominal.smartcard._errors import SmartcardPinError, SmartcardProviderError
from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import SmartcardTransportProvider


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[SmartcardSession] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        return self.context


class _InterruptingBridge(_FakeBridge):
    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        raise KeyboardInterrupt


class _ProviderErrorBridge(_FakeBridge):
    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        raise SmartcardProviderError("OSSL_STORE_load error: unknown error")


class _PinErrorThenSuccessBridge(_FakeBridge):
    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        if len(self.calls) == 1:
            raise SmartcardPinError("CKR_PIN_INCORRECT")
        return self.context


def _make_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[SmartcardTransportProvider, _FakeBridge]:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    bridge = _FakeBridge()
    provider = SmartcardTransportProvider(
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


def test_ssl_context_provider_does_not_retry_keyboard_interrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    interrupting_bridge = _InterruptingBridge()
    provider._openssl_bridge = interrupting_bridge

    with pytest.raises(KeyboardInterrupt):
        provider.create_ssl_context()

    assert len(interrupting_bridge.calls) == 1


def test_ssl_context_provider_does_not_retry_provider_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    provider_error_bridge = _ProviderErrorBridge()
    provider._openssl_bridge = provider_error_bridge

    with pytest.raises(SystemExit, match="PIN entry may have been cancelled"):
        provider.create_ssl_context()

    assert len(provider_error_bridge.calls) == 1


def test_ssl_context_provider_retries_pin_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    pin_error_bridge = _PinErrorThenSuccessBridge()
    provider._openssl_bridge = pin_error_bridge

    ctx = provider.create_ssl_context()

    assert ctx is pin_error_bridge.context
    assert len(pin_error_bridge.calls) == 2


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
    provider = SmartcardTransportProvider(
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    provider.create_ssl_context()
    assert bridge.calls[0].certificate is certificate


def test_ssl_context_provider_caches_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    ctx1 = provider.create_ssl_context()
    ctx2 = provider.create_ssl_context()
    assert ctx1 is ctx2
    assert len(bridge.calls) == 1


def test_ssl_context_provider_pin_prompted_once_across_threads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    barrier = threading.Barrier(10)
    results: list[ssl.SSLContext] = []
    lock = threading.Lock()

    def call() -> None:
        barrier.wait()
        ctx = provider.create_ssl_context()
        with lock:
            results.append(ctx)

    threads = [threading.Thread(target=call) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(bridge.calls) == 1
    assert all(ctx is bridge.context for ctx in results)


# SmartcardTransportProvider property factory


def test_ssl_context_provider_session_manager_defaults_to_shared(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SmartcardSessionManager, "_shared_manager", None)
    provider = SmartcardTransportProvider()
    assert provider.session_manager is SmartcardSessionManager.shared()
