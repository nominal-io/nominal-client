from __future__ import annotations

import ssl
import threading
from pathlib import Path

import pytest
from _helpers import _candidate, _FakeBackend, _make_der_cert
from urllib3.util.retry import Retry

from nominal.core._utils.networking import NominalRequestsAdapter, NominalSslRequestsAdapter
from nominal.smartcard._errors import SmartcardPinError, SmartcardPinLockedError, SmartcardProviderError
from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import MAX_PIN_ATTEMPTS, SmartcardTransportProvider

_RETRY = Retry(total=0)


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


class _PinLockedBridge(_FakeBridge):
    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        raise SmartcardPinLockedError("CKR_PIN_LOCKED")


class _AlwaysPinErrorBridge(_FakeBridge):
    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        raise SmartcardPinError("CKR_PIN_INCORRECT")


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


def test_http_adapter_uses_pkcs11_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)

    adapter = provider.create_http_adapter(max_retries=_RETRY)

    assert isinstance(adapter, NominalRequestsAdapter)
    assert adapter._ssl_context is bridge.context
    assert len(bridge.calls) == 1


def test_multipart_adapter_does_not_use_pkcs11_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Object-store multipart traffic must not present a client certificate."""
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)

    adapter = provider.create_multipart_adapter(max_retries=_RETRY, pool_size=4)

    assert isinstance(adapter, NominalSslRequestsAdapter)
    assert not isinstance(adapter, NominalRequestsAdapter)
    assert adapter._ssl_context is not bridge.context
    assert bridge.calls == []


def test_http_adapter_does_not_retry_keyboard_interrupt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    interrupting_bridge = _InterruptingBridge()
    provider._openssl_bridge = interrupting_bridge

    with pytest.raises(KeyboardInterrupt):
        provider.create_http_adapter(max_retries=_RETRY)

    assert len(interrupting_bridge.calls) == 1


def test_http_adapter_does_not_retry_provider_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    provider_error_bridge = _ProviderErrorBridge()
    provider._openssl_bridge = provider_error_bridge

    with pytest.raises(SystemExit, match="PIN entry may have been cancelled"):
        provider.create_http_adapter(max_retries=_RETRY)

    assert len(provider_error_bridge.calls) == 1


def test_http_adapter_exits_on_pin_locked_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    pin_locked_bridge = _PinLockedBridge()
    provider._openssl_bridge = pin_locked_bridge

    with pytest.raises(SystemExit, match="Card PIN is locked"):
        provider.create_http_adapter(max_retries=_RETRY)

    assert len(pin_locked_bridge.calls) == 1


def test_http_adapter_exits_after_all_pin_attempts_exhausted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    always_error_bridge = _AlwaysPinErrorBridge()
    provider._openssl_bridge = always_error_bridge

    with pytest.raises(SystemExit, match="No attempts remaining"):
        provider.create_http_adapter(max_retries=_RETRY)

    assert len(always_error_bridge.calls) == MAX_PIN_ATTEMPTS


def test_http_adapter_retries_pin_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    pin_error_bridge = _PinErrorThenSuccessBridge()
    provider._openssl_bridge = pin_error_bridge

    adapter = provider.create_http_adapter(max_retries=_RETRY)

    assert adapter._ssl_context is pin_error_bridge.context
    assert len(pin_error_bridge.calls) == 2


def test_http_adapter_passes_session_to_bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider.create_http_adapter(max_retries=_RETRY)
    assert bridge.calls[0].certificate is certificate


def test_http_adapter_caches_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    adapter1 = provider.create_http_adapter(max_retries=_RETRY)
    adapter2 = provider.create_http_adapter(max_retries=_RETRY)
    assert adapter1._ssl_context is adapter2._ssl_context
    assert len(bridge.calls) == 1


def test_http_adapter_pin_prompted_once_across_threads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    barrier = threading.Barrier(10)
    results: list[ssl.SSLContext] = []
    lock = threading.Lock()

    def call() -> None:
        barrier.wait()
        adapter = provider.create_http_adapter(max_retries=_RETRY)
        with lock:
            results.append(adapter._ssl_context)

    threads = [threading.Thread(target=call) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(bridge.calls) == 1
    assert all(ctx is bridge.context for ctx in results)


# SmartcardTransportProvider property factory


def test_provider_session_manager_defaults_to_shared(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SmartcardSessionManager, "_shared_manager", None)
    provider = SmartcardTransportProvider()
    assert provider.session_manager is SmartcardSessionManager.shared()
