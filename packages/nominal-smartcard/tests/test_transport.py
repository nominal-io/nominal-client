from __future__ import annotations

import ssl
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _helpers import _candidate, _FakeBackend, _make_der_cert

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import SmartcardSslContextProvider


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[SmartcardSession] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        return self.context


def _make_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[SmartcardSslContextProvider, _FakeBridge]:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
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


# SmartcardSslContextProvider property factory


def test_ssl_context_provider_session_manager_defaults_to_shared(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SmartcardSessionManager, "_shared_manager", None)
    provider = SmartcardSslContextProvider()
    assert provider.session_manager is SmartcardSessionManager.shared()


# ---------------------------------------------------------------------------
# create_grpc_channel_credentials
# ---------------------------------------------------------------------------


def _make_grpc_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    pin: str = "123456",
) -> SmartcardSslContextProvider:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    monkeypatch.setattr("nominal.smartcard._grpc_signer._prompt_for_pin", lambda prompt: pin)
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    return SmartcardSslContextProvider(_session_manager=manager)


def test_grpc_credentials_calls_grpc_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_creds = MagicMock()
    fake_ssl_fn = MagicMock(return_value=fake_creds)

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        creds = provider.create_grpc_channel_credentials()

    assert creds is fake_creds
    fake_ssl_fn.assert_called_once()
    call_kwargs = fake_ssl_fn.call_args.kwargs
    assert "private_key_sign_fn" in call_kwargs
    assert "certificate_chain" in call_kwargs
    # certificate_chain must be PEM bytes (starts with -----BEGIN CERTIFICATE-----)
    assert call_kwargs["certificate_chain"].startswith(b"-----BEGIN CERTIFICATE-----")
    # root_certificates defaults to None
    assert call_kwargs["root_certificates"] is None


def test_grpc_credentials_passes_root_certificates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_ssl_fn = MagicMock(return_value=MagicMock())
    root_ca = b"-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----\n"

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials(root_certificates=root_ca)

    assert fake_ssl_fn.call_args.kwargs["root_certificates"] == root_ca


def test_grpc_credentials_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_creds = MagicMock()
    fake_ssl_fn = MagicMock(return_value=fake_creds)

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        creds1 = provider.create_grpc_channel_credentials()
        creds2 = provider.create_grpc_channel_credentials()

    assert creds1 is creds2
    assert fake_ssl_fn.call_count == 1


def test_grpc_credentials_signer_receives_correct_token_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(
        der_certificate=_make_der_cert(),
        token_label="MY_TOKEN",
        object_id_bytes=b"\x02",
    )
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    monkeypatch.setattr("nominal.smartcard._grpc_signer._prompt_for_pin", lambda prompt: "pin")
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [candidate]),
    )
    provider = SmartcardSslContextProvider(_session_manager=manager)

    captured_signer: list[dict[str, object]] = []

    from nominal.smartcard._grpc_signer import SmartcardPrivateKeySigner

    original_signer_init = SmartcardPrivateKeySigner.__init__

    def capturing_init(self: SmartcardPrivateKeySigner, **kwargs: object) -> None:
        captured_signer.append(kwargs)
        original_signer_init(self, **kwargs)

    monkeypatch.setattr(SmartcardPrivateKeySigner, "__init__", capturing_init)

    fake_ssl_fn = MagicMock(return_value=MagicMock())
    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials()

    assert len(captured_signer) == 1
    assert captured_signer[0]["token_label"] == "MY_TOKEN"
    assert captured_signer[0]["object_id_bytes"] == b"\x02"


def test_grpc_credentials_uses_custom_certificate_chain_pem(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    custom_chain = b"-----BEGIN CERTIFICATE-----\nCUSTOM\n-----END CERTIFICATE-----\n"
    fake_ssl_fn = MagicMock(return_value=MagicMock())

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials(certificate_chain_pem=custom_chain)

    assert fake_ssl_fn.call_args.kwargs["certificate_chain"] == custom_chain


def test_grpc_credentials_raises_on_missing_token_label(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(
        der_certificate=_make_der_cert(),
        token_label="",
        object_id_bytes=b"\x01",
    )
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [candidate]),
    )
    provider = SmartcardSslContextProvider(_session_manager=manager)

    with pytest.raises(SmartcardConfigurationError, match="token label"):
        provider.create_grpc_channel_credentials()


def test_grpc_credentials_raises_on_missing_object_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(
        der_certificate=_make_der_cert(),
        token_label="MY_TOKEN",
        object_id_bytes=None,
    )
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [candidate]),
    )
    provider = SmartcardSslContextProvider(_session_manager=manager)

    with pytest.raises(SmartcardConfigurationError, match="object ID"):
        provider.create_grpc_channel_credentials()


def test_grpc_credentials_close_releases_signer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cryptography")
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_ssl_fn = MagicMock(return_value=MagicMock())

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials()

    assert provider._signer is not None
    assert provider._cached_grpc_credentials is not None

    provider.close()

    assert provider._signer is None
    assert provider._cached_grpc_credentials is None
