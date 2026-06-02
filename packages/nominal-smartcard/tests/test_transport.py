from __future__ import annotations

import ssl
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _helpers import _candidate, _FakeBackend, _make_der_cert
from urllib3.util.retry import Retry

pytest.importorskip("cryptography")

from nominal.core._utils.networking import NominalRequestsAdapter, NominalSslRequestsAdapter
from nominal.smartcard._errors import (
    SmartcardConfigurationError,
    SmartcardPinError,
    SmartcardPinLockedError,
    SmartcardProviderError,
)
from nominal.smartcard._grpc_signer import SmartcardPrivateKeySigner
from nominal.smartcard._pkcs11 import NOMINAL_PKCS11_MODULE_ENV_VAR
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import MAX_PIN_ATTEMPTS, SmartcardTransportProvider
from nominal.smartcard._windows_cert_store import WindowsCertificateIdentity

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


def _make_windows_identity() -> WindowsCertificateIdentity:
    return WindowsCertificateIdentity(
        certificate=MagicMock(name="windows_certificate"),
        der_certificate=_make_der_cert(),
        thumbprint="AABBCC",
        subject="CN=Test",
        issuer="CN=Issuer",
        not_after="2099-01-01",
        public_key_oid="1.2.840.113549.1.1.1",
    )


def test_http_adapter_uses_pkcs11_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider, bridge = _make_provider(tmp_path, monkeypatch)

    adapter = provider.create_http_adapter(max_retries=_RETRY)

    assert isinstance(adapter, NominalRequestsAdapter)
    assert adapter._ssl_context is bridge.context
    assert len(bridge.calls) == 1


def test_multipart_adapter_does_not_use_pkcs11_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Object-store multipart traffic must not present a client certificate."""
    provider, bridge = _make_provider(tmp_path, monkeypatch)

    adapter = provider.create_multipart_adapter(max_retries=_RETRY, pool_size=4)

    assert isinstance(adapter, NominalSslRequestsAdapter)
    assert not isinstance(adapter, NominalRequestsAdapter)
    assert adapter._ssl_context is not bridge.context
    assert bridge.calls == []


def test_http_adapter_does_not_retry_keyboard_interrupt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    interrupting_bridge = _InterruptingBridge()
    provider._openssl_bridge = interrupting_bridge

    with pytest.raises(KeyboardInterrupt):
        provider.create_http_adapter(max_retries=_RETRY)

    assert len(interrupting_bridge.calls) == 1


def test_http_adapter_does_not_retry_provider_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider, _bridge = _make_provider(tmp_path, monkeypatch)
    pin_error_bridge = _PinErrorThenSuccessBridge()
    provider._openssl_bridge = pin_error_bridge

    adapter = provider.create_http_adapter(max_retries=_RETRY)

    assert adapter._ssl_context is pin_error_bridge.context
    assert len(pin_error_bridge.calls) == 2


def test_http_adapter_passes_session_to_bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider, bridge = _make_provider(tmp_path, monkeypatch)
    adapter1 = provider.create_http_adapter(max_retries=_RETRY)
    adapter2 = provider.create_http_adapter(max_retries=_RETRY)
    assert adapter1._ssl_context is adapter2._ssl_context
    assert len(bridge.calls) == 1


def test_http_adapter_pin_prompted_once_across_threads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


# ---------------------------------------------------------------------------
# create_grpc_channel_credentials
# ---------------------------------------------------------------------------


def _make_grpc_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    pin: str = "123456",
) -> SmartcardTransportProvider:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    monkeypatch.setattr("nominal.smartcard._grpc_signer._prompt_for_pin", lambda prompt: pin)
    monkeypatch.setattr(SmartcardPrivateKeySigner, "connect", lambda self: None)
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [_candidate(der_certificate=_make_der_cert())]),
    )
    return SmartcardTransportProvider(_session_manager=manager)


def test_grpc_credentials_calls_grpc_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_ssl_fn = MagicMock(return_value=MagicMock())
    root_ca = b"-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----\n"

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials(root_certificates=root_ca)

    assert fake_ssl_fn.call_args.kwargs["root_certificates"] == root_ca


def test_grpc_credentials_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_creds = MagicMock()
    fake_ssl_fn = MagicMock(return_value=fake_creds)

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        creds1 = provider.create_grpc_channel_credentials()
        creds2 = provider.create_grpc_channel_credentials()

    assert creds1 is creds2
    assert fake_ssl_fn.call_count == 1


def test_grpc_credentials_cache_is_keyed_by_trust_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_ssl_fn = MagicMock(side_effect=[MagicMock(name="creds1"), MagicMock(name="creds2")])

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        creds1 = provider.create_grpc_channel_credentials(root_certificates=b"root-a")
        creds2 = provider.create_grpc_channel_credentials(root_certificates=b"root-b")

    assert creds1 is not creds2
    assert fake_ssl_fn.call_count == 2


def test_grpc_credentials_signer_receives_correct_token_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = _candidate(
        der_certificate=_make_der_cert(),
        token_label="MY_TOKEN",
        object_id_bytes=b"\x02",
    )
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))
    monkeypatch.setattr("nominal.smartcard._grpc_signer._prompt_for_pin", lambda prompt: "pin")
    monkeypatch.setattr(SmartcardPrivateKeySigner, "connect", lambda self: None)
    manager = SmartcardSessionManager(
        backend_factory=lambda path: _FakeBackend(path, [candidate]),
    )
    provider = SmartcardTransportProvider(_session_manager=manager)

    captured_signer: list[dict[str, object]] = []

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
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    custom_chain = b"-----BEGIN CERTIFICATE-----\nCUSTOM\n-----END CERTIFICATE-----\n"
    fake_ssl_fn = MagicMock(return_value=MagicMock())

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials(certificate_chain_pem=custom_chain)

    assert fake_ssl_fn.call_args.kwargs["certificate_chain"] == custom_chain


def test_grpc_credentials_raises_on_missing_token_label(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider = SmartcardTransportProvider(_session_manager=manager)

    with pytest.raises(SmartcardConfigurationError, match="token label"):
        provider.create_grpc_channel_credentials()


def test_grpc_credentials_raises_on_missing_object_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    provider = SmartcardTransportProvider(_session_manager=manager)

    with pytest.raises(SmartcardConfigurationError, match="object ID"):
        provider.create_grpc_channel_credentials()


def test_grpc_credentials_close_releases_signer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_grpc_provider(tmp_path, monkeypatch)

    fake_ssl_fn = MagicMock(return_value=MagicMock())

    with patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn):
        provider.create_grpc_channel_credentials()

    assert len(provider._signers) == 1
    assert provider._cached_grpc_credentials

    provider.close()

    assert provider._signers == []
    assert provider._cached_grpc_credentials == {}


def test_windows_grpc_credentials_use_shared_windows_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    identity = _make_windows_identity()
    provider = SmartcardTransportProvider(_windows_identity=identity)

    class FakeWindowsCngSigner:
        instances: list[FakeWindowsCngSigner] = []

        def __init__(self, *, identity: WindowsCertificateIdentity) -> None:
            self.identity = identity
            self.connected = False
            self.closed = False
            FakeWindowsCngSigner.instances.append(self)

        def connect(self) -> None:
            self.connected = True

        @property
        def der_certificate(self) -> bytes:
            return self.identity.der_certificate

        def sign(self, data_to_sign: bytes, signature_algorithm: object, on_complete: object) -> bytes:
            del data_to_sign, signature_algorithm, on_complete
            return b"signature"

        def close(self) -> None:
            self.closed = True

    fake_creds = MagicMock()
    fake_ssl_fn = MagicMock(return_value=fake_creds)

    monkeypatch.setattr("nominal.smartcard._windows_cng_signer.WindowsCngSigner", FakeWindowsCngSigner)
    with (
        patch("nominal.smartcard._transport.platform") as mock_platform,
        patch("nominal.smartcard._transport.ssl_channel_credentials_with_custom_signer", fake_ssl_fn),
    ):
        mock_platform.system.return_value = "Windows"
        http_adapter = provider.create_http_adapter(max_retries=_RETRY)
        grpc_creds = provider.create_grpc_channel_credentials(root_certificates=b"root")

    assert http_adapter._client_certificate is identity.certificate
    assert grpc_creds is fake_creds
    assert FakeWindowsCngSigner.instances[0].identity is identity
    assert FakeWindowsCngSigner.instances[0].connected is True
    assert fake_ssl_fn.call_args.kwargs["certificate_chain"].startswith(b"-----BEGIN CERTIFICATE-----")
    assert fake_ssl_fn.call_args.kwargs["root_certificates"] == b"root"

    provider.close()
    assert FakeWindowsCngSigner.instances[0].closed is True
