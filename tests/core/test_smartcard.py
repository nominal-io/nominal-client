from __future__ import annotations

import ssl
from pathlib import Path

import pytest

from nominal.smartcard import (
    NOMINAL_PKCS11_MODULE_ENV_VAR,
    CertificateCandidate,
    SmartcardCertificateSelectionError,
    SmartcardConfig,
    SmartcardSession,
    SmartcardSessionManager,
    SmartcardSslContextProvider,
    discover_pkcs11_module,
    select_piv_authentication_certificate,
)
from nominal.smartcard._dependencies import assert_required_dependencies_available
from nominal.smartcard._pkcs11 import Pkcs11Backend
from nominal.smartcard.errors import SmartcardConfigurationError, SmartcardDependencyError


def _candidate(
    *,
    label: str = "PIV Authentication",
    slot: str | None = "9A",
    object_id: str | None = "01",
    fingerprint: str = "AA:BB",
    pkcs11_uri: str = "pkcs11:object=piv-auth",
    ekus: tuple[str, ...] = (),
) -> CertificateCandidate:
    return CertificateCandidate(
        label=label,
        token_label="CAC",
        slot=slot,
        object_id=object_id,
        sha256_fingerprint=fingerprint,
        pkcs11_uri=pkcs11_uri,
        der_certificate=b"cert",
        extended_key_usages=ekus,
    )


def test_discover_pkcs11_module_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))

    assert discover_pkcs11_module() == module_path


def test_discover_pkcs11_module_rejects_missing_explicit_path(tmp_path: Path) -> None:
    with pytest.raises(SmartcardConfigurationError, match="does not exist"):
        discover_pkcs11_module(tmp_path / "missing.so")


def test_select_piv_authentication_certificate_prefers_explicit_fingerprint() -> None:
    selected = _candidate(label="configured", fingerprint="11:22", slot=None, object_id=None)
    other = _candidate(fingerprint="33:44")

    assert (
        select_piv_authentication_certificate(
            [other, selected],
            SmartcardConfig(certificate_fingerprint="1122"),
        )
        is selected
    )


def test_select_piv_authentication_certificate_rejects_ambiguous_piv_candidates() -> None:
    first = _candidate(label="PIV Authentication 1", pkcs11_uri="pkcs11:object=one")
    second = _candidate(label="PIV Authentication 2", pkcs11_uri="pkcs11:object=two")

    with pytest.raises(SmartcardCertificateSelectionError, match="Multiple PIV Authentication"):
        select_piv_authentication_certificate([first, second], SmartcardConfig())


class _FakeBackend(Pkcs11Backend):
    def __init__(self, module_path: Path, candidates: list[CertificateCandidate]) -> None:
        super().__init__(module_path)
        self._candidates = candidates
        self.login_calls: list[tuple[CertificateCandidate, str]] = []
        self.close_calls = 0

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        return self._candidates

    def login(self, certificate: CertificateCandidate, pin: str) -> None:
        self.login_calls.append((certificate, pin))

    def close(self) -> None:
        self.close_calls += 1


def test_smartcard_session_manager_prompts_once_and_caches_session(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    backends: list[_FakeBackend] = []

    def backend_factory(path: Path) -> _FakeBackend:
        backend = _FakeBackend(path, [certificate])
        backends.append(backend)
        return backend

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "123456",
        backend_factory=backend_factory,
    )

    first = manager.get_session()
    second = manager.get_session()

    assert first is second
    assert first.module_path == module_path
    assert first.certificate is certificate
    assert len(backends) == 1
    assert backends[0].login_calls == [(certificate, "123456")]


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[SmartcardSession] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        return self.context


def test_smartcard_ssl_context_provider_builds_ssl_context(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "123456",
        backend_factory=lambda path: _FakeBackend(path, [_candidate()]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        config=SmartcardConfig(pkcs11_module_path=module_path),
        _session_manager=manager,
        _openssl_bridge=bridge,
    )

    ctx = provider.create_ssl_context()

    assert ctx is bridge.context
    assert len(bridge.calls) == 1


def test_smartcard_dependency_check_names_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    def find_spec(import_name: str):
        if import_name == "PyKCS11":
            return None
        return object()

    monkeypatch.setattr("nominal.smartcard._dependencies.importlib.util.find_spec", find_spec)

    with pytest.raises(SmartcardDependencyError, match="pip install 'nominal\\[smartcard\\]'"):
        assert_required_dependencies_available()
