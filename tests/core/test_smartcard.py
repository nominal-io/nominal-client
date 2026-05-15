from __future__ import annotations

import ssl
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

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
from nominal.smartcard._openssl_provider import _key_uri_from_cert_uri
from nominal.smartcard._pkcs11 import Pkcs11Backend, _build_pkcs11_uri
from nominal.smartcard.errors import (
    SmartcardConfigurationError,
    SmartcardDependencyError,
    SmartcardPinError,
    SmartcardPinLockedError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _candidate(
    *,
    label: str = "PIV Authentication",
    slot: str | None = "9A",
    pkcs11_uri: str = "pkcs11:token=CAC;id=%01",
) -> CertificateCandidate:
    return CertificateCandidate(
        label=label,
        slot=slot,
        pkcs11_uri=pkcs11_uri,
    )


class _FakeBackend(Pkcs11Backend):
    def __init__(
        self,
        module_path: Path,
        candidates: list[CertificateCandidate],
        *,
        pin_error: Exception | None = None,
    ) -> None:
        super().__init__(module_path)
        self._candidates = candidates
        self._pin_error = pin_error
        self.login_calls: list[tuple[CertificateCandidate, str]] = []
        self.close_calls = 0

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        return self._candidates

    def login(self, certificate: CertificateCandidate, pin: str) -> None:
        self.login_calls.append((certificate, pin))
        if self._pin_error is not None:
            raise self._pin_error

    def close(self) -> None:
        self.close_calls += 1


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[SmartcardSession] = []
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        self.calls.append(session)
        return self.context


# ---------------------------------------------------------------------------
# discover_pkcs11_module
# ---------------------------------------------------------------------------


def test_discover_pkcs11_module_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))

    assert discover_pkcs11_module() == module_path


def test_discover_pkcs11_module_explicit_path_takes_priority_over_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_path = tmp_path / "env.so"
    env_path.write_text("")
    explicit_path = tmp_path / "explicit.so"
    explicit_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(env_path))

    assert discover_pkcs11_module(explicit_path) == explicit_path


def test_discover_pkcs11_module_rejects_missing_explicit_path(tmp_path: Path) -> None:
    with pytest.raises(SmartcardConfigurationError, match="does not exist"):
        discover_pkcs11_module(tmp_path / "missing.so")


def test_discover_pkcs11_module_rejects_missing_env_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(tmp_path / "missing.so"))
    with pytest.raises(SmartcardConfigurationError, match="does not exist"):
        discover_pkcs11_module()


def test_discover_pkcs11_module_falls_back_to_platform_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_so = tmp_path / "opensc-pkcs11.so"
    fake_so.write_text("")
    monkeypatch.delenv(NOMINAL_PKCS11_MODULE_ENV_VAR, raising=False)

    from nominal.smartcard import _pkcs11

    monkeypatch.setattr(_pkcs11, "_platform_default_paths", lambda: (str(fake_so),))
    assert discover_pkcs11_module() == fake_so


def test_discover_pkcs11_module_raises_when_no_platform_path_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(NOMINAL_PKCS11_MODULE_ENV_VAR, raising=False)

    from nominal.smartcard import _pkcs11

    monkeypatch.setattr(_pkcs11, "_platform_default_paths", lambda: ())
    with pytest.raises(SmartcardConfigurationError, match="OpenSC"):
        discover_pkcs11_module()


# ---------------------------------------------------------------------------
# CertificateCandidate.is_piv_authentication_candidate
# ---------------------------------------------------------------------------


def test_is_piv_candidate_slot_9a() -> None:
    assert _candidate(slot="9A").is_piv_authentication_candidate


def test_is_piv_candidate_slot_case_insensitive() -> None:
    assert _candidate(slot="9a").is_piv_authentication_candidate


def test_is_not_piv_candidate_slot_9c() -> None:
    c = _candidate(label="Digital Signature", slot="9C")
    assert not c.is_piv_authentication_candidate


def test_is_not_piv_candidate_no_slot() -> None:
    c = _candidate(slot=None)
    assert not c.is_piv_authentication_candidate


# ---------------------------------------------------------------------------
# select_piv_authentication_certificate
# ---------------------------------------------------------------------------


def test_select_raises_when_no_candidates() -> None:
    with pytest.raises(SmartcardCertificateSelectionError, match="No certificates"):
        select_piv_authentication_certificate([])


def test_select_single_piv_auth_candidate() -> None:
    piv = _candidate(slot="9A")
    dig = _candidate(label="Digital Signature", slot="9C")
    assert select_piv_authentication_certificate([dig, piv]) is piv


def test_select_rejects_ambiguous_piv_candidates() -> None:
    first = _candidate(label="PIV Authentication 1", pkcs11_uri="pkcs11:object=one")
    second = _candidate(label="PIV Authentication 2", pkcs11_uri="pkcs11:object=two")

    with pytest.raises(SmartcardCertificateSelectionError, match="Multiple PIV Authentication"):
        select_piv_authentication_certificate([first, second])


def test_select_no_piv_candidates_raises_with_discovered_list() -> None:
    c = _candidate(label="Digital Signature", slot="9C")
    with pytest.raises(SmartcardCertificateSelectionError, match="Digital Signature"):
        select_piv_authentication_certificate([c])


# ---------------------------------------------------------------------------
# SmartcardSessionManager — caching and PIN retry
# ---------------------------------------------------------------------------


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


def test_smartcard_session_manager_close_clears_session(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    backends: list[_FakeBackend] = []

    def backend_factory(path: Path) -> _FakeBackend:
        b = _FakeBackend(path, [certificate])
        backends.append(b)
        return b

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "123456",
        backend_factory=backend_factory,
    )
    manager.get_session()
    manager.close()
    manager.get_session()

    assert len(backends) == 2
    assert backends[0].close_calls == 1


def test_smartcard_session_manager_retries_on_pin_error(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    call_count = 0

    class _RetryBackend(Pkcs11Backend):
        def list_certificate_candidates(self) -> list[CertificateCandidate]:
            return [certificate]

        def login(self, cert: CertificateCandidate, pin: str) -> None:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise SmartcardPinError("wrong PIN")

        def close(self) -> None:
            pass

    prompts: list[str] = []

    def pin_provider(prompt: str) -> str:
        prompts.append(prompt)
        return "pin"

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=pin_provider,
        backend_factory=lambda path: _RetryBackend(path),
    )
    session = manager.get_session()
    assert session is not None
    assert call_count == 3
    assert len(prompts) == 3
    assert "remaining" in prompts[1]


def test_smartcard_session_manager_raises_after_max_attempts(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()

    class _AlwaysWrongPin(Pkcs11Backend):
        def list_certificate_candidates(self) -> list[CertificateCandidate]:
            return [certificate]

        def login(self, cert: CertificateCandidate, pin: str) -> None:
            raise SmartcardPinError("wrong PIN")

        def close(self) -> None:
            pass

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "bad",
        backend_factory=lambda path: _AlwaysWrongPin(path),
    )
    with pytest.raises(SmartcardPinError):
        manager.get_session()


def test_smartcard_session_manager_propagates_pin_locked_immediately(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    call_count = 0

    class _LockedPin(Pkcs11Backend):
        def list_certificate_candidates(self) -> list[CertificateCandidate]:
            return [certificate]

        def login(self, cert: CertificateCandidate, pin: str) -> None:
            nonlocal call_count
            call_count += 1
            raise SmartcardPinLockedError("locked")

        def close(self) -> None:
            pass

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "pin",
        backend_factory=lambda path: _LockedPin(path),
    )
    with pytest.raises(SmartcardPinLockedError):
        manager.get_session()
    assert call_count == 1


def test_smartcard_session_manager_thread_safety(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    backend_count = 0

    def backend_factory(path: Path) -> _FakeBackend:
        nonlocal backend_count
        backend_count += 1
        return _FakeBackend(path, [certificate])

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "1234",
        backend_factory=backend_factory,
    )

    results: list[SmartcardSession] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(10)

    def worker() -> None:
        try:
            barrier.wait()
            results.append(manager.get_session())
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert all(s is results[0] for s in results)
    assert backend_count == 1


# ---------------------------------------------------------------------------
# SmartcardSslContextProvider
# ---------------------------------------------------------------------------


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


def test_smartcard_ssl_context_provider_passes_session_to_bridge(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    certificate = _candidate()
    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        pin_provider=lambda prompt: "pin",
        backend_factory=lambda path: _FakeBackend(path, [certificate]),
    )
    bridge = _FakeBridge()
    provider = SmartcardSslContextProvider(
        config=SmartcardConfig(pkcs11_module_path=module_path),
        _session_manager=manager,
        _openssl_bridge=bridge,
    )
    provider.create_ssl_context()

    assert bridge.calls[0].certificate is certificate


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------


def test_smartcard_dependency_check_names_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    def find_spec(import_name: str):
        if import_name == "PyKCS11":
            return None
        return object()

    monkeypatch.setattr("nominal.smartcard._dependencies.importlib.util.find_spec", find_spec)

    with pytest.raises(SmartcardDependencyError, match="pip install 'nominal\\[smartcard\\]'"):
        assert_required_dependencies_available()


def test_smartcard_dependency_check_passes_when_all_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("nominal.smartcard._dependencies.importlib.util.find_spec", lambda _: object())
    assert_required_dependencies_available() is None  # no exception


# ---------------------------------------------------------------------------
# _build_pkcs11_uri
# ---------------------------------------------------------------------------


def test_build_pkcs11_uri_single_byte() -> None:
    assert _build_pkcs11_uri("MY TOKEN", b"\x01") == "pkcs11:token=MY%20TOKEN;id=%01"


def test_build_pkcs11_uri_multi_byte() -> None:
    assert _build_pkcs11_uri("CAC", b"\x0a\xff") == "pkcs11:token=CAC;id=%0a%ff"


def test_build_pkcs11_uri_empty_id() -> None:
    assert _build_pkcs11_uri("CAC", b"") == "pkcs11:token=CAC;id="


# ---------------------------------------------------------------------------
# _key_uri_from_cert_uri
# ---------------------------------------------------------------------------


def test_key_uri_from_cert_uri_appends_type_private() -> None:
    assert _key_uri_from_cert_uri("pkcs11:token=CAC;id=%01") == "pkcs11:token=CAC;id=%01;type=private"


def test_key_uri_from_cert_uri_replaces_existing_type() -> None:
    assert _key_uri_from_cert_uri("pkcs11:token=CAC;id=%01;type=cert") == "pkcs11:token=CAC;id=%01;type=private"


def test_key_uri_from_cert_uri_strips_type_anywhere() -> None:
    result = _key_uri_from_cert_uri("pkcs11:type=cert;token=CAC;id=%01")
    assert "type=private" in result
    assert "type=cert" not in result


# ---------------------------------------------------------------------------
# PyKCS11Backend (via mock)
# ---------------------------------------------------------------------------


def _make_mock_pykcs11_env():
    """Return (mock_PyKCS11_module, mock_session)."""
    PyKCS11 = MagicMock()
    PyKCS11.CKF_SERIAL_SESSION = 4
    PyKCS11.CKU_USER = 1
    PyKCS11.CKA_CLASS = 0
    PyKCS11.CKO_CERTIFICATE = 1
    PyKCS11.CKA_CERTIFICATE_TYPE = 0x80
    PyKCS11.CKC_X_509 = 0
    PyKCS11.CKA_LABEL = 3
    PyKCS11.CKA_ID = 0x102

    token_info = MagicMock()
    token_info.label = "CAC TOKEN     "

    cert_obj = MagicMock()
    session = MagicMock()
    session.findObjects.return_value = [cert_obj]
    session.getAttributeValue.return_value = ["PIV Authentication", b"\x01"]

    lib = MagicMock()
    lib.getSlotList.return_value = [0]
    lib.getTokenInfo.return_value = token_info
    lib.openSession.return_value = session

    PyKCS11.PyKCS11Lib.return_value = lib
    PyKCS11.PyKCS11Error = type("PyKCS11Error", (Exception,), {"value": 0})

    return PyKCS11, session


def test_pykcs11_backend_list_certificate_candidates(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, _ = _make_mock_pykcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1
    c = candidates[0]
    assert c.label == "PIV Authentication"
    assert c.slot == "9A"
    assert c.pkcs11_uri == "pkcs11:token=CAC%20TOKEN;id=%01"


def test_pykcs11_backend_login_calls_session_login(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, mock_session = _make_mock_pykcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()
        backend.login(candidates[0], "123456")

    mock_session.login.assert_called_once_with(mock_pykcs11.CKU_USER, "123456")


def test_pykcs11_backend_login_raises_smartcard_pin_error_on_incorrect_pin(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, mock_session = _make_mock_pykcs11_env()
    wrong_pin_err = mock_pykcs11.PyKCS11Error("wrong pin")
    wrong_pin_err.value = 0xA0  # CKR_PIN_INCORRECT
    mock_session.login.side_effect = wrong_pin_err

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()
        with pytest.raises(SmartcardPinError):
            backend.login(candidates[0], "wrong")


def test_pykcs11_backend_login_raises_pin_locked_error(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, mock_session = _make_mock_pykcs11_env()
    locked_err = mock_pykcs11.PyKCS11Error("locked")
    locked_err.value = 0xA4  # CKR_PIN_LOCKED
    mock_session.login.side_effect = locked_err

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()
        with pytest.raises(SmartcardPinLockedError):
            backend.login(candidates[0], "wrong")


def test_pykcs11_backend_close_closes_sessions(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, mock_session = _make_mock_pykcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        backend.list_certificate_candidates()
        backend.close()

    mock_session.closeSession.assert_called_once()


def test_pykcs11_backend_skips_slots_that_fail_to_open(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    mock_pykcs11, _ = _make_mock_pykcs11_env()
    lib = mock_pykcs11.PyKCS11Lib.return_value
    lib.getSlotList.return_value = [0, 1]
    good_session = lib.openSession.return_value
    lib.openSession.side_effect = [good_session, mock_pykcs11.PyKCS11Error("no token")]

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1


def test_pykcs11_backend_login_raises_when_no_session_for_certificate(tmp_path: Path) -> None:
    from nominal.smartcard._pkcs11 import PyKCS11Backend

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    # Build a backend without calling list_certificate_candidates first
    mock_pykcs11, _ = _make_mock_pykcs11_env()

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        # Attempt login with a candidate that was never enrolled
        orphan = _candidate(pkcs11_uri="pkcs11:token=NOTFOUND;id=%99")
        with pytest.raises(SmartcardConfigurationError, match="No open PKCS#11 session"):
            backend.login(orphan, "pin")


# ---------------------------------------------------------------------------
# OpenSslProviderBridge._get_ssl_ctx_ptr smoke test
# ---------------------------------------------------------------------------


def test_get_ssl_ctx_ptr_returns_nonzero() -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("typedef struct ssl_ctx_st SSL_CTX;")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    from nominal.smartcard._openssl_provider import _get_ssl_ctx_ptr

    ptr = _get_ssl_ctx_ptr(ffi, ctx)
    assert int(ffi.cast("uintptr_t", ptr)) != 0
