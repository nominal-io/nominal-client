from __future__ import annotations

import threading
from pathlib import Path

import pytest

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._pkcs11 import Pkcs11Backend
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard.errors import SmartcardPinError, SmartcardPinLockedError
from tests.smartcard._helpers import _candidate, _FakeBackend


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
