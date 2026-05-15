from __future__ import annotations

import threading
from pathlib import Path

import pytest

from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._errors import SmartcardCertificateSelectionError
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from tests.smartcard._helpers import _candidate, _FakeBackend


def test_smartcard_session_manager_caches_session(tmp_path: Path) -> None:
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
        backend_factory=backend_factory,
    )

    first = manager.get_session()
    second = manager.get_session()

    assert first is second
    assert first.module_path == module_path
    assert first.certificate is certificate
    assert len(backends) == 1


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
        backend_factory=backend_factory,
    )
    manager.get_session()
    manager.close()
    manager.get_session()

    assert len(backends) == 2
    assert backends[0].close_calls == 1


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


def test_smartcard_session_manager_closes_backend_on_cert_selection_failure(tmp_path: Path) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    backends: list[_FakeBackend] = []

    def backend_factory(path: Path) -> _FakeBackend:
        b = _FakeBackend(path, [])  # no candidates → cert selection will raise
        backends.append(b)
        return b

    manager = SmartcardSessionManager(
        SmartcardConfig(pkcs11_module_path=module_path),
        backend_factory=backend_factory,
    )
    with pytest.raises(SmartcardCertificateSelectionError):
        manager.get_session()

    assert len(backends) == 1
    assert backends[0].close_calls == 1
