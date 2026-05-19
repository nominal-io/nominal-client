from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from nominal.smartcard._cert_selection import CertificateCandidate, select_piv_authentication_certificate
from nominal.smartcard._pkcs11 import Pkcs11Backend, PyKCS11Backend, discover_pkcs11_module


@dataclass(frozen=True)
class SmartcardSession:
    """Selected smartcard module and PIV Authentication certificate."""

    module_path: Path
    certificate: CertificateCandidate

    @property
    def certificate_uri(self) -> str:
        return self.certificate.certificate_uri

    @property
    def private_key_uri(self) -> str:
        return self.certificate.private_key_uri


class SmartcardSessionManager:
    """Create and cache selected smartcard certificate metadata.

    One manager discovers the PKCS#11 certificate at most once. The shared manager gives profile-created clients the
    desired process-wide behavior while tests and other callers can still inject a dedicated manager.
    """

    _shared_lock = threading.Lock()
    _shared_manager: SmartcardSessionManager | None = None

    def __init__(
        self,
        *,
        backend_factory: Callable[[Path], Pkcs11Backend] = PyKCS11Backend,
    ) -> None:
        self._backend_factory = backend_factory
        self._lock = threading.Lock()
        self._session: SmartcardSession | None = None

    @classmethod
    def shared(cls) -> SmartcardSessionManager:
        with cls._shared_lock:
            if cls._shared_manager is None:
                cls._shared_manager = cls()
            return cls._shared_manager

    def get_session(self) -> SmartcardSession:
        with self._lock:
            if self._session is None:
                self._session = self._open_session()
            return self._session

    def close(self) -> None:
        with self._lock:
            self._session = None

    def _open_session(self) -> SmartcardSession:
        module_path = discover_pkcs11_module()
        backend = self._backend_factory(module_path)
        try:
            certificate = select_piv_authentication_certificate(
                backend.list_certificate_candidates(),
            )
        finally:
            backend.close()
        return SmartcardSession(module_path=module_path, certificate=certificate)
