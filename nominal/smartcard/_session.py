from __future__ import annotations

import getpass
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from nominal.smartcard._cert_selection import CertificateCandidate, select_piv_authentication_certificate
from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._pkcs11 import Pkcs11Backend, PyKCS11Backend, discover_pkcs11_module
from nominal.smartcard.errors import SmartcardPinError, SmartcardPinLockedError

PinProvider = Callable[[str], str]
BackendFactory = Callable[[Path], Pkcs11Backend]

# CAC standard: 3 incorrect PIN attempts before the card locks.
_CAC_MAX_PIN_ATTEMPTS = 3


@dataclass(frozen=True)
class SmartcardSession:
    """A logged-in smartcard session and selected PIV Authentication certificate."""

    module_path: Path
    certificate: CertificateCandidate
    backend: Pkcs11Backend = field(repr=False)

    @property
    def pkcs11_uri(self) -> str:
        return self.certificate.pkcs11_uri


class SmartcardSessionManager:
    """Create and cache a logged-in smartcard session.

    One manager prompts for the PIN at most once. The shared manager registry gives profile-created clients the desired
    process-wide behavior while tests and advanced callers can still inject a dedicated manager.
    """

    _shared_lock = threading.Lock()
    _shared_managers: dict[SmartcardConfig, SmartcardSessionManager] = {}

    def __init__(
        self,
        config: SmartcardConfig,
        *,
        pin_provider: PinProvider = getpass.getpass,
        backend_factory: BackendFactory = PyKCS11Backend,
    ) -> None:
        self._config = config
        self._pin_provider = pin_provider
        self._backend_factory = backend_factory
        self._lock = threading.Lock()
        self._session: SmartcardSession | None = None

    @classmethod
    def shared(cls, config: SmartcardConfig) -> SmartcardSessionManager:
        with cls._shared_lock:
            manager = cls._shared_managers.get(config)
            if manager is None:
                manager = cls(config)
                cls._shared_managers[config] = manager
            return manager

    def get_session(self) -> SmartcardSession:
        with self._lock:
            if self._session is None:
                self._session = self._open_session()
            return self._session

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                self._session.backend.close()
                self._session = None

    def _open_session(self) -> SmartcardSession:
        module_path = discover_pkcs11_module(self._config.pkcs11_module_path)
        backend = self._backend_factory(module_path)
        certificate = select_piv_authentication_certificate(
            backend.list_certificate_candidates(),
        )

        for attempt in range(_CAC_MAX_PIN_ATTEMPTS):
            remaining_after = _CAC_MAX_PIN_ATTEMPTS - attempt - 1
            if attempt == 0:
                prompt = self._config.pin_prompt
            else:
                prompt = (
                    f"Incorrect PIN — {remaining_after} attempt(s) remaining before card locks. "
                    f"{self._config.pin_prompt}"
                )
            pin = self._pin_provider(prompt)
            try:
                backend.login(certificate, pin)
                break
            except SmartcardPinLockedError:
                raise
            except SmartcardPinError as e:
                if remaining_after == 0:
                    raise SmartcardPinError(
                        f"Incorrect PIN entered {_CAC_MAX_PIN_ATTEMPTS} times. "
                        "Contact your CAC office if the card is now locked."
                    ) from e
            finally:
                del pin

        return SmartcardSession(module_path=module_path, certificate=certificate, backend=backend)
