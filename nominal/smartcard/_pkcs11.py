from __future__ import annotations

import os
import platform
from abc import ABC, abstractmethod
from pathlib import Path

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard.errors import SmartcardConfigurationError, SmartcardNotImplementedError

NOMINAL_PKCS11_MODULE_ENV_VAR = "NOMINAL_PKCS11_MODULE"

_LINUX_OPENSC_PATHS = (
    "/usr/lib64/opensc-pkcs11.so",
    "/usr/lib/x86_64-linux-gnu/opensc-pkcs11.so",
    "/usr/lib/aarch64-linux-gnu/opensc-pkcs11.so",
    "/usr/lib/opensc-pkcs11.so",
)
_MACOS_OPENSC_PATHS = (
    "/Library/OpenSC/lib/opensc-pkcs11.so",
    "/opt/homebrew/lib/opensc-pkcs11.so",
    "/usr/local/lib/opensc-pkcs11.so",
)
_WINDOWS_OPENSC_PATHS = (
    r"C:\Program Files\OpenSC Project\OpenSC\pkcs11\opensc-pkcs11.dll",
    r"C:\Program Files (x86)\OpenSC Project\OpenSC\pkcs11\opensc-pkcs11.dll",
)


def discover_pkcs11_module(explicit_path: Path | None = None) -> Path:
    """Find the OpenSC PKCS#11 module used to communicate with the smartcard."""
    configured_path = explicit_path
    if configured_path is None:
        env_path = os.environ.get(NOMINAL_PKCS11_MODULE_ENV_VAR)
        configured_path = Path(env_path) if env_path else None

    if configured_path is not None:
        module_path = configured_path.expanduser()
        if module_path.exists():
            return module_path
        raise SmartcardConfigurationError(f"Configured PKCS#11 module does not exist: {module_path}")

    for candidate in _platform_default_paths():
        path = Path(candidate)
        if path.exists():
            return path

    raise SmartcardConfigurationError(
        "Could not find an OpenSC PKCS#11 module. Install OpenSC or set "
        f"{NOMINAL_PKCS11_MODULE_ENV_VAR} to the module path."
    )


def _platform_default_paths() -> tuple[str, ...]:
    system = platform.system()
    if system == "Darwin":
        return _MACOS_OPENSC_PATHS
    if system == "Windows":
        return _WINDOWS_OPENSC_PATHS
    return _LINUX_OPENSC_PATHS


class Pkcs11Backend(ABC):
    """Backend responsible for direct PKCS#11 token access.

    The production implementation will use PyKCS11 here. The surrounding session manager is already final and testable;
    these methods are the intentionally empty hardware boundary.
    """

    def __init__(self, module_path: Path) -> None:
        self.module_path = module_path

    @abstractmethod
    def list_certificate_candidates(self) -> list[CertificateCandidate]: ...

    @abstractmethod
    def login(self, certificate: CertificateCandidate, pin: str) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class PyKCS11Backend(Pkcs11Backend):
    """PyKCS11-backed token reader placeholder."""

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        raise SmartcardNotImplementedError("PKCS#11 certificate enumeration is not implemented yet.")

    def login(self, certificate: CertificateCandidate, pin: str) -> None:
        del certificate, pin
        raise SmartcardNotImplementedError("PKCS#11 smartcard login is not implemented yet.")

    def close(self) -> None:
        raise SmartcardNotImplementedError("PKCS#11 session close is not implemented yet.")
