from __future__ import annotations

import os
import platform
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard._errors import SmartcardConfigurationError

NOMINAL_PKCS11_MODULE_ENV_VAR = "NOMINAL_PKCS11_MODULE"

CLIENT_AUTH_EKU = "1.3.6.1.5.5.7.3.2"

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

# Maps PKCS#11 CKA_ID (hex string) to PIV key reference slot label.
# Per NIST SP 800-73-4 and OpenSC conventions.
_OBJECT_ID_TO_PIV_SLOT: dict[str, str] = {
    "01": "9A",  # PIV Authentication
    "02": "9C",  # Digital Signature
    "03": "9D",  # Key Management
    "04": "9E",  # Card Authentication
}


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
    """Backend responsible for direct PKCS#11 token access."""

    def __init__(self, module_path: Path) -> None:
        self.module_path = module_path

    @abstractmethod
    def list_certificate_candidates(self) -> list[CertificateCandidate]: ...

    @abstractmethod
    def close(self) -> None: ...


# Characters allowed unencoded in a pk11-pchar value (RFC 7512 §2.3):
# unreserved (RFC 3986) + ":" / "[" / "]" / "@" / "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / ","
# Notably absent: ";" (path separator), "=" (name-value separator), "%" (must be part of pct-encoded).
_PK11_PCHAR_SAFE: frozenset[str] = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:[]@!$&'()*+,"
)


def _pct_encode_pk11_pchar(value: str) -> str:
    """Percent-encode every character outside the pk11-pchar set (RFC 7512 §2.3)."""
    parts = []
    for ch in value:
        if ch in _PK11_PCHAR_SAFE:
            parts.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                parts.append(f"%{byte:02x}")
    return "".join(parts)


def _build_pkcs11_uri(token_label: str, object_id_bytes: bytes) -> str:
    """Build a PKCS#11 URI for a token + object identifier.

    Format: pkcs11:token=TOKEN_LABEL;id=%XX%YY...
    Both the token label and the id bytes are percent-encoded per RFC 7512.
    """
    pct_id = "".join(f"%{b:02x}" for b in object_id_bytes)
    pct_label = _pct_encode_pk11_pchar(token_label)
    return f"pkcs11:token={pct_label};id={pct_id}"


class PyKCS11Backend(Pkcs11Backend):
    """PKCS#11 token backend backed by the PyKCS11 library."""

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)
        self._lib: Any = None
        # Maps pkcs11_uri → open session for that slot
        self._sessions: dict[str, Any] = {}

    def _get_lib(self) -> Any:
        if self._lib is not None:
            return self._lib
        try:
            import PyKCS11
        except ImportError as e:
            raise SmartcardConfigurationError(
                "PyKCS11 is not installed. Run `pip install 'nominal[smartcard]'`."
            ) from e
        lib = PyKCS11.PyKCS11Lib()
        try:
            lib.load(str(self.module_path))
        except PyKCS11.PyKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to load PKCS#11 module {self.module_path}: {e}") from e
        self._lib = lib
        return lib

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        import PyKCS11

        lib = self._get_lib()
        try:
            slots = lib.getSlotList(tokenPresent=True)
        except PyKCS11.PyKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        candidates: list[CertificateCandidate] = []
        for slot in slots:
            try:
                token_info = lib.getTokenInfo(slot)
                token_label = str(token_info.label).strip()
                session = lib.openSession(slot, PyKCS11.CKF_SERIAL_SESSION)
            except PyKCS11.PyKCS11Error:
                continue

            try:
                cert_objects = session.findObjects(
                    [
                        (PyKCS11.CKA_CLASS, PyKCS11.CKO_CERTIFICATE),
                        (PyKCS11.CKA_CERTIFICATE_TYPE, PyKCS11.CKC_X_509),
                    ]
                )
            except PyKCS11.PyKCS11Error:
                session.closeSession()
                continue

            for cert_obj in cert_objects:
                try:
                    attrs = session.getAttributeValue(
                        cert_obj,
                        [PyKCS11.CKA_LABEL, PyKCS11.CKA_ID, PyKCS11.CKA_VALUE],
                    )
                    label_raw, id_raw, value_raw = attrs[0], attrs[1], attrs[2]
                    label = str(label_raw).strip() if label_raw else None
                    object_id_bytes = bytes(id_raw) if id_raw else b""
                    der_certificate = bytes(value_raw) if value_raw else b""

                    object_id_str = object_id_bytes.hex() if object_id_bytes else None
                    piv_slot = _OBJECT_ID_TO_PIV_SLOT.get(object_id_str or "") if object_id_str else None
                    pkcs11_uri = _build_pkcs11_uri(token_label, object_id_bytes)

                    self._sessions[pkcs11_uri] = session

                    candidates.append(
                        CertificateCandidate(
                            label=label,
                            slot=piv_slot,
                            pkcs11_uri=pkcs11_uri,
                            der_certificate=der_certificate,
                        )
                    )
                except PyKCS11.PyKCS11Error:
                    continue

        return candidates

    def close(self) -> None:
        import PyKCS11

        seen_sessions: set[int] = set()
        for session in self._sessions.values():
            session_id = id(session)
            if session_id in seen_sessions:
                continue
            seen_sessions.add(session_id)
            try:
                session.closeSession()
            except PyKCS11.PyKCS11Error:
                pass
        self._sessions.clear()
        self._lib = None
