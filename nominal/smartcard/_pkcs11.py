from __future__ import annotations

import os
import platform
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard.errors import (
    SmartcardConfigurationError,
    SmartcardPinError,
    SmartcardPinLockedError,
)

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
    def login(self, certificate: CertificateCandidate, pin: str) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


def _build_pkcs11_uri(token_label: str, object_id_bytes: bytes) -> str:
    """Build a PKCS#11 URI for a token + object identifier.

    Format: pkcs11:token=TOKEN_LABEL;id=%XX%YY...
    The id bytes are percent-encoded per RFC 7512.
    """
    pct_id = "".join(f"%{b:02x}" for b in object_id_bytes)
    # RFC 7512 requires spaces in token labels to be percent-encoded or preserved.
    # OpenSC/pkcs11-provider handle spaces in token labels, so we keep them as-is.
    return f"pkcs11:token={token_label};id={pct_id}"


def _parse_certificate_metadata(der_cert: bytes) -> tuple[str, tuple[str, ...]]:
    """Return (sha256_fingerprint, extended_key_usages) for a DER-encoded certificate."""
    from cryptography import x509 as cryptography_x509
    from cryptography.hazmat.primitives import hashes

    cert = cryptography_x509.load_der_x509_certificate(der_cert)
    fp_bytes = cert.fingerprint(hashes.SHA256())
    sha256_fingerprint = ":".join(f"{b:02x}" for b in fp_bytes)

    try:
        ekus_ext = cert.extensions.get_extension_for_oid(cryptography_x509.ExtensionOID.EXTENDED_KEY_USAGE)
        ekus: tuple[str, ...] = tuple(oid.dotted_string for oid in ekus_ext.value)
    except cryptography_x509.ExtensionNotFound:
        ekus = ()

    return sha256_fingerprint, ekus


class PyKCS11Backend(Pkcs11Backend):
    """PKCS#11 token backend backed by the PyKCS11 library."""

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)
        self._lib: Any = None
        # Maps (token_label, hex_object_id) → open session for that slot
        self._sessions: dict[tuple[str, str], Any] = {}

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
                        [
                            PyKCS11.CKA_LABEL,
                            PyKCS11.CKA_ID,
                            PyKCS11.CKA_VALUE,
                        ],
                    )
                    label_raw, id_raw, value_raw = attrs[0], attrs[1], attrs[2]
                    label = str(label_raw).strip() if label_raw else None
                    object_id_bytes = bytes(id_raw) if id_raw else b""
                    der_cert = bytes(value_raw) if value_raw else b""

                    if not der_cert:
                        continue

                    object_id_str = object_id_bytes.hex() if object_id_bytes else None
                    piv_slot = _OBJECT_ID_TO_PIV_SLOT.get(object_id_str or "") if object_id_str else None
                    pkcs11_uri = _build_pkcs11_uri(token_label, object_id_bytes)
                    sha256_fingerprint, ekus = _parse_certificate_metadata(der_cert)

                    session_key = (token_label, object_id_str or "")
                    self._sessions[session_key] = session

                    candidates.append(
                        CertificateCandidate(
                            label=label,
                            token_label=token_label,
                            slot=piv_slot,
                            object_id=object_id_str,
                            sha256_fingerprint=sha256_fingerprint,
                            pkcs11_uri=pkcs11_uri,
                            der_certificate=der_cert,
                            extended_key_usages=ekus,
                        )
                    )
                except PyKCS11.PyKCS11Error:
                    continue

        return candidates

    def login(self, certificate: CertificateCandidate, pin: str) -> None:
        import PyKCS11

        session_key = (certificate.token_label or "", certificate.object_id or "")
        session = self._sessions.get(session_key)
        if session is None:
            raise SmartcardConfigurationError(
                f"No open PKCS#11 session for token {certificate.token_label!r} / "
                f"object {certificate.object_id!r}. Call list_certificate_candidates() first."
            )

        try:
            session.login(PyKCS11.CKU_USER, pin)
        except PyKCS11.PyKCS11Error as e:
            error_code = e.value if hasattr(e, "value") else None
            # CKR_PIN_LOCKED = 0x000000A4
            if error_code == 0xA4:
                raise SmartcardPinLockedError("CAC PIN is locked. Contact your CAC office to unlock it.") from e
            # CKR_PIN_INCORRECT = 0x000000A0, CKR_PIN_INVALID = 0x000000A1
            if error_code in (0xA0, 0xA1):
                raise SmartcardPinError("Incorrect PIN.") from e
            raise SmartcardConfigurationError(f"PKCS#11 login failed: {e}") from e

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
