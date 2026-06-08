from __future__ import annotations

import os
import platform
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pkcs11

from nominal.smartcard.pkcs11._cert_selection import PIV_AUTHENTICATION_SLOT, CertificateCandidate
from nominal.smartcard._errors import SmartcardConfigurationError

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

# Maps PKCS#11 CKA_ID (hex string) to PIV key reference slot label.
_OBJECT_ID_TO_PIV_SLOT: dict[str, str] = {
    "01": PIV_AUTHENTICATION_SLOT,  # PIV Authentication
    "02": "9C",  # Digital Signature
    "03": "9D",  # Key Management
    "04": "9E",  # Card Authentication
}


def discover_pkcs11_module() -> Path:
    """Find the OpenSC PKCS#11 module used to communicate with the smartcard."""
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
    if system == "Linux":
        return _LINUX_OPENSC_PATHS

    raise SmartcardConfigurationError(f"Unsupported platform: {system}")


class Pkcs11Backend(ABC):
    """Backend responsible for direct PKCS#11 token discovery."""

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


def _build_pkcs11_uri(token_label: str, object_id_bytes: bytes, *, object_type: str | None = None) -> str:
    """Build a PKCS#11 URI for a token + object identifier.

    Format: pkcs11:token=TOKEN_LABEL;id=%XX%YY...;type=OBJECT_TYPE
    Both the token label and the id bytes are percent-encoded per RFC 7512.
    """
    pct_id = "".join(f"%{b:02x}" for b in object_id_bytes)
    pct_label = _pct_encode_pk11_pchar(token_label)
    uri = f"pkcs11:token={pct_label};id={pct_id}"
    if object_type is not None:
        uri += f";type={object_type}"
    return uri


class DefaultPkcs11Backend(Pkcs11Backend):
    """PKCS#11 token backend backed by the python-pkcs11 library."""

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)
        self._lib: Any = None

    def _get_lib(self) -> Any:
        if self._lib is not None:
            return self._lib

        try:
            lib = pkcs11.lib(str(self.module_path))
        except Exception as e:
            raise SmartcardConfigurationError(f"Failed to load PKCS#11 module {self.module_path}: {e}") from e

        self._lib = lib
        return lib

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        lib = self._get_lib()

        try:
            slots = lib.get_slots(token_present=True)
        except pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        candidates: list[CertificateCandidate] = []
        for slot in slots:
            try:
                token = slot.get_token()
                token_label = token.label.strip()
                with token.open() as session:
                    for cert_obj in session.get_objects(
                        {
                            pkcs11.Attribute.CLASS: pkcs11.ObjectClass.CERTIFICATE,
                            pkcs11.Attribute.CERTIFICATE_TYPE: pkcs11.CertificateType.X_509,
                        }
                    ):
                        try:
                            label_raw = cert_obj[pkcs11.Attribute.LABEL]
                            label = label_raw.strip() if isinstance(label_raw, str) else None
                        except pkcs11.exceptions.PKCS11Error:
                            label = None

                        try:
                            object_id_bytes = cert_obj[pkcs11.Attribute.ID]
                            if not isinstance(object_id_bytes, (bytes, bytearray)):
                                object_id_bytes = bytes(object_id_bytes)
                        except pkcs11.exceptions.PKCS11Error:
                            object_id_bytes = None

                        try:
                            der_certificate = cert_obj[pkcs11.Attribute.VALUE]
                            if not isinstance(der_certificate, (bytes, bytearray)):
                                der_certificate = bytes(der_certificate)
                        except pkcs11.exceptions.PKCS11Error:
                            der_certificate = b""

                        if object_id_bytes is None:
                            continue

                        object_id_str = object_id_bytes.hex()
                        piv_slot = _OBJECT_ID_TO_PIV_SLOT.get(object_id_str)
                        certificate_uri = _build_pkcs11_uri(token_label, object_id_bytes, object_type="cert")
                        private_key_uri = _build_pkcs11_uri(token_label, object_id_bytes, object_type="private")

                        candidates.append(
                            CertificateCandidate(
                                label=label,
                                slot=piv_slot,
                                certificate_uri=certificate_uri,
                                private_key_uri=private_key_uri,
                                der_certificate=der_certificate,
                                token_label=token_label,
                                object_id_bytes=object_id_bytes,
                            )
                        )
            except pkcs11.exceptions.PKCS11Error:
                pass  # Skip slots we can't access or that don't conform to expected structure

        return candidates

    def close(self) -> None:
        self._lib = None
