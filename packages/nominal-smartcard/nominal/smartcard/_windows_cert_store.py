from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from nominal.smartcard._errors import SmartcardConfigurationError

NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR = "NOMINAL_WINDOWS_CERT_THUMBPRINT"

_OID_CLIENT_AUTH = "1.3.6.1.5.5.7.3.2"
_OID_ENHANCED_KEY_USAGE = "2.5.29.37"
_OID_KEY_USAGE = "2.5.29.15"


@dataclass(frozen=True)
class WindowsCertificateIdentity:
    r"""A selected Windows certificate from ``CurrentUser\My``.

    The ``certificate`` field is a .NET ``X509Certificate2`` whose private key
    remains managed by Windows. It is shared by the Windows HTTP adapter and the
    Windows CNG gRPC signer so every transport presents the same CAC identity.
    """

    certificate: Any
    der_certificate: bytes
    thumbprint: str
    subject: str
    issuer: str
    not_after: str
    public_key_oid: str

    def describe(self) -> str:
        return (
            f"thumbprint={self.thumbprint}, subject={self.subject!r}, issuer={self.issuer!r}, expires={self.not_after}"
        )

    def close(self) -> None:
        try:
            self.certificate.Dispose()
        except Exception:
            pass


class WindowsCertificateSelector:
    """Select the Windows CAC certificate shared by HTTP and gRPC."""

    def __init__(self, *, cert_thumbprint: str | None = None) -> None:
        self._cert_thumbprint = _normalize_thumbprint(cert_thumbprint)

    @classmethod
    def from_environment(cls, *, cert_thumbprint: str | None = None) -> WindowsCertificateSelector:
        configured_thumbprint = cert_thumbprint or os.environ.get(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR)
        return cls(cert_thumbprint=configured_thumbprint)

    def select(self) -> WindowsCertificateIdentity:
        return _load_windows_certificate(self._cert_thumbprint)


def _normalize_thumbprint(thumbprint: str | None) -> str | None:
    if thumbprint is None:
        return None
    normalized = "".join(ch for ch in thumbprint if not ch.isspace()).upper()
    return normalized or None


def _load_windows_certificate(cert_thumbprint: str | None) -> WindowsCertificateIdentity:
    r"""Open ``CurrentUser\My`` and select exactly one client-auth certificate."""
    import clr  # type: ignore[import-untyped]  # noqa: PLC0415

    clr.AddReference("System.Security")

    from System import DateTime  # type: ignore[import-not-found]
    from System.Security.Cryptography.X509Certificates import (  # type: ignore[import-not-found]
        OpenFlags,
        StoreLocation,
        StoreName,
        X509FindType,
        X509Store,
    )

    store = X509Store(StoreName.My, StoreLocation.CurrentUser)
    store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly)
    try:
        now = DateTime.Now
        if cert_thumbprint is not None:
            matches = store.Certificates.Find(X509FindType.FindByThumbprint, cert_thumbprint, False)
            candidates = [_to_identity(cert) for cert in matches if _is_usable_client_certificate(cert, now)]
            if len(candidates) == 1:
                return candidates[0]
            if not candidates:
                raise SmartcardConfigurationError(
                    f"Certificate with thumbprint {cert_thumbprint!r} was not found in CurrentUser\\My, "
                    "or it is expired, lacks a private key, or is not valid for client authentication."
                )
            raise SmartcardConfigurationError(
                f"Multiple usable certificates matched thumbprint {cert_thumbprint!r}; "
                "remove duplicate certificates from CurrentUser\\My."
            )

        candidates = [_to_identity(cert) for cert in store.Certificates if _is_usable_client_certificate(cert, now)]
        if len(candidates) == 1:
            return candidates[0]
        if not candidates:
            raise SmartcardConfigurationError(
                "No unexpired client-auth certificate with an accessible private key was found in CurrentUser\\My. "
                "Insert your CAC/smart card and ensure the Windows Smart Card service is running."
            )

        details = "\n".join(f"  - {candidate.describe()}" for candidate in candidates)
        raise SmartcardConfigurationError(
            "Multiple usable client-auth certificates were found in CurrentUser\\My. "
            f"Set {NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR} to choose one:\n{details}"
        )
    finally:
        store.Close()


def _is_usable_client_certificate(cert: Any, now: Any) -> bool:
    try:
        if not cert.HasPrivateKey:
            return False
        if cert.NotBefore > now or cert.NotAfter < now:
            return False
        if not _has_client_auth_eku(cert):
            return False
        if not _has_digital_signature_key_usage(cert):
            return False
        return True
    except Exception:
        return False


def _has_client_auth_eku(cert: Any) -> bool:
    for extension in cert.Extensions:
        oid = getattr(getattr(extension, "Oid", None), "Value", None)
        if str(oid) != _OID_ENHANCED_KEY_USAGE:
            continue
        enhanced_key_usages = getattr(extension, "EnhancedKeyUsages", None)
        if enhanced_key_usages is None:
            return False
        return any(str(usage.Value) == _OID_CLIENT_AUTH for usage in enhanced_key_usages)
    return False


def _has_digital_signature_key_usage(cert: Any) -> bool:
    from System.Security.Cryptography.X509Certificates import X509KeyUsageFlags

    saw_key_usage = False
    for extension in cert.Extensions:
        oid = getattr(getattr(extension, "Oid", None), "Value", None)
        if str(oid) != _OID_KEY_USAGE:
            continue
        saw_key_usage = True
        key_usages = getattr(extension, "KeyUsages", None)
        if key_usages is None:
            return False
        return (int(key_usages) & int(X509KeyUsageFlags.DigitalSignature)) != 0
    return not saw_key_usage


def _to_identity(cert: Any) -> WindowsCertificateIdentity:
    thumbprint = _normalize_thumbprint(str(cert.Thumbprint)) or ""
    return WindowsCertificateIdentity(
        certificate=cert,
        der_certificate=bytes(cert.RawData),
        thumbprint=thumbprint,
        subject=str(cert.Subject),
        issuer=str(cert.Issuer),
        not_after=str(cert.NotAfter),
        public_key_oid=str(cert.PublicKey.Oid.Value),
    )
