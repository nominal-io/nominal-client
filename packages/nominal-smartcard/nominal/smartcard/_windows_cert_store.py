from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nominal.smartcard._errors import SmartcardConfigurationError

_OID_CLIENT_AUTH = "1.3.6.1.5.5.7.3.2"
_OID_SMARTCARD_LOGON = "1.3.6.1.4.1.311.20.2.2"
_OID_ENHANCED_KEY_USAGE = "2.5.29.37"
_OID_KEY_USAGE = "2.5.29.15"

_ATTR_OID = "Oid"
_ATTR_VALUE = "Value"
_ATTR_ENHANCED_KEY_USAGES = "EnhancedKeyUsages"
_ATTR_KEY_USAGES = "KeyUsages"


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


def _normalize_thumbprint(thumbprint: str | None) -> str | None:
    if thumbprint is None:
        return None
    normalized = "".join(ch for ch in thumbprint if not ch.isspace()).upper()
    return normalized or None


def select_windows_certificate() -> WindowsCertificateIdentity:
    r"""Open ``CurrentUser\My`` and select the client-auth certificate to authenticate with.

    Shared by the Windows HTTP adapter and the Windows CNG gRPC signer so every
    transport presents the same CAC identity.
    """
    import clr  # type: ignore[import-untyped]  # noqa: PLC0415

    clr.AddReference("System.Security")

    from System import DateTime  # type: ignore[import-not-found]
    from System.Security.Cryptography.X509Certificates import (  # type: ignore[import-not-found]
        OpenFlags,
        StoreLocation,
        StoreName,
        X509Store,
    )

    store = X509Store(StoreName.My, StoreLocation.CurrentUser)
    store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly)
    try:
        certificates = list(store.Certificates)
        identity = _select_certificate(certificates, now=DateTime.Now)
        for cert in certificates:
            if cert is not identity.certificate:
                try:
                    cert.Dispose()
                except Exception:
                    pass
        return identity
    finally:
        store.Close()


def _select_certificate(certificates: Any, *, now: Any) -> WindowsCertificateIdentity:
    """Pick the certificate to authenticate with from an enumerable of ``X509Certificate2``."""
    usable = [(cert, _to_identity(cert)) for cert in certificates if _is_usable_client_certificate(cert, now)]
    if len(usable) == 1:
        return usable[0][1]
    if not usable:
        raise SmartcardConfigurationError(
            "No unexpired client-auth certificate with an accessible private key was found in CurrentUser\\My. "
            "Insert your CAC/smart card and ensure the Windows Smart Card service is running."
        )

    logon = [identity for cert, identity in usable if _has_eku(cert, _OID_SMARTCARD_LOGON)]
    if len(logon) == 1:
        return logon[0]

    details = "\n".join(f"  - {identity.describe()}" for _, identity in usable)
    raise SmartcardConfigurationError(
        "Multiple client-authentication certificates with an accessible private key were found in "
        "CurrentUser\\My, and the PIV Authentication certificate could not be identified unambiguously "
        f"(none or several carry the Smart Card Logon EKU):\n{details}"
    )


def _is_usable_client_certificate(cert: Any, now: Any) -> bool:
    try:
        if not cert.HasPrivateKey:
            return False
        if cert.NotBefore > now or cert.NotAfter < now:
            return False
        if not _has_eku(cert, _OID_CLIENT_AUTH):
            return False
        if not _has_digital_signature_key_usage(cert):
            return False
        return True
    except Exception:
        return False


def _has_eku(cert: Any, target_oid: str) -> bool:
    """Return whether the certificate's ExtendedKeyUsage extension lists ``target_oid``."""
    for extension in cert.Extensions:
        oid = getattr(getattr(extension, _ATTR_OID, None), _ATTR_VALUE, None)
        if str(oid) != _OID_ENHANCED_KEY_USAGE:
            continue
        enhanced_key_usages = getattr(extension, _ATTR_ENHANCED_KEY_USAGES, None)
        if enhanced_key_usages is None:
            return False
        return any(str(usage.Value) == target_oid for usage in enhanced_key_usages)
    return False


def _has_digital_signature_key_usage(cert: Any) -> bool:
    from System.Security.Cryptography.X509Certificates import X509KeyUsageFlags

    for extension in cert.Extensions:
        oid = getattr(getattr(extension, _ATTR_OID, None), _ATTR_VALUE, None)
        if str(oid) != _OID_KEY_USAGE:
            continue
        key_usages = getattr(extension, _ATTR_KEY_USAGES, None)
        if key_usages is None:
            return False
        return (int(key_usages) & int(X509KeyUsageFlags.DigitalSignature)) != 0
    # No KeyUsage extension present: per RFC 5280 §4.2.1.3, all usages are permitted.
    return True


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
