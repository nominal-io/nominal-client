from __future__ import annotations

from dataclasses import dataclass

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

from nominal.smartcard._errors import SmartcardCertificateSelectionError

# Slot 9A is reserved for PIV Authentication keys on the smartcard.
PIV_AUTHENTICATION_SLOT = "9A"


@dataclass(frozen=True)
class CertificateCandidate:
    """A certificate/key pair discovered on a PKCS#11 token."""

    label: str | None
    slot: str | None
    certificate_uri: str
    private_key_uri: str
    der_certificate: bytes = b""
    token_label: str = ""
    object_id_bytes: bytes | None = None

    @property
    def is_piv_authentication_candidate(self) -> bool:
        return self.slot is not None and self.slot.upper() == PIV_AUTHENTICATION_SLOT


def _assert_client_auth_eku(candidate: CertificateCandidate) -> None:
    """Raise SmartcardCertificateSelectionError if the certificate lacks clientAuth EKU.

    RFC 5280 and TLS 1.3 (RFC 8446 §4.4.2.1) require id-kp-clientAuth
    (OID 1.3.6.1.5.5.7.3.2) for certificates used in client authentication.
    A server that enforces EKU will reject a cert missing this OID; catching
    it here produces a clear diagnostic instead of a cryptic TLS handshake failure.
    """
    if not candidate.der_certificate:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} has no DER data; "
            "cannot verify ExtendedKeyUsage."
        )
    cert = x509.load_der_x509_certificate(candidate.der_certificate)
    try:
        eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
    except x509.ExtensionNotFound:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} in PIV Authentication slot "
            "has no ExtendedKeyUsage extension and cannot be used for client authentication."
        )
    if ExtendedKeyUsageOID.CLIENT_AUTH not in eku.value:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} in PIV Authentication slot "
            "does not include clientAuth (OID 1.3.6.1.5.5.7.3.2) in its ExtendedKeyUsage."
        )


def select_piv_authentication_certificate(
    candidates: list[CertificateCandidate],
) -> CertificateCandidate:
    """Select the PIV Authentication cert/key pair from discovered candidates."""
    if not candidates:
        raise SmartcardCertificateSelectionError("No certificates were found on the smartcard token.")

    piv_auth_candidates = [candidate for candidate in candidates if candidate.is_piv_authentication_candidate]
    if len(piv_auth_candidates) == 1:
        _assert_client_auth_eku(piv_auth_candidates[0])
        return piv_auth_candidates[0]

    if not piv_auth_candidates:
        raise SmartcardCertificateSelectionError(
            "Could not find a PIV Authentication certificate on the smartcard token."
        )

    labels = ", ".join(candidate.label or candidate.certificate_uri for candidate in piv_auth_candidates)
    raise SmartcardCertificateSelectionError(f"Multiple PIV Authentication certificate candidates were found: {labels}")
