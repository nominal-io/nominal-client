from __future__ import annotations

from dataclasses import dataclass

from nominal.smartcard._errors import SmartcardCertificateSelectionError

PIV_AUTHENTICATION_SLOT = "9A"


@dataclass(frozen=True)
class CertificateCandidate:
    """A certificate/key pair discovered on a PKCS#11 token."""

    label: str | None
    slot: str | None
    pkcs11_uri: str
    der_certificate: bytes = b""

    @property
    def is_piv_authentication_candidate(self) -> bool:
        return self.slot is not None and self.slot.upper() == PIV_AUTHENTICATION_SLOT


def select_piv_authentication_certificate(
    candidates: list[CertificateCandidate],
) -> CertificateCandidate:
    """Select the CAC PIV Authentication cert/key pair from discovered candidates."""
    if not candidates:
        raise SmartcardCertificateSelectionError("No certificates were found on the smartcard token.")

    piv_auth_candidates = [candidate for candidate in candidates if candidate.is_piv_authentication_candidate]
    if len(piv_auth_candidates) == 1:
        return piv_auth_candidates[0]

    if not piv_auth_candidates:
        raise SmartcardCertificateSelectionError(
            "Could not find a PIV Authentication certificate on the smartcard token. "
            "Do not use the Digital Signature, Key Management, or PIV Card Authentication certificate."
        )

    labels = ", ".join(candidate.label or candidate.pkcs11_uri for candidate in piv_auth_candidates)
    raise SmartcardCertificateSelectionError(f"Multiple PIV Authentication certificate candidates were found: {labels}")
