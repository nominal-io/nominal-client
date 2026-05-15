from __future__ import annotations

from dataclasses import dataclass

from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard.errors import SmartcardCertificateSelectionError

PIV_AUTHENTICATION_SLOT = "9A"


@dataclass(frozen=True)
class CertificateCandidate:
    """A certificate/key pair discovered on a PKCS#11 token."""

    label: str | None
    token_label: str | None
    slot: str | None
    object_id: str | None
    sha256_fingerprint: str
    pkcs11_uri: str
    der_certificate: bytes
    extended_key_usages: tuple[str, ...] = ()

    @property
    def normalized_fingerprint(self) -> str:
        return normalize_fingerprint(self.sha256_fingerprint)

    @property
    def is_piv_authentication_candidate(self) -> bool:
        return self.slot is not None and self.slot.upper() == PIV_AUTHENTICATION_SLOT


def normalize_fingerprint(fingerprint: str) -> str:
    return fingerprint.replace(":", "").replace(" ", "").lower()


def select_piv_authentication_certificate(
    candidates: list[CertificateCandidate],
    config: SmartcardConfig,
) -> CertificateCandidate:
    """Select the CAC PIV Authentication cert/key pair from discovered candidates."""
    if not candidates:
        raise SmartcardCertificateSelectionError("No certificates were found on the smartcard token.")

    filtered = candidates
    if config.token_label is not None:
        filtered = [candidate for candidate in filtered if candidate.token_label == config.token_label]
        if not filtered:
            raise SmartcardCertificateSelectionError(f"No certificates were found on token {config.token_label!r}.")

    if config.certificate_fingerprint is not None:
        fingerprint = normalize_fingerprint(config.certificate_fingerprint)
        matches = [candidate for candidate in filtered if candidate.normalized_fingerprint == fingerprint]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise SmartcardCertificateSelectionError(
                f"No smartcard certificate matched fingerprint {config.certificate_fingerprint!r}."
            )
        raise SmartcardCertificateSelectionError(
            f"Multiple smartcard certificates matched fingerprint {config.certificate_fingerprint!r}."
        )

    piv_auth_candidates = [candidate for candidate in filtered if candidate.is_piv_authentication_candidate]
    if len(piv_auth_candidates) == 1:
        return piv_auth_candidates[0]
    if not piv_auth_candidates:
        raise SmartcardCertificateSelectionError(
            "Could not find a PIV Authentication certificate on the smartcard token. "
            "Do not use the Digital Signature, Key Management, or PIV Card Authentication certificate."
        )
    labels = ", ".join(candidate.label or candidate.pkcs11_uri for candidate in piv_auth_candidates)
    raise SmartcardCertificateSelectionError(
        "Multiple PIV Authentication certificate candidates were found. "
        f"Configure a certificate fingerprint to choose one: {labels}"
    )
