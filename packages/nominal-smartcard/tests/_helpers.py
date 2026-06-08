from __future__ import annotations

import datetime
from pathlib import Path

from nominal.smartcard.pkcs11._cert_selection import CertificateCandidate
from nominal.smartcard.pkcs11._discovery import Pkcs11Backend

# Minimal DER stub — not a valid X.509 certificate.
# Use _make_der_cert() when the test needs a parseable cert (e.g. EKU checks).
FAKE_DER = b"\x30\x82\x01\x00"


def _make_der_cert(*, client_auth_eku: bool = True, has_eku_extension: bool = True) -> bytes:
    """Generate a minimal self-signed DER certificate for testing.

    Requires the 'cryptography' package (part of the smartcard extras).
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric.ec import SECP256R1, generate_private_key
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = generate_private_key(SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test PIV Auth")])
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
    )
    if has_eku_extension:
        usages = [ExtendedKeyUsageOID.CLIENT_AUTH] if client_auth_eku else [ExtendedKeyUsageOID.EMAIL_PROTECTION]
        builder = builder.add_extension(x509.ExtendedKeyUsage(usages), critical=False)
    cert = builder.sign(key, hashes.SHA256())
    return cert.public_bytes(serialization.Encoding.DER)


def _candidate(
    *,
    label: str = "PIV Authentication",
    slot: str | None = "9A",
    certificate_uri: str = "pkcs11:token=CAC;id=%01;type=cert",
    private_key_uri: str = "pkcs11:token=CAC;id=%01;type=private",
    der_certificate: bytes = FAKE_DER,
    token_label: str = "CAC",
    object_id_bytes: bytes | None = b"\x01",
) -> CertificateCandidate:
    return CertificateCandidate(
        label=label,
        slot=slot,
        certificate_uri=certificate_uri,
        private_key_uri=private_key_uri,
        der_certificate=der_certificate,
        token_label=token_label,
        object_id_bytes=object_id_bytes,
    )


class _FakeBackend(Pkcs11Backend):
    def __init__(
        self,
        module_path: Path,
        candidates: list[CertificateCandidate],
    ) -> None:
        super().__init__(module_path)
        self._candidates = candidates
        self.close_calls = 0

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        return self._candidates

    def close(self) -> None:
        self.close_calls += 1
