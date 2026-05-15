from __future__ import annotations

from pathlib import Path

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard._pkcs11 import Pkcs11Backend

FAKE_DER = b"\x30\x82\x01\x00"  # minimal DER stub


def _candidate(
    *,
    label: str = "PIV Authentication",
    slot: str | None = "9A",
    pkcs11_uri: str = "pkcs11:token=CAC;id=%01",
    der_certificate: bytes = FAKE_DER,
) -> CertificateCandidate:
    return CertificateCandidate(
        label=label,
        slot=slot,
        pkcs11_uri=pkcs11_uri,
        der_certificate=der_certificate,
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
