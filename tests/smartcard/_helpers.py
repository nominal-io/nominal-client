from __future__ import annotations

from pathlib import Path

from nominal.smartcard._cert_selection import CertificateCandidate
from nominal.smartcard._pkcs11 import Pkcs11Backend


def _candidate(
    *,
    label: str = "PIV Authentication",
    slot: str | None = "9A",
    pkcs11_uri: str = "pkcs11:token=CAC;id=%01",
) -> CertificateCandidate:
    return CertificateCandidate(
        label=label,
        slot=slot,
        pkcs11_uri=pkcs11_uri,
    )


class _FakeBackend(Pkcs11Backend):
    def __init__(
        self,
        module_path: Path,
        candidates: list[CertificateCandidate],
        *,
        pin_error: Exception | None = None,
    ) -> None:
        super().__init__(module_path)
        self._candidates = candidates
        self._pin_error = pin_error
        self.login_calls: list[tuple[CertificateCandidate, str]] = []
        self.close_calls = 0

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        return self._candidates

    def login(self, certificate: CertificateCandidate, pin: str) -> None:
        self.login_calls.append((certificate, pin))
        if self._pin_error is not None:
            raise self._pin_error

    def close(self) -> None:
        self.close_calls += 1
