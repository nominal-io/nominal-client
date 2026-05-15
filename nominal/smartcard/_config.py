from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SmartcardConfig:
    """Local smartcard TLS configuration.

    Machine-local details are discovered from environment variables or platform defaults unless explicitly provided.
    """

    pkcs11_module_path: Path | None = None
    openssl_provider_path: Path | None = None
    pin_prompt: str = "CAC PIN: "
