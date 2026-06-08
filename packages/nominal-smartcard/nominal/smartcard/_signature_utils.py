from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from nominal.smartcard._errors import SmartcardConfigurationError


def encode_ecdsa_der(raw_sig: bytes) -> bytes:
    """Convert a raw ECDSA signature (r||s) to DER ASN.1 for gRPC/BoringSSL."""
    if len(raw_sig) == 0 or len(raw_sig) % 2 != 0:
        raise SmartcardConfigurationError(
            f"Unexpected ECDSA signature length {len(raw_sig)}; expected a non-empty even number of bytes."
        )
    half = len(raw_sig) // 2
    r = int.from_bytes(raw_sig[:half], "big")
    s = int.from_bytes(raw_sig[half:], "big")
    return encode_dss_signature(r, s)
