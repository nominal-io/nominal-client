from __future__ import annotations

import pytest
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._signature_utils import encode_ecdsa_der

# ---------------------------------------------------------------------------
# encode_ecdsa_der
# ---------------------------------------------------------------------------


def test_encode_ecdsa_der_produces_valid_der() -> None:
    # 64-byte raw P-256 signature: 32-byte r, 32-byte s
    r_bytes = b"\x01" * 32
    s_bytes = b"\x02" * 32
    raw = r_bytes + s_bytes
    der = encode_ecdsa_der(raw)
    # DER SEQUENCE must start with 0x30
    assert der[0] == 0x30
    # Round-trip through decode_dss_signature to verify correctness
    r, s = decode_dss_signature(der)
    assert r == int.from_bytes(r_bytes, "big")
    assert s == int.from_bytes(s_bytes, "big")


def test_encode_ecdsa_der_rejects_odd_length() -> None:
    with pytest.raises(SmartcardConfigurationError, match="Unexpected ECDSA signature length"):
        encode_ecdsa_der(b"\x01" * 63)


def test_encode_ecdsa_der_rejects_empty() -> None:
    with pytest.raises(SmartcardConfigurationError, match="Unexpected ECDSA signature length"):
        encode_ecdsa_der(b"")
