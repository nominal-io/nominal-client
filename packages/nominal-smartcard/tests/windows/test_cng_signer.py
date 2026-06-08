from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard._errors import SmartcardConfigurationError, SmartcardRuntimeError
from nominal.smartcard.windows._cert_store import WindowsCertificateIdentity
from nominal.smartcard.windows._cng_signer import _OID_RSA, WindowsCngSigner, _Algorithm, _sign_ecdsa, _sign_rsa

# ---------------------------------------------------------------------------
# _sign_rsa
# ---------------------------------------------------------------------------


_RSA_CASES = [
    (_Algorithm.RSA_PKCS1_SHA256, "SHA256", "Pkcs1"),
    (_Algorithm.RSA_PKCS1_SHA384, "SHA384", "Pkcs1"),
    (_Algorithm.RSA_PKCS1_SHA512, "SHA512", "Pkcs1"),
    (_Algorithm.RSA_PSS_RSAE_SHA256, "SHA256", "Pss"),
    (_Algorithm.RSA_PSS_RSAE_SHA384, "SHA384", "Pss"),
    (_Algorithm.RSA_PSS_RSAE_SHA512, "SHA512", "Pss"),
]


@pytest.mark.parametrize(("algorithm", "hash_name", "padding_name"), _RSA_CASES)
def test_sign_rsa_selects_expected_hash_and_padding(algorithm: object, hash_name: str, padding_name: str) -> None:
    key = MagicMock(name="rsa_key")
    key.SignData.return_value = b"raw-rsa-signature"
    hash_algorithm_name = MagicMock(name="HashAlgorithmName")
    rsa_padding = MagicMock(name="RSASignaturePadding")
    data = MagicMock(name="data_net")

    result = _sign_rsa(key, data, algorithm, hash_algorithm_name, rsa_padding)

    key.SignData.assert_called_once_with(
        data, getattr(hash_algorithm_name, hash_name), getattr(rsa_padding, padding_name)
    )
    # RSA signatures are returned as-is (already in the wire format gRPC expects).
    assert result == b"raw-rsa-signature"


def test_sign_rsa_rejects_ecdsa_algorithm() -> None:
    with pytest.raises(SmartcardConfigurationError, match="not an RSA algorithm"):
        _sign_rsa(MagicMock(), MagicMock(), _Algorithm.ECDSA_SECP256R1_SHA256, MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# _sign_ecdsa
# ---------------------------------------------------------------------------


_ECDSA_CASES = [
    (_Algorithm.ECDSA_SECP256R1_SHA256, "SHA256"),
    (_Algorithm.ECDSA_SECP384R1_SHA384, "SHA384"),
    (_Algorithm.ECDSA_SECP521R1_SHA512, "SHA512"),
]


@pytest.mark.parametrize(("algorithm", "hash_name"), _ECDSA_CASES)
def test_sign_ecdsa_selects_expected_hash_and_der_encodes(algorithm: object, hash_name: str) -> None:
    key = MagicMock(name="ecdsa_key")
    raw_signature = b"\x01" * 64  # raw r||s (equal halves) as CNG returns it
    key.SignData.return_value = raw_signature
    hash_algorithm_name = MagicMock(name="HashAlgorithmName")
    data = MagicMock(name="data_net")

    result = _sign_ecdsa(key, data, algorithm, hash_algorithm_name)

    key.SignData.assert_called_once_with(data, getattr(hash_algorithm_name, hash_name))
    # ECDSA signatures must be DER-encoded for gRPC/BoringSSL, not returned as raw r||s.
    assert result[0] == 0x30
    assert result != raw_signature


def test_sign_ecdsa_rejects_rsa_algorithm() -> None:
    with pytest.raises(SmartcardConfigurationError, match="not an ECDSA algorithm"):
        _sign_ecdsa(MagicMock(), MagicMock(), _Algorithm.RSA_PKCS1_SHA256, MagicMock())


# ---------------------------------------------------------------------------
# WindowsCngSigner public surface (connect / sign / close)
# ---------------------------------------------------------------------------

_SIGN = "nominal.smartcard.windows._cng_signer._sign_with_cert"
_WARMUP = "nominal.smartcard.windows._cng_signer._warmup_sign"


def _make_signer(public_key_oid: str = _OID_RSA) -> tuple[WindowsCngSigner, WindowsCertificateIdentity]:
    identity = WindowsCertificateIdentity(
        certificate=MagicMock(name="certificate"),
        der_certificate=b"DER-BYTES",
        thumbprint="AABBCC",
        subject="CN=PIV Auth",
        issuer="CN=DoD CA",
        not_after="2099-01-01",
        public_key_oid=public_key_oid,
    )
    return WindowsCngSigner(identity=identity), identity


def test_sign_before_connect_raises() -> None:
    signer, _ = _make_signer()
    with pytest.raises(SmartcardRuntimeError, match=r"connect\(\) has not been called"):
        signer.sign(b"data", _Algorithm.RSA_PKCS1_SHA256, None)


def test_connect_warms_up_once_and_is_idempotent() -> None:
    signer, identity = _make_signer()
    with patch(_WARMUP) as warmup:
        signer.connect()
        signer.connect()
    warmup.assert_called_once_with(identity.certificate, identity.public_key_oid)


def test_sign_returns_signature_when_connected() -> None:
    signer, identity = _make_signer()
    with patch(_WARMUP), patch(_SIGN, return_value=b"signature") as sign_with_cert:
        signer.connect()
        result = signer.sign(b"data", _Algorithm.RSA_PKCS1_SHA256, None)
    assert result == b"signature"
    sign_with_cert.assert_called_once_with(
        identity.certificate, identity.public_key_oid, b"data", _Algorithm.RSA_PKCS1_SHA256
    )


def test_sign_wraps_generic_exception_and_names_algorithm() -> None:
    signer, _ = _make_signer()
    with patch(_WARMUP), patch(_SIGN, side_effect=RuntimeError("boom")):
        signer.connect()
        with pytest.raises(SmartcardConfigurationError, match="Windows CNG signing failed") as exc_info:
            signer.sign(b"data", _Algorithm.ECDSA_SECP256R1_SHA256, None)
    assert "ECDSA_SECP256R1_SHA256" in str(exc_info.value)


def test_sign_passes_through_smartcard_configuration_error() -> None:
    signer, _ = _make_signer()
    original = SmartcardConfigurationError("a specific cause")
    with patch(_WARMUP), patch(_SIGN, side_effect=original):
        signer.connect()
        with pytest.raises(SmartcardConfigurationError) as exc_info:
            signer.sign(b"data", _Algorithm.RSA_PKCS1_SHA256, None)
    # The original error must propagate unchanged, not be re-wrapped.
    assert exc_info.value is original


def test_close_resets_connected_so_later_sign_raises() -> None:
    signer, _ = _make_signer()
    with patch(_WARMUP), patch(_SIGN, return_value=b"signature"):
        signer.connect()
        assert signer.sign(b"d", _Algorithm.RSA_PKCS1_SHA256, None) == b"signature"
        signer.close()
        with pytest.raises(SmartcardRuntimeError, match=r"connect\(\) has not been called"):
            signer.sign(b"d", _Algorithm.RSA_PKCS1_SHA256, None)


def test_der_certificate_exposes_identity_bytes() -> None:
    signer, identity = _make_signer()
    assert signer.der_certificate == identity.der_certificate
