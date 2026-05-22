from __future__ import annotations

import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard._errors import SmartcardConfigurationError, SmartcardPinError, SmartcardPinLockedError
from nominal.smartcard._grpc_signer import (
    _ECDSA_SECP256R1_SHA256,
    _ECDSA_SECP384R1_SHA384,
    _ECDSA_SECP521R1_SHA512,
    _RSA_PKCS1_SHA256,
    _RSA_PKCS1_SHA384,
    _RSA_PKCS1_SHA512,
    _RSA_PSS_RSAE_SHA256,
    _RSA_PSS_RSAE_SHA384,
    _RSA_PSS_RSAE_SHA512,
    SmartcardPrivateKeySigner,
    _encode_ecdsa_der,
    _get_mechanism_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_signer(
    *,
    module_path: Path | None = None,
    token_label: str = "CAC",
    object_id_bytes: bytes = b"\x01",
    pin: str = "123456",
) -> SmartcardPrivateKeySigner:
    return SmartcardPrivateKeySigner(
        module_path=module_path or Path("/fake/opensc-pkcs11.so"),
        token_label=token_label,
        object_id_bytes=object_id_bytes,
        pin=pin,
    )


def _fake_pkcs11_module(
    *,
    token_label: str = "CAC",
    sign_return: bytes = b"\x00" * 64,
    pin_error: type[Exception] | None = None,
    key_error: type[Exception] | None = None,
) -> MagicMock:
    """Build a mock pkcs11 module hierarchy: lib → slot → token → session → key."""
    import pkcs11.exceptions

    key = MagicMock()
    if key_error is not None:
        key.sign.side_effect = key_error("sign failed")
    else:
        key.sign.return_value = sign_return

    session = MagicMock()
    session.get_key.return_value = key

    token = MagicMock()
    token.label = token_label
    if pin_error is not None:
        token.open.side_effect = pin_error("bad pin")
    else:
        token.open.return_value = session

    slot = MagicMock()
    slot.get_token.return_value = token

    lib = MagicMock()
    lib.get_slots.return_value = [slot]

    pkcs11_mod = MagicMock()
    pkcs11_mod.lib.return_value = lib
    pkcs11_mod.exceptions = pkcs11.exceptions
    pkcs11_mod.ObjectClass = __import__("pkcs11").ObjectClass
    return pkcs11_mod


# ---------------------------------------------------------------------------
# _encode_ecdsa_der
# ---------------------------------------------------------------------------


def test_encode_ecdsa_der_produces_valid_der() -> None:
    pytest.importorskip("cryptography")
    # 64-byte raw P-256 signature: 32-byte r, 32-byte s
    r_bytes = b"\x01" * 32
    s_bytes = b"\x02" * 32
    raw = r_bytes + s_bytes
    der = _encode_ecdsa_der(raw)
    # DER SEQUENCE must start with 0x30
    assert der[0] == 0x30
    # Round-trip through decode_dss_signature to verify correctness
    from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

    r, s = decode_dss_signature(der)
    assert r == int.from_bytes(r_bytes, "big")
    assert s == int.from_bytes(s_bytes, "big")


def test_encode_ecdsa_der_rejects_odd_length() -> None:
    with pytest.raises(SmartcardConfigurationError, match="Unexpected ECDSA signature length"):
        _encode_ecdsa_der(b"\x01" * 63)


def test_encode_ecdsa_der_rejects_empty() -> None:
    with pytest.raises(SmartcardConfigurationError, match="Unexpected ECDSA signature length"):
        _encode_ecdsa_der(b"")


# ---------------------------------------------------------------------------
# Mechanism table
# ---------------------------------------------------------------------------


def test_mechanism_table_covers_all_nine_algorithms() -> None:
    table = _get_mechanism_table()
    expected = {
        _RSA_PKCS1_SHA256,
        _RSA_PKCS1_SHA384,
        _RSA_PKCS1_SHA512,
        _RSA_PSS_RSAE_SHA256,
        _RSA_PSS_RSAE_SHA384,
        _RSA_PSS_RSAE_SHA512,
        _ECDSA_SECP256R1_SHA256,
        _ECDSA_SECP384R1_SHA384,
        _ECDSA_SECP521R1_SHA512,
    }
    assert set(table.keys()) == expected


def test_rsa_pss_sha256_has_correct_params() -> None:
    from pkcs11.mechanisms import MGF, Mechanism

    table = _get_mechanism_table()
    mech, params = table[_RSA_PSS_RSAE_SHA256]
    assert mech == Mechanism.SHA256_RSA_PKCS_PSS
    assert params == (Mechanism.SHA256, MGF.SHA256, 32)


def test_rsa_pss_sha384_has_correct_params() -> None:
    from pkcs11.mechanisms import MGF, Mechanism

    table = _get_mechanism_table()
    mech, params = table[_RSA_PSS_RSAE_SHA384]
    assert mech == Mechanism.SHA384_RSA_PKCS_PSS
    assert params == (Mechanism.SHA384, MGF.SHA384, 48)


def test_rsa_pss_sha512_has_correct_params() -> None:
    from pkcs11.mechanisms import MGF, Mechanism

    table = _get_mechanism_table()
    mech, params = table[_RSA_PSS_RSAE_SHA512]
    assert mech == Mechanism.SHA512_RSA_PKCS_PSS
    assert params == (Mechanism.SHA512, MGF.SHA512, 64)


def test_ecdsa_mechanisms_have_no_params() -> None:
    table = _get_mechanism_table()
    for algo in (_ECDSA_SECP256R1_SHA256, _ECDSA_SECP384R1_SHA384, _ECDSA_SECP521R1_SHA512):
        _, params = table[algo]
        assert params is None, f"Expected no params for ECDSA algo 0x{algo:04x}"


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner.sign — happy paths
# ---------------------------------------------------------------------------


def test_sign_rsa_pkcs1_returns_raw_bytes() -> None:
    expected_sig = b"\xab" * 256  # 2048-bit RSA signature
    pkcs11_mod = _fake_pkcs11_module(sign_return=expected_sig)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        result = signer.sign(b"data", _RSA_PKCS1_SHA256, None)

    assert result == expected_sig


def test_sign_ecdsa_secp384r1_returns_der_encoded() -> None:
    pytest.importorskip("cryptography")
    # Simulate P-384 raw r||s output: 48 bytes each
    r_bytes = b"\x11" * 48
    s_bytes = b"\x22" * 48
    raw_sig = r_bytes + s_bytes
    pkcs11_mod = _fake_pkcs11_module(sign_return=raw_sig)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        result = signer.sign(b"tls-transcript", _ECDSA_SECP384R1_SHA384, None)

    # Must be DER-encoded
    assert result[0] == 0x30
    from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

    r, s = decode_dss_signature(result)
    assert r == int.from_bytes(r_bytes, "big")
    assert s == int.from_bytes(s_bytes, "big")


def test_sign_passes_mechanism_param_to_key_sign() -> None:
    from pkcs11.mechanisms import MGF, Mechanism

    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 256)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _RSA_PSS_RSAE_SHA256, None)

    key = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value.get_key.return_value
    key.sign.assert_called_once_with(
        b"data",
        mechanism=Mechanism.SHA256_RSA_PKCS_PSS,
        mechanism_param=(Mechanism.SHA256, MGF.SHA256, 32),
    )


def test_sign_uses_correct_ecdsa_mechanism() -> None:
    from pkcs11.mechanisms import Mechanism

    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)

    key = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value.get_key.return_value
    key.sign.assert_called_once_with(b"data", mechanism=Mechanism.ECDSA_SHA256, mechanism_param=None)


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner — session lifecycle
# ---------------------------------------------------------------------------


def test_session_opened_once_across_multiple_sign_calls() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data1", _ECDSA_SECP256R1_SHA256, None)
        signer.sign(b"data2", _ECDSA_SECP256R1_SHA256, None)
        signer.sign(b"data3", _ECDSA_SECP256R1_SHA256, None)

    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    token.open.assert_called_once()


def test_pin_retained_during_session_for_recovery() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer(pin="secret")

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)

    assert signer._pin == "secret"


def test_pin_cleared_after_close() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer(pin="secret")

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)

    signer.close()
    assert signer._pin == ""


def test_session_opened_concurrently_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()
    barrier = threading.Barrier(8)
    errors: list[Exception] = []

    def call() -> None:
        try:
            barrier.wait()
            with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
                signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=call) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    token.open.assert_called_once()


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner — error paths
# ---------------------------------------------------------------------------


def test_sign_unsupported_algorithm_raises() -> None:
    signer = _make_signer()
    with pytest.raises(SmartcardConfigurationError, match="Unsupported TLS signature algorithm"):
        signer.sign(b"data", 0x9999, None)


def test_pin_incorrect_raises_smartcard_pin_error() -> None:
    import pkcs11.exceptions

    pkcs11_mod = _fake_pkcs11_module(pin_error=pkcs11.exceptions.PinIncorrect)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        with pytest.raises(SmartcardPinError, match="Incorrect PIN"):
            signer.sign(b"data", _RSA_PKCS1_SHA256, None)


def test_pin_locked_raises_smartcard_pin_locked_error() -> None:
    import pkcs11.exceptions

    pkcs11_mod = _fake_pkcs11_module(pin_error=pkcs11.exceptions.PinLocked)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        with pytest.raises(SmartcardPinLockedError, match="PIN is locked"):
            signer.sign(b"data", _RSA_PKCS1_SHA256, None)


def test_token_not_found_raises_configuration_error() -> None:
    pkcs11_mod = _fake_pkcs11_module(token_label="OTHER_TOKEN")
    signer = _make_signer(token_label="CAC")

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        with pytest.raises(SmartcardConfigurationError, match="not found"):
            signer.sign(b"data", _RSA_PKCS1_SHA256, None)


def test_pkcs11_sign_error_raises_configuration_error() -> None:
    import pkcs11.exceptions

    pkcs11_mod = _fake_pkcs11_module()
    key = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value.get_key.return_value
    key.sign.side_effect = pkcs11.exceptions.PKCS11Error("device error")
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        with pytest.raises(SmartcardConfigurationError, match="signing failed"):
            signer.sign(b"data", _RSA_PKCS1_SHA256, None)


def test_sign_invalid_algorithm_type_raises_configuration_error() -> None:
    signer = _make_signer()
    with pytest.raises(SmartcardConfigurationError, match="Invalid signature algorithm"):
        signer.sign(b"data", "not-an-int", None)


def test_session_cleared_on_device_removed_error() -> None:
    import pkcs11.exceptions

    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)
        assert signer._session is not None

        key = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value.get_key.return_value
        key.sign.side_effect = pkcs11.exceptions.DeviceRemoved("card pulled")

        with pytest.raises(SmartcardConfigurationError):
            signer.sign(b"data2", _ECDSA_SECP256R1_SHA256, None)

    assert signer._session is None
    assert signer._key is None


def test_session_re_established_after_device_removed() -> None:
    import pkcs11.exceptions

    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        # First sign succeeds, establishing a session.
        signer.sign(b"data1", _ECDSA_SECP256R1_SHA256, None)

        # Simulate card removal on the next sign.
        key = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value.get_key.return_value
        key.sign.side_effect = pkcs11.exceptions.DeviceRemoved("card pulled")
        with pytest.raises(SmartcardConfigurationError):
            signer.sign(b"data2", _ECDSA_SECP256R1_SHA256, None)

        # Card reinserted: reset the mock to succeed again.
        key.sign.side_effect = None
        key.sign.return_value = b"\x00" * 64

        # Recovery sign must succeed and re-open the session.
        result = signer.sign(b"data3", _ECDSA_SECP256R1_SHA256, None)

    assert signer._session is not None
    assert result is not None


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner.close
# ---------------------------------------------------------------------------


def test_close_releases_session() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)

    session = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value
    signer.close()
    session.close.assert_called_once()
    assert signer._session is None
    assert signer._key is None


def test_close_idempotent() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with patch.dict("sys.modules", {"pkcs11": pkcs11_mod, "pkcs11.exceptions": pkcs11_mod.exceptions}):
        signer.sign(b"data", _ECDSA_SECP256R1_SHA256, None)

    signer.close()
    signer.close()  # must not raise
