from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import grpc.experimental
import pkcs11.exceptions
import pytest
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from pkcs11.mechanisms import MGF, Mechanism

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._grpc_signer import (
    MAX_PIN_ATTEMPTS,
    SmartcardPrivateKeySigner,
    _encode_ecdsa_der,
    _pin_prompt,
)

pytest.importorskip("pkcs11")
pytest.importorskip("cryptography")

_A = grpc.experimental.PrivateKeySignatureAlgorithm


@pytest.fixture(autouse=True)
def _default_pin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch _prompt_for_pin so tests never block on getpass."""
    monkeypatch.setattr("nominal.smartcard._grpc_signer._prompt_for_pin", lambda _: "123456")


@contextmanager
def _patch_pkcs11(pkcs11_mod: MagicMock) -> Iterator[None]:
    with patch("nominal.smartcard._grpc_signer.pkcs11", pkcs11_mod):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_signer(
    *,
    module_path: Path | None = None,
    token_label: str = "CAC",
    object_id_bytes: bytes = b"\x01",
) -> SmartcardPrivateKeySigner:
    return SmartcardPrivateKeySigner(
        module_path=module_path or Path("/fake/opensc-pkcs11.so"),
        token_label=token_label,
        object_id_bytes=object_id_bytes,
    )


def _fake_pkcs11_module(
    *,
    token_label: str = "CAC",
    sign_return: bytes = b"\x00" * 64,
    pin_error: type[Exception] | None = None,
    key_error: type[Exception] | None = None,
) -> MagicMock:
    """Build a mock pkcs11 module hierarchy: lib → slot → token → session → key."""
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
    # 64-byte raw P-256 signature: 32-byte r, 32-byte s
    r_bytes = b"\x01" * 32
    s_bytes = b"\x02" * 32
    raw = r_bytes + s_bytes
    der = _encode_ecdsa_der(raw)
    # DER SEQUENCE must start with 0x30
    assert der[0] == 0x30
    # Round-trip through decode_dss_signature to verify correctness
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
# SmartcardPrivateKeySigner.sign
# ---------------------------------------------------------------------------


def test_sign_rsa_pkcs1_returns_raw_bytes() -> None:
    expected_sig = b"\xab" * 256  # 2048-bit RSA signature
    pkcs11_mod = _fake_pkcs11_module(sign_return=expected_sig)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        result = signer.sign(b"data", _A.RSA_PKCS1_SHA256, None)

    assert result == expected_sig


def test_sign_ecdsa_secp384r1_returns_der_encoded() -> None:
    # Simulate P-384 raw r||s output: 48 bytes each
    r_bytes = b"\x11" * 48
    s_bytes = b"\x22" * 48
    raw_sig = r_bytes + s_bytes
    pkcs11_mod = _fake_pkcs11_module(sign_return=raw_sig)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        result = signer.sign(b"tls-transcript", _A.ECDSA_SECP384R1_SHA384, None)

    # Must be DER-encoded
    assert result[0] == 0x30
    r, s = decode_dss_signature(result)
    assert r == int.from_bytes(r_bytes, "big")
    assert s == int.from_bytes(s_bytes, "big")


def test_sign_passes_mechanism_param_to_key_sign() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 256)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.sign(b"data", _A.RSA_PSS_RSAE_SHA256, None)

    key = pkcs11_mod.lib.return_value.get_slots.return_value[
        0
    ].get_token.return_value.open.return_value.get_key.return_value
    key.sign.assert_called_once_with(
        b"data",
        mechanism=Mechanism.SHA256_RSA_PKCS_PSS,
        mechanism_param=(Mechanism.SHA256, MGF.SHA256, 32),
    )


def test_sign_uses_correct_ecdsa_mechanism() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.sign(b"data", _A.ECDSA_SECP256R1_SHA256, None)

    key = pkcs11_mod.lib.return_value.get_slots.return_value[
        0
    ].get_token.return_value.open.return_value.get_key.return_value
    key.sign.assert_called_once_with(b"data", mechanism=Mechanism.ECDSA_SHA256, mechanism_param=None)


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner — session lifecycle
# ---------------------------------------------------------------------------


def test_session_opened_once_across_multiple_sign_calls() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.sign(b"data1", _A.ECDSA_SECP256R1_SHA256, None)
        signer.sign(b"data2", _A.ECDSA_SECP256R1_SHA256, None)
        signer.sign(b"data3", _A.ECDSA_SECP256R1_SHA256, None)

    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    token.open.assert_called_once()


def test_pin_prompted_in_connect_not_in_sign() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    prompt_calls: list[str] = []

    def counting_pin(prompt: str) -> str:
        prompt_calls.append(prompt)
        return "123456"

    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod), patch("nominal.smartcard._grpc_signer._prompt_for_pin", counting_pin):
        signer.connect()  # 1 prompt — session established
        signer.sign(b"data1", _A.ECDSA_SECP256R1_SHA256, None)  # uses cached session
        signer.sign(b"data2", _A.ECDSA_SECP256R1_SHA256, None)  # uses cached session

    assert len(prompt_calls) == 1


def test_session_opened_concurrently_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()
    barrier = threading.Barrier(8)
    errors: list[Exception] = []

    def call() -> None:
        try:
            barrier.wait()
            with _patch_pkcs11(pkcs11_mod):
                signer.sign(b"data", _A.ECDSA_SECP256R1_SHA256, None)
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


def test_pin_incorrect_exhausts_all_attempts_then_exits() -> None:
    pkcs11_mod = _fake_pkcs11_module(pin_error=pkcs11.exceptions.PinIncorrect)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SystemExit, match="No attempts remaining"):
            signer.connect()

    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    assert token.open.call_count == MAX_PIN_ATTEMPTS


def test_pin_incorrect_once_then_correct_succeeds() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 256)
    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    session = token.open.return_value
    token.open.side_effect = [pkcs11.exceptions.PinIncorrect("bad"), session]
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.connect()

    assert token.open.call_count == 2


def test_pin_locked_exits_immediately() -> None:
    pkcs11_mod = _fake_pkcs11_module(pin_error=pkcs11.exceptions.PinLocked)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SystemExit, match="locked"):
            signer.connect()

    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    assert token.open.call_count == 1


def test_pin_len_range_exhausts_all_attempts_then_exits() -> None:
    pkcs11_mod = _fake_pkcs11_module(pin_error=pkcs11.exceptions.PinLenRange)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SystemExit, match="Incorrect PIN. No attempts remaining"):
            signer.connect()

    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    assert token.open.call_count == MAX_PIN_ATTEMPTS


def test_pin_len_range_then_correct_succeeds() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 256)
    token = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value
    session = token.open.return_value
    token.open.side_effect = [pkcs11.exceptions.PinLenRange("too short"), session]
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.connect()

    assert token.open.call_count == 2


def test_token_not_found_raises_configuration_error() -> None:
    pkcs11_mod = _fake_pkcs11_module(token_label="OTHER_TOKEN")
    signer = _make_signer(token_label="CAC")

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SmartcardConfigurationError, match="not found"):
            signer.sign(b"data", _A.RSA_PKCS1_SHA256, None)


def test_pkcs11_sign_error_raises_configuration_error() -> None:
    pkcs11_mod = _fake_pkcs11_module()
    key = pkcs11_mod.lib.return_value.get_slots.return_value[
        0
    ].get_token.return_value.open.return_value.get_key.return_value
    key.sign.side_effect = pkcs11.exceptions.PKCS11Error("device error")
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SmartcardConfigurationError, match="signing failed"):
            signer.sign(b"data", _A.RSA_PKCS1_SHA256, None)


def test_pin_incorrect_from_sign_raises_configuration_error() -> None:
    """Tokens that defer PIN verification to C_Sign surface as a signing error.
    sign() does not retry — connect() must have been called beforehand.
    """
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    key = pkcs11_mod.lib.return_value.get_slots.return_value[
        0
    ].get_token.return_value.open.return_value.get_key.return_value
    key.sign.side_effect = pkcs11.exceptions.PinIncorrect("deferred pin check")
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        with pytest.raises(SmartcardConfigurationError, match="signing failed"):
            signer.sign(b"data", _A.RSA_PKCS1_SHA256, None)


# ---------------------------------------------------------------------------
# SmartcardPrivateKeySigner.close
# ---------------------------------------------------------------------------


def test_close_releases_session() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.sign(b"data", _A.ECDSA_SECP256R1_SHA256, None)

    session = pkcs11_mod.lib.return_value.get_slots.return_value[0].get_token.return_value.open.return_value
    signer.close()
    session.close.assert_called_once()
    assert signer._session is None
    assert signer._key is None


def test_close_idempotent() -> None:
    pkcs11_mod = _fake_pkcs11_module(sign_return=b"\x00" * 64)
    signer = _make_signer()

    with _patch_pkcs11(pkcs11_mod):
        signer.sign(b"data", _A.ECDSA_SECP256R1_SHA256, None)

    signer.close()
    signer.close()  # must not raise


# ---------------------------------------------------------------------------
# _pin_prompt
# ---------------------------------------------------------------------------


def test_pin_prompt_includes_slot_id_and_description() -> None:
    token = MagicMock()
    token.slot.slot_id = 0
    token.slot.slot_description = "Yubico YubiKey OTP+FIDO+CCID"

    assert _pin_prompt(token, "CAC") == "Enter PIN for 'CAC' (Slot 0 - Yubico YubiKey OTP+FIDO+CCID): "


def test_pin_prompt_omits_description_when_blank() -> None:
    token = MagicMock()
    token.slot.slot_id = 3
    token.slot.slot_description = "   "

    assert _pin_prompt(token, "CAC") == "Enter PIN for 'CAC' (Slot 3): "


def test_pin_prompt_falls_back_to_label_when_slot_unavailable() -> None:
    token = MagicMock()
    type(token).slot = property(lambda _: (_ for _ in ()).throw(pkcs11.exceptions.PKCS11Error("no slot")))

    assert _pin_prompt(token, "CAC") == "Enter PIN for 'CAC': "
