"""Unit tests for the cffi bridge that backs smartcard / CAC TLS.

These tests exercise the control flow with a fake handle injected — they do NOT call into real
libcrypto. End-to-end verification against a real CAC is covered manually; here we just make sure the
orchestration is correct and the bridge talks to PyKCS11 in the expected sequence.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from nominal.core._utils import _pkcs11_bridge as bridge


class _FakeFFI:
    """Minimal stand-in for cffi.FFI that mirrors NULL / cdata semantics for our helpers."""

    NULL = object()  # sentinel; equality with itself only

    def new(self, _spec: str) -> Any:
        buf = MagicMock()
        buf.value = b""
        return buf

    def string(self, buf: Any) -> bytes:
        return buf.value

    def cast(self, _type: str, val: Any) -> Any:
        return val


def _fake_handle() -> bridge._LibHandle:
    h = bridge._LibHandle(MagicMock(name="libcrypto"), MagicMock(name="libssl"))
    # Override the FFI on the per-instance handle without mutating the module-level singleton.
    h.ffi = _FakeFFI()  # type: ignore[assignment]
    return h


@pytest.fixture(autouse=True)
def _reset_module_state() -> None:
    bridge.reset_for_test()
    yield
    bridge.reset_for_test()


@pytest.fixture(scope="module")
def _rsa_cert_der() -> bytes:
    """Generate a small self-signed RSA cert once per test run."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.DER)


def _stub_method_registration_calls(handle: bridge._LibHandle) -> None:
    """Make the libcrypto stubs return success for the method-registration sequence: find default,
    new, copy, set_sign, add0.
    """
    handle.libcrypto.EVP_PKEY_meth_find.return_value = object()
    handle.libcrypto.EVP_PKEY_meth_new.return_value = object()
    handle.libcrypto.EVP_PKEY_meth_add0.return_value = 1


def _fake_pyopenssl_cert(handle: bridge._LibHandle, address: int = 0xDEADBEEF) -> MagicMock:
    """A stand-in for pyOpenSSL's `X509` wrapper. The bridge round-trips `_x509` through `_ffi.cast` to
    an integer address, so we feed it an int and let the fake FFI pass it through unchanged.
    """
    cert = MagicMock()
    cert._ffi = handle.ffi
    cert._x509 = address
    return cert


def test_drain_errors_returns_placeholder_when_queue_empty() -> None:
    handle = _fake_handle()
    handle.libcrypto.ERR_get_error.return_value = 0

    assert bridge.drain_errors(handle) == "(no OpenSSL error queued)"


def test_drain_errors_concatenates_queued_messages() -> None:
    handle = _fake_handle()
    codes = iter([0x1, 0x2, 0])
    handle.libcrypto.ERR_get_error.side_effect = lambda: next(codes)

    msgs = iter([b"first", b"second"])

    def _fill(_code: int, buf: Any, _len: int) -> None:
        buf.value = next(msgs)

    handle.libcrypto.ERR_error_string_n.side_effect = _fill

    assert bridge.drain_errors(handle) == "first; second"


def test_install_pkcs11_key_creates_pkey_and_records_active_state(_rsa_cert_der: bytes) -> None:
    """install_pkcs11_key must: register method (copy default + override sign), build RSA from cert
    pubkey, set active state.
    """
    handle = _fake_handle()
    _stub_method_registration_calls(handle)
    handle.libcrypto.BN_bin2bn.side_effect = lambda *_args: object()
    handle.libcrypto.RSA_new.return_value = object()
    handle.libcrypto.RSA_set0_key.return_value = 1
    pkey_sentinel = object()
    handle.libcrypto.EVP_PKEY_new.return_value = pkey_sentinel
    handle.libcrypto.EVP_PKEY_set1_RSA.return_value = 1

    sign_cb = MagicMock(return_value=b"sig")
    pkey = bridge.install_pkcs11_key(handle, sign_cb, _rsa_cert_der)

    assert pkey is pkey_sentinel
    assert bridge._sign_callable is sign_cb
    # 2048-bit modulus → 256-byte signature.
    assert bridge._active_sig_len == 256
    # set1 took its own reference; our RSA ref was freed.
    handle.libcrypto.RSA_free.assert_called_once()
    # Default RSA method was copied in (so non-sign ops still work in the process).
    handle.libcrypto.EVP_PKEY_meth_copy.assert_called_once_with(
        handle.libcrypto.EVP_PKEY_meth_new.return_value,
        handle.libcrypto.EVP_PKEY_meth_find.return_value,
    )
    handle.libcrypto.EVP_PKEY_meth_set_sign.assert_called_once()


def test_install_pkcs11_key_registers_method_only_once(_rsa_cert_der: bytes) -> None:
    """A second install must not re-register the EVP_PKEY_METHOD — global registry is permanent."""
    handle = _fake_handle()
    _stub_method_registration_calls(handle)
    handle.libcrypto.BN_bin2bn.side_effect = lambda *_args: object()
    handle.libcrypto.RSA_new.return_value = object()
    handle.libcrypto.RSA_set0_key.return_value = 1
    handle.libcrypto.EVP_PKEY_new.return_value = object()
    handle.libcrypto.EVP_PKEY_set1_RSA.return_value = 1

    bridge.install_pkcs11_key(handle, MagicMock(), _rsa_cert_der)
    bridge.install_pkcs11_key(handle, MagicMock(), _rsa_cert_der)

    assert handle.libcrypto.EVP_PKEY_meth_new.call_count == 1
    assert handle.libcrypto.EVP_PKEY_meth_add0.call_count == 1
    assert handle.libcrypto.EVP_PKEY_meth_copy.call_count == 1


def test_install_pkcs11_key_raises_when_meth_find_returns_null(_rsa_cert_der: bytes) -> None:
    handle = _fake_handle()
    handle.libcrypto.EVP_PKEY_meth_find.return_value = handle.ffi.NULL
    handle.libcrypto.ERR_get_error.return_value = 0

    with pytest.raises(bridge.PKCS11BridgeError, match="EVP_PKEY_meth_find"):
        bridge.install_pkcs11_key(handle, MagicMock(), _rsa_cert_der)


def test_install_pkcs11_key_raises_when_method_new_returns_null(_rsa_cert_der: bytes) -> None:
    handle = _fake_handle()
    handle.libcrypto.EVP_PKEY_meth_find.return_value = object()
    handle.libcrypto.EVP_PKEY_meth_new.return_value = handle.ffi.NULL
    handle.libcrypto.ERR_get_error.return_value = 0

    with pytest.raises(bridge.PKCS11BridgeError, match="EVP_PKEY_meth_new"):
        bridge.install_pkcs11_key(handle, MagicMock(), _rsa_cert_der)


def test_install_on_ssl_context_happy_path_frees_only_pkey() -> None:
    """SSL_CTX_use_certificate increments cert refcount (pyOpenSSL still owns it); only pkey is freed.
    We do NOT call SSL_CTX_check_private_key — our EVP_PKEY has no private material to derive a public
    component from, so that check would unconditionally fail.
    """
    handle = _fake_handle()
    handle.libssl.SSL_CTX_use_certificate.return_value = 1
    handle.libssl.SSL_CTX_use_PrivateKey.return_value = 1

    cert = _fake_pyopenssl_cert(handle)

    bridge.install_on_ssl_context(handle, "<ssl_ctx>", cert, "<pkey>")

    handle.libssl.SSL_CTX_use_certificate.assert_called_once_with("<ssl_ctx>", 0xDEADBEEF)
    handle.libssl.SSL_CTX_use_PrivateKey.assert_called_once_with("<ssl_ctx>", "<pkey>")
    handle.libssl.SSL_CTX_check_private_key.assert_not_called()
    handle.libcrypto.EVP_PKEY_free.assert_called_once_with("<pkey>")
    handle.libcrypto.X509_free.assert_not_called()


def test_install_on_ssl_context_surfaces_use_certificate_error() -> None:
    handle = _fake_handle()
    handle.libssl.SSL_CTX_use_certificate.return_value = 0
    handle.libcrypto.ERR_get_error.return_value = 0

    cert = _fake_pyopenssl_cert(handle)

    with pytest.raises(bridge.PKCS11BridgeError, match="SSL_CTX_use_certificate"):
        bridge.install_on_ssl_context(handle, "<ssl_ctx>", cert, "<pkey>")

    # Still drops our pkey reference.
    handle.libcrypto.EVP_PKEY_free.assert_called_once_with("<pkey>")


def test_install_on_ssl_context_surfaces_use_private_key_error() -> None:
    handle = _fake_handle()
    handle.libssl.SSL_CTX_use_certificate.return_value = 1
    handle.libssl.SSL_CTX_use_PrivateKey.return_value = 0
    handle.libcrypto.ERR_get_error.return_value = 0

    cert = _fake_pyopenssl_cert(handle)

    with pytest.raises(bridge.PKCS11BridgeError, match="SSL_CTX_use_PrivateKey"):
        bridge.install_on_ssl_context(handle, "<ssl_ctx>", cert, "<pkey>")

    handle.libcrypto.EVP_PKEY_free.assert_called_once_with("<pkey>")
