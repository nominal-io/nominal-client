"""Unit tests for the cffi wrapper around OpenSSL's provider + store APIs.

These tests exercise control flow with a fake handle injected — they do NOT call into real libcrypto.
End-to-end exercise against a real CAC is covered manually; here we just make sure the orchestration is
correct.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core._utils import _openssl_provider as op


class _FakeFFI:
    """Minimal stand-in for cffi.FFI that behaves like NULL-vs-cdata for the provider helpers."""

    NULL = object()  # sentinel; equality with itself only

    def new(self, _spec):
        # Return a tiny object with a writeable .value attribute (ERR_error_string_n fills it).
        buf = MagicMock()
        buf.value = b""
        return buf

    def string(self, buf):
        return buf.value

    def cast(self, _type, val):
        return val


def _fake_handle() -> op._LibHandle:
    return op._LibHandle(_FakeFFI(), MagicMock(name="libcrypto"), MagicMock(name="libssl"))


@pytest.fixture(autouse=True)
def _reset_module_state() -> None:
    op.reset_for_test()
    op._loaded_providers.clear()
    yield
    op.reset_for_test()
    op._loaded_providers.clear()


def test_drain_errors_returns_placeholder_when_queue_empty() -> None:
    handle = _fake_handle()
    handle.libcrypto.ERR_get_error.return_value = 0

    assert op.drain_errors(handle) == "(no OpenSSL error queued)"


def test_drain_errors_concatenates_queued_messages() -> None:
    handle = _fake_handle()
    codes = iter([0x1, 0x2, 0])
    handle.libcrypto.ERR_get_error.side_effect = lambda: next(codes)

    msgs = iter([b"first", b"second"])

    def _fill(_code, buf, _len):
        buf.value = next(msgs)

    handle.libcrypto.ERR_error_string_n.side_effect = _fill

    assert op.drain_errors(handle) == "first; second"


def test_load_provider_raises_with_diagnostic_on_null_return() -> None:
    handle = _fake_handle()
    handle.libcrypto.OSSL_PROVIDER_load.return_value = handle.ffi.NULL
    handle.libcrypto.OSSL_PROVIDER_set_default_search_path.return_value = 1
    codes = iter([0x80000001, 0])
    handle.libcrypto.ERR_get_error.side_effect = lambda: next(codes)

    def _fill(_code, buf, _len):
        buf.value = b"unknown module"

    handle.libcrypto.ERR_error_string_n.side_effect = _fill

    with pytest.raises(op.OpenSSLProviderError, match="OSSL_PROVIDER_load.*unknown module"):
        op.load_provider(handle, "pkcs11", search_dir="/opt/ossl-modules")

    handle.libcrypto.OSSL_PROVIDER_set_default_search_path.assert_called_once()


def test_load_provider_is_idempotent() -> None:
    """A second load_provider call for the same name must not call into libcrypto again."""
    handle = _fake_handle()
    handle.libcrypto.OSSL_PROVIDER_load.return_value = object()  # non-NULL provider handle

    op.load_provider(handle, "pkcs11")
    op.load_provider(handle, "pkcs11")

    assert handle.libcrypto.OSSL_PROVIDER_load.call_count == 1


def test_load_cert_and_key_returns_first_pair_then_stops_walking() -> None:
    """The store-walk loop must short-circuit as soon as both objects are in hand."""
    handle = _fake_handle()
    handle.libcrypto.OSSL_STORE_open.return_value = object()
    # Walk produces: PKEY, CERT, would-produce-third-but-we-stop.
    eof_seq = iter([0, 0, 0, 1])
    handle.libcrypto.OSSL_STORE_eof.side_effect = lambda _store: next(eof_seq)
    handle.libcrypto.OSSL_STORE_load.side_effect = [
        "<info-pkey>",
        "<info-cert>",
        "<info-extra>",
    ]
    handle.libcrypto.OSSL_STORE_INFO_get_type.side_effect = [
        op._OSSL_STORE_INFO_PKEY,
        op._OSSL_STORE_INFO_CERT,
    ]
    handle.libcrypto.OSSL_STORE_INFO_get1_PKEY.return_value = "<pkey>"
    handle.libcrypto.OSSL_STORE_INFO_get1_CERT.return_value = "<cert>"

    cert, pkey = op.load_cert_and_key(handle, "pkcs11:?module-path=x")

    assert cert == "<cert>"
    assert pkey == "<pkey>"
    # Loop exited before a third OSSL_STORE_load.
    assert handle.libcrypto.OSSL_STORE_load.call_count == 2
    handle.libcrypto.OSSL_STORE_close.assert_called_once()


def test_load_cert_and_key_frees_partial_results_and_raises() -> None:
    """If the store only yielded a cert (no key), we must free the cert before raising."""
    handle = _fake_handle()
    handle.libcrypto.OSSL_STORE_open.return_value = object()
    eof_seq = iter([0, 1])
    handle.libcrypto.OSSL_STORE_eof.side_effect = lambda _store: next(eof_seq)
    handle.libcrypto.OSSL_STORE_load.return_value = "<info-cert>"
    handle.libcrypto.OSSL_STORE_INFO_get_type.return_value = op._OSSL_STORE_INFO_CERT
    handle.libcrypto.OSSL_STORE_INFO_get1_CERT.return_value = "<cert>"
    handle.libcrypto.ERR_get_error.return_value = 0

    with pytest.raises(op.OpenSSLProviderError, match="did not yield both"):
        op.load_cert_and_key(handle, "pkcs11:?module-path=x")

    handle.libcrypto.X509_free.assert_called_once_with("<cert>")
    handle.libcrypto.EVP_PKEY_free.assert_not_called()


def test_load_cert_and_key_open_failure_surfaces_drained_errors() -> None:
    handle = _fake_handle()
    handle.libcrypto.OSSL_STORE_open.return_value = handle.ffi.NULL
    codes = iter([0xFEED, 0])
    handle.libcrypto.ERR_get_error.side_effect = lambda: next(codes)

    def _fill(_code, buf, _len):
        buf.value = b"pkcs11: bad PIN"

    handle.libcrypto.ERR_error_string_n.side_effect = _fill

    with pytest.raises(op.OpenSSLProviderError, match="OSSL_STORE_open failed.*bad PIN"):
        op.load_cert_and_key(handle, "pkcs11:?module-path=x")


def test_load_cert_and_key_raises_when_load_returns_null_before_eof() -> None:
    handle = _fake_handle()
    handle.libcrypto.OSSL_STORE_open.return_value = object()
    eof_seq = iter([0, 0])
    handle.libcrypto.OSSL_STORE_eof.side_effect = lambda _store: next(eof_seq)
    handle.libcrypto.OSSL_STORE_load.return_value = handle.ffi.NULL
    codes = iter([0xBAD, 0])
    handle.libcrypto.ERR_get_error.side_effect = lambda: next(codes)

    def _fill(_code, buf, _len):
        buf.value = b"provider decode failed"

    handle.libcrypto.ERR_error_string_n.side_effect = _fill

    with pytest.raises(op.OpenSSLProviderError, match="OSSL_STORE_load failed.*provider decode failed"):
        op.load_cert_and_key(handle, "pkcs11:?module-path=x")


def test_install_on_ssl_context_verifies_pair_and_frees_refs() -> None:
    """Happy path: install cert + key, check the pair matches, then free our local references."""
    handle = _fake_handle()
    handle.libssl.SSL_CTX_use_certificate.return_value = 1
    handle.libssl.SSL_CTX_use_PrivateKey.return_value = 1
    handle.libssl.SSL_CTX_check_private_key.return_value = 1

    op.install_on_ssl_context(handle, "<ssl_ctx>", "<cert>", "<pkey>")

    handle.libssl.SSL_CTX_use_certificate.assert_called_once_with("<ssl_ctx>", "<cert>")
    handle.libssl.SSL_CTX_use_PrivateKey.assert_called_once_with("<ssl_ctx>", "<pkey>")
    handle.libssl.SSL_CTX_check_private_key.assert_called_once_with("<ssl_ctx>")
    handle.libcrypto.EVP_PKEY_free.assert_called_once_with("<pkey>")
    handle.libcrypto.X509_free.assert_called_once_with("<cert>")


def test_install_on_ssl_context_raises_on_mismatch_and_still_frees() -> None:
    """A cert/key mismatch from SSL_CTX_check_private_key must still trigger refcount cleanup."""
    handle = _fake_handle()
    handle.libssl.SSL_CTX_use_certificate.return_value = 1
    handle.libssl.SSL_CTX_use_PrivateKey.return_value = 1
    handle.libssl.SSL_CTX_check_private_key.return_value = 0
    handle.libcrypto.ERR_get_error.return_value = 0

    with pytest.raises(op.OpenSSLProviderError, match="check_private_key failed"):
        op.install_on_ssl_context(handle, "<ssl_ctx>", "<cert>", "<pkey>")

    # Local refs still freed.
    handle.libcrypto.EVP_PKEY_free.assert_called_once_with("<pkey>")
    handle.libcrypto.X509_free.assert_called_once_with("<cert>")


def test_configure_hostname_verification_sets_dns_host() -> None:
    handle = _fake_handle()
    handle.libssl.SSL_get0_param.return_value = "<verify-param>"
    handle.libcrypto.X509_VERIFY_PARAM_set1_host.return_value = 1

    op.configure_hostname_verification(handle, "<ssl>", "api.example.com")

    handle.libcrypto.X509_VERIFY_PARAM_set1_host.assert_called_once_with(
        "<verify-param>", b"api.example.com", len(b"api.example.com")
    )
    handle.libcrypto.X509_VERIFY_PARAM_set1_ip_asc.assert_not_called()


def test_configure_hostname_verification_sets_ip_address() -> None:
    handle = _fake_handle()
    handle.libssl.SSL_get0_param.return_value = "<verify-param>"
    handle.libcrypto.X509_VERIFY_PARAM_set1_ip_asc.return_value = 1

    op.configure_hostname_verification(handle, "<ssl>", "127.0.0.1")

    handle.libcrypto.X509_VERIFY_PARAM_set1_ip_asc.assert_called_once_with("<verify-param>", b"127.0.0.1")
    handle.libcrypto.X509_VERIFY_PARAM_set1_host.assert_not_called()


def test_assert_verify_ok_raises_on_nonzero_result() -> None:
    handle = _fake_handle()
    handle.libssl.SSL_get_verify_result.return_value = 62
    handle.libcrypto.ERR_get_error.return_value = 0

    with pytest.raises(op.OpenSSLProviderError, match="verification failed.*api.example.com.*62"):
        op.assert_verify_ok(handle, "<ssl>", "api.example.com")