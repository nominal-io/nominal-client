from __future__ import annotations

import ssl

import pytest

from nominal.smartcard._openssl_provider import _key_uri_from_cert_uri

# _key_uri_from_cert_uri


def test_key_uri_from_cert_uri_appends_type_private() -> None:
    assert _key_uri_from_cert_uri("pkcs11:token=CAC;id=%01") == "pkcs11:token=CAC;id=%01;type=private"


def test_key_uri_from_cert_uri_replaces_existing_type() -> None:
    assert _key_uri_from_cert_uri("pkcs11:token=CAC;id=%01;type=cert") == "pkcs11:token=CAC;id=%01;type=private"


def test_key_uri_from_cert_uri_strips_type_anywhere() -> None:
    result = _key_uri_from_cert_uri("pkcs11:type=cert;token=CAC;id=%01")
    assert "type=private" in result
    assert "type=cert" not in result


# _get_ssl_ctx_ptr


def test_get_ssl_ctx_ptr_returns_nonzero() -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("typedef struct ssl_ctx_st SSL_CTX;")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    from nominal.smartcard._openssl_provider import _get_ssl_ctx_ptr

    ptr = _get_ssl_ctx_ptr(ffi, ctx)
    assert int(ffi.cast("uintptr_t", ptr)) != 0
