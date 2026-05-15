from __future__ import annotations

import ssl
from unittest.mock import MagicMock

import pytest

from nominal.smartcard._openssl_provider import (
    OpenSslProviderBridge,
    _get_openssl_error,
    _get_ssl_ctx_ptr,
    _pct_encode_pin,
)

# _get_ssl_ctx_ptr


def test_get_ssl_ctx_ptr_returns_nonzero() -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("typedef struct ssl_ctx_st SSL_CTX;")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    ptr = _get_ssl_ctx_ptr(ffi, ctx)
    assert int(ffi.cast("uintptr_t", ptr)) != 0


# _pct_encode_pin


def test_pct_encode_pin_passes_safe_alphanumeric() -> None:
    assert _pct_encode_pin("ABC123xyz") == "ABC123xyz"


def test_pct_encode_pin_encodes_space() -> None:
    assert _pct_encode_pin("my pin") == "my%20pin"


def test_pct_encode_pin_encodes_semicolon() -> None:
    assert _pct_encode_pin(";") == "%3b"


def test_pct_encode_pin_encodes_unicode() -> None:
    # é is UTF-8 0xc3 0xa9
    assert _pct_encode_pin("é") == "%c3%a9"


def test_pct_encode_pin_safe_special_chars_pass_through() -> None:
    safe = "-._~:[]@!$&'()*+,"
    assert _pct_encode_pin(safe) == safe


def test_pct_encode_pin_empty_string() -> None:
    assert _pct_encode_pin("") == ""


# _get_openssl_error


def test_get_openssl_error_returns_unknown_when_no_error() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.ERR_get_error.return_value = 0
    assert _get_openssl_error(ffi, lib) == "unknown error"


def test_get_openssl_error_returns_formatted_string() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.ERR_get_error.return_value = 42
    fake_buf = MagicMock()
    ffi.new.return_value = fake_buf
    ffi.string.return_value = b"error:1234:some library:some function:some reason"

    result = _get_openssl_error(ffi, lib)

    assert result == "error:1234:some library:some function:some reason"
    lib.ERR_error_string_n.assert_called_once_with(42, fake_buf, 256)


# OpenSslProviderBridge._make_base_ssl_context


def test_make_base_ssl_context_check_hostname_enabled() -> None:
    bridge = OpenSslProviderBridge()
    ctx = bridge._make_base_ssl_context()
    assert ctx.check_hostname is True


def test_make_base_ssl_context_requires_certificate() -> None:
    bridge = OpenSslProviderBridge()
    ctx = bridge._make_base_ssl_context()
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_make_base_ssl_context_minimum_tls_version() -> None:
    bridge = OpenSslProviderBridge()
    ctx = bridge._make_base_ssl_context()
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
