from __future__ import annotations

import ssl
import sys
from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._openssl_provider import (
    OpenSslProviderBridge,
    _get_openssl_error,
    _get_ssl_ctx_ptr,
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


def test_get_ssl_ctx_ptr_raises_on_nogil(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("typedef struct ssl_ctx_st SSL_CTX;")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    mock_flags = MagicMock()
    mock_flags.nogil = True
    monkeypatch.setattr(sys, "flags", mock_flags)

    with pytest.raises(SmartcardConfigurationError, match="free-threaded"):
        _get_ssl_ctx_ptr(ffi, ctx)


def test_get_ssl_ctx_ptr_raises_on_debug_build(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("typedef struct ssl_ctx_st SSL_CTX;")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    monkeypatch.setattr(sys, "gettotalrefcount", lambda: 0, raising=False)

    with pytest.raises(SmartcardConfigurationError, match="debug build"):
        _get_ssl_ctx_ptr(ffi, ctx)


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


def test_make_base_ssl_context_loads_os_default_certs() -> None:
    bridge = OpenSslProviderBridge()
    with patch.object(ssl.SSLContext, "load_default_certs") as mock_load:
        bridge._make_base_ssl_context()
    mock_load.assert_called_once_with(ssl.Purpose.SERVER_AUTH)
