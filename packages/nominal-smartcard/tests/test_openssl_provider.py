from __future__ import annotations

import ctypes
import ssl
import sys
from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._openssl_provider import (
    OpenSslProviderBridge,
    _ensure_provider_loaded,
    _get_openssl_error,
    _get_ssl_ctx_ptr,
    _load_pkey_from_store,
    _load_x509_from_der,
    _validate_library_binding,
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


# _load_pkey_from_store


def test_load_pkey_from_store_uri_unchanged_without_pin() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.OSSL_STORE_open.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardConfigurationError):
        _load_pkey_from_store(ffi, lib, "pkcs11:object=mykey")

    ffi.new.assert_called_once_with("char[]", b"pkcs11:object=mykey")


def test_load_pkey_from_store_pin_appended_without_trailing_paren() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.OSSL_STORE_open.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardConfigurationError):
        _load_pkey_from_store(ffi, lib, "pkcs11:object=mykey", pin="1234")

    ffi.new.assert_called_once_with("char[]", b"pkcs11:object=mykey?pin-value=1234")


# _load_x509_from_der


def test_load_x509_from_der_raises_on_invalid_der() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.d2i_X509.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardConfigurationError, match="Failed to parse DER certificate"):
        _load_x509_from_der(ffi, lib, b"\xff\xff\xff\xff")


def test_load_x509_from_der_raises_on_truncated_der() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.d2i_X509.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardConfigurationError, match="Failed to parse DER certificate"):
        _load_x509_from_der(ffi, lib, b"\x30\x82")


# _validate_library_binding


def test_validate_library_binding_raises_on_address_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    ffi = MagicMock()
    lib = MagicMock()

    # cffi resolves the symbol to 0x1000
    ffi.cast.return_value = 0x1000

    # ctypes resolves it to a different address
    mock_ctypes_result = MagicMock()
    mock_ctypes_result.value = 0x2000
    monkeypatch.setattr(ctypes, "CDLL", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(ctypes, "cast", MagicMock(return_value=mock_ctypes_result))

    with pytest.raises(SmartcardConfigurationError, match="does not match"):
        _validate_library_binding(ffi, lib)


def test_validate_library_binding_raises_when_symbol_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    ffi = MagicMock()
    lib = MagicMock()

    # Simulate a platform where SSL_CTX_check_private_key isn't in the process namespace
    monkeypatch.setattr(ctypes, "CDLL", MagicMock(return_value=MagicMock(spec=[])))

    with pytest.raises(SmartcardConfigurationError, match="SSL_CTX_check_private_key symbol not found"):
        _validate_library_binding(ffi, lib)


# _ensure_provider_loaded


def test_ensure_provider_loaded_raises_when_load_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(mod, "_loaded_provider", None)

    ffi = MagicMock()
    lib = MagicMock()
    lib.OSSL_PROVIDER_load.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardConfigurationError, match="Failed to load OpenSSL provider"):
        _ensure_provider_loaded(ffi, lib)


def test_ensure_provider_loaded_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    sentinel = object()
    monkeypatch.setattr(mod, "_loaded_provider", sentinel)

    ffi = MagicMock()
    lib = MagicMock()
    _ensure_provider_loaded(ffi, lib)

    lib.OSSL_PROVIDER_load.assert_not_called()
