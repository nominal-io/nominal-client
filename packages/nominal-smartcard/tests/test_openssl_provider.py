from __future__ import annotations

import ctypes
import ssl
import sys
from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard._errors import (
    SmartcardConfigurationError,
    SmartcardPinError,
    SmartcardPinLockedError,
    SmartcardProviderError,
)
from nominal.smartcard._openssl_provider import (
    OpenSslProviderBridge,
    _ensure_provider_loaded,
    _get_openssl_error,
    _get_ssl_ctx_ptr,
    _load_pkey_from_store,
    _load_python_ssl_library,
    _load_x509_from_der,
    _python_ssl_extension_path,
    _raise_store_error,
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


def test_make_base_ssl_context() -> None:
    bridge = OpenSslProviderBridge()
    with patch.object(ssl.SSLContext, "load_default_certs") as mock_load:
        ctx = bridge._make_base_ssl_context()
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
    mock_load.assert_called_once_with(ssl.Purpose.SERVER_AUTH)


# _load_pkey_from_store


def test_load_pkey_from_store_raises_provider_error_on_open_failure() -> None:
    ffi = MagicMock()
    lib = MagicMock()
    lib.OSSL_STORE_open.return_value = ffi.NULL
    lib.ERR_get_error.return_value = 0

    with pytest.raises(SmartcardProviderError):
        _load_pkey_from_store(ffi, lib, "pkcs11:object=mykey")

    ffi.new.assert_called_once_with("char[]", b"pkcs11:object=mykey")


# _raise_store_error


def test_raise_store_error_raises_pin_locked_error() -> None:
    with pytest.raises(SmartcardPinLockedError):
        _raise_store_error("CKR_PIN_LOCKED", "context")


def test_raise_store_error_raises_pin_error_on_incorrect() -> None:
    with pytest.raises(SmartcardPinError):
        _raise_store_error("CKR_PIN_INCORRECT", "context")


def test_raise_store_error_pin_locked_takes_priority_over_incorrect() -> None:
    with pytest.raises(SmartcardPinLockedError):
        _raise_store_error("CKR_PIN_LOCKED CKR_PIN_INCORRECT", "context")


def test_raise_store_error_raises_provider_error_for_other_errors() -> None:
    with pytest.raises(SmartcardProviderError):
        _raise_store_error("some other error", "context")


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


# _python_ssl_extension_path


def test_python_ssl_extension_path_returns_ssl_module_file(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSslModule:
        __file__ = "/tmp/_ssl.test.so"

    monkeypatch.setattr(ssl, "_ssl", FakeSslModule())

    assert _python_ssl_extension_path() == "/tmp/_ssl.test.so"


def test_python_ssl_extension_path_returns_none_without_file(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSslModule:
        pass

    monkeypatch.setattr(ssl, "_ssl", FakeSslModule())

    assert _python_ssl_extension_path() is None


def test_python_ssl_extension_path_returns_none_when_ssl_has_no_ssl_attr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(ssl, "_ssl", raising=False)

    assert _python_ssl_extension_path() is None


def test_python_ssl_extension_path_returns_none_when_file_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSslModule:
        __file__ = None

    monkeypatch.setattr(ssl, "_ssl", FakeSslModule())

    assert _python_ssl_extension_path() is None


def test_python_ssl_extension_path_returns_none_when_file_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSslModule:
        __file__ = ""

    monkeypatch.setattr(ssl, "_ssl", FakeSslModule())

    assert _python_ssl_extension_path() is None


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS-only")
def test_python_ssl_extension_path_resolves_openssl_symbols_on_macos() -> None:
    path = _python_ssl_extension_path()
    assert path is not None

    ctypes_lib = ctypes.CDLL(path)
    assert ctypes.cast(ctypes_lib.SSL_CTX_check_private_key, ctypes.c_void_p).value is not None


# _load_python_ssl_library


def test_load_python_ssl_library_returns_none_without_anchor_on_non_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(mod, "_python_ssl_extension_path", lambda: None)

    assert _load_python_ssl_library(MagicMock()) is None


def test_load_python_ssl_library_raises_without_anchor_on_macos(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(mod, "_python_ssl_extension_path", lambda: None)

    with pytest.raises(SmartcardConfigurationError, match="Could not locate Python's _ssl extension"):
        _load_python_ssl_library(MagicMock())


def test_load_python_ssl_library_raises_on_macos_dlopen_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(mod, "_python_ssl_extension_path", lambda: "/tmp/_ssl.test.so")

    ffi = MagicMock()
    ffi.dlopen.side_effect = OSError("boom")

    with pytest.raises(SmartcardConfigurationError, match="Failed to load OpenSSL symbols"):
        _load_python_ssl_library(ffi)


def test_load_python_ssl_library_returns_none_on_non_darwin_dlopen_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(mod, "_python_ssl_extension_path", lambda: "/tmp/_ssl.test.so")

    ffi = MagicMock()
    ffi.dlopen.side_effect = OSError("no such file")

    assert _load_python_ssl_library(ffi) is None


def test_load_python_ssl_library_validates_and_returns_lib_when_anchor_found(monkeypatch: pytest.MonkeyPatch) -> None:
    import nominal.smartcard._openssl_provider as mod

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(mod, "_python_ssl_extension_path", lambda: "/tmp/_ssl.test.so")

    fake_lib = MagicMock()
    ffi = MagicMock()
    ffi.dlopen.return_value = fake_lib

    validated_paths: list[str | None] = []

    def fake_validate(ffi: object, lib: object, python_ssl_path: str | None = None) -> None:
        validated_paths.append(python_ssl_path)

    monkeypatch.setattr(mod, "_validate_library_binding", fake_validate)

    result = _load_python_ssl_library(ffi)

    assert result is fake_lib
    assert validated_paths == ["/tmp/_ssl.test.so"]


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS-only")
def test_load_python_ssl_library_anchors_cffi_to_python_ssl_on_macos() -> None:
    pytest.importorskip("cffi")
    import cffi

    ffi = cffi.FFI()
    ffi.cdef("""
        typedef struct ssl_ctx_st SSL_CTX;
        typedef struct ossl_provider_st OSSL_PROVIDER;
        typedef struct ossl_lib_ctx_st OSSL_LIB_CTX;

        int SSL_CTX_check_private_key(const SSL_CTX *ctx);
        OSSL_PROVIDER *OSSL_PROVIDER_load(OSSL_LIB_CTX *libctx, const char *name);
        unsigned long ERR_get_error(void);
    """)

    assert _load_python_ssl_library(ffi) is not None


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


def test_validate_library_binding_uses_python_ssl_anchor(monkeypatch: pytest.MonkeyPatch) -> None:
    ffi = MagicMock()
    lib = MagicMock()

    ffi.cast.return_value = 0x1000

    mock_ctypes_result = MagicMock()
    mock_ctypes_result.value = 0x1000
    cdll = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(ctypes, "CDLL", cdll)
    monkeypatch.setattr(ctypes, "cast", MagicMock(return_value=mock_ctypes_result))

    _validate_library_binding(ffi, lib, python_ssl_path="/tmp/_ssl.test.so")

    cdll.assert_called_once_with("/tmp/_ssl.test.so")


def test_validate_library_binding_raises_when_symbol_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    ffi = MagicMock()
    lib = MagicMock()

    # Simulate a platform where SSL_CTX_check_private_key isn't in the process namespace
    monkeypatch.setattr(ctypes, "CDLL", MagicMock(return_value=MagicMock(spec=[])))

    with pytest.raises(SmartcardConfigurationError, match="required OpenSSL symbol was not found"):
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
