from __future__ import annotations

import ctypes
import os
import ssl
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cffi

from nominal.smartcard._errors import (
    SmartcardConfigurationError,
    SmartcardPinError,
    SmartcardPinLockedError,
    SmartcardProviderError,
)
from nominal.smartcard._session import SmartcardSession

# OSSL_STORE_INFO_get_type returns this value for private keys.
_OSSL_STORE_INFO_PKEY = 4
_PROVIDER_NAME = "pkcs11"

# `pkcs11-module-path` is set in openssl.cnf but can be overridden.
_PKCS11_MODULE_ENV_VAR = "PKCS11_PROVIDER_MODULE"
_OPENSSL_VALIDATION_SYMBOLS = (
    "SSL_CTX_check_private_key",  # libssl
    "OSSL_PROVIDER_load",  # libcrypto, OpenSSL 3 provider API
    "ERR_get_error",  # libcrypto error queue
)
_CKR_PIN_LOCKED = "CKR_PIN_LOCKED"
_CKR_PIN_INCORRECT = "CKR_PIN_INCORRECT"
_CKR_PIN_LEN_RANGE = "CKR_PIN_LEN_RANGE"

# Deferred at module import; initialised once by _load_ffi().
_ffi_lock: threading.Lock = threading.Lock()
_ffi: Any = None
_lib: Any = None

_provider_lock: threading.Lock = threading.Lock()
_loaded_provider: Any = None  # kept alive for the process lifetime once loaded


def _python_ssl_extension_path() -> str | None:
    """Return the filesystem path for CPython's ``_ssl`` extension, when available."""
    path = getattr(getattr(ssl, "_ssl", None), "__file__", None)
    if isinstance(path, str) and path:
        return path
    return None


def _load_python_ssl_library(ffi: Any) -> Any | None:
    """Load OpenSSL symbols through Python's own ``_ssl`` extension.

    CPython's ``ssl.SSLContext`` is backed by the OpenSSL instance linked into
    the private ``_ssl`` extension. Resolving cffi symbols through that extension
    lets the dynamic loader follow exactly the same dependency edges Python uses.

    Specifically on MacOS, ``dlopen(None)`` may resolve libssl symbols from
    Apple's dyld shared-cache compatibility library rather than the Homebrew or
    python.org OpenSSL that created the ``SSL_CTX*`` inside ``ssl.SSLContext``.
    Passing an ``SSL_CTX*`` to functions from that other library can crash the
    process. On Darwin, absence of a usable anchor is therefore fatal. On other
    platforms we keep the older process-namespace fallback for unusual Python
    builds that do not expose ``ssl._ssl.__file__``.
    """
    ssl_extension_path = _python_ssl_extension_path()
    if ssl_extension_path is None:
        if sys.platform == "darwin":
            raise SmartcardConfigurationError(
                "Could not locate Python's _ssl extension. Refusing to bind OpenSSL "
                "symbols without proving they match ssl.SSLContext."
            )
        return None

    try:
        lib = ffi.dlopen(ssl_extension_path)
    except OSError as e:
        if sys.platform == "darwin":
            raise SmartcardConfigurationError(
                "Failed to load OpenSSL symbols through Python's _ssl extension. "
                "Refusing to bind OpenSSL symbols without proving they match ssl.SSLContext."
            ) from e
        return None

    _validate_library_binding(ffi, lib, python_ssl_path=ssl_extension_path)
    return lib


def _load_ffi() -> tuple[Any, Any]:
    """Lazily initialise the cffi bindings to libssl/libcrypto."""
    global _ffi, _lib
    with _ffi_lock:
        if _ffi is not None:
            return _ffi, _lib

        ffi = cffi.FFI()
        ffi.cdef("""
            /* Opaque handles */
            typedef struct ssl_ctx_st          SSL_CTX;
            typedef struct evp_pkey_st         EVP_PKEY;
            typedef struct x509_st             X509;
            typedef struct ossl_provider_st    OSSL_PROVIDER;
            typedef struct ossl_store_ctx_st   OSSL_STORE_CTX;
            typedef struct ossl_store_info_st  OSSL_STORE_INFO;
            typedef struct ossl_lib_ctx_st     OSSL_LIB_CTX;
            typedef struct ui_method_st        UI_METHOD;

            /* Provider management */
            OSSL_PROVIDER *OSSL_PROVIDER_load(OSSL_LIB_CTX *libctx, const char *name);
            int            OSSL_PROVIDER_unload(OSSL_PROVIDER *prov);

            /* OSSL_STORE — URI-based key/cert loading */
            typedef OSSL_STORE_INFO *(*OSSL_STORE_post_process_info_fn)(OSSL_STORE_INFO *, void *);

            OSSL_STORE_CTX *OSSL_STORE_open(
                const char *uri,
                const UI_METHOD *ui_method,
                void *ui_data,
                OSSL_STORE_post_process_info_fn post_process,
                void *post_process_data);

            int             OSSL_STORE_eof(OSSL_STORE_CTX *ctx);
            int             OSSL_STORE_error(OSSL_STORE_CTX *ctx);
            OSSL_STORE_INFO *OSSL_STORE_load(OSSL_STORE_CTX *ctx);
            int             OSSL_STORE_INFO_get_type(const OSSL_STORE_INFO *info);
            EVP_PKEY       *OSSL_STORE_INFO_get1_PKEY(OSSL_STORE_INFO *info);
            X509           *OSSL_STORE_INFO_get1_CERT(OSSL_STORE_INFO *info);
            void            OSSL_STORE_INFO_free(OSSL_STORE_INFO *info);
            int             OSSL_STORE_close(OSSL_STORE_CTX *ctx);

            /* Load X509 from DER bytes */
            X509 *d2i_X509(X509 **px, const unsigned char **in, long len);

            /* SSL_CTX key/cert installation */
            int SSL_CTX_use_certificate(SSL_CTX *ctx, X509 *x);
            int SSL_CTX_use_PrivateKey(SSL_CTX *ctx, EVP_PKEY *pkey);
            int SSL_CTX_check_private_key(const SSL_CTX *ctx);
            X509 *SSL_CTX_get0_certificate(const SSL_CTX *ctx);

            /* Memory management */
            void EVP_PKEY_free(EVP_PKEY *pkey);
            void X509_free(X509 *a);

            /* Error reporting */
            unsigned long ERR_get_error(void);
            void          ERR_error_string_n(unsigned long e, char *buf, size_t len);
        """)

        lib = _load_python_ssl_library(ffi)
        if lib is None:
            lib = ffi.dlopen(None)
            _validate_library_binding(ffi, lib)

        _ffi = ffi
        _lib = lib
        return ffi, lib


def _validate_library_binding(ffi: Any, lib: Any, python_ssl_path: str | None = None) -> None:
    """Verify that our cffi handle resolves to Python's OpenSSL symbols.

    When ``python_ssl_path`` is provided, ``ctypes`` opens Python's private
    ``_ssl`` extension and resolves symbols through the same dependency graph
    that backs ``ssl.SSLContext``. Without it, this falls back to the process
    namespace for platforms where that remains the best available option.

    A mismatch means two incompatible OpenSSL instances are in the process;
    passing pointers between them would corrupt memory.
    """
    try:
        ctypes_lib = ctypes.CDLL(python_ssl_path) if python_ssl_path else ctypes.CDLL(None)
        for symbol in _OPENSSL_VALIDATION_SYMBOLS:
            ctypes_addr = ctypes.cast(getattr(ctypes_lib, symbol), ctypes.c_void_p).value
            cffi_addr = int(ffi.cast("uintptr_t", getattr(lib, symbol)))
            if ctypes_addr != cffi_addr:
                raise SmartcardConfigurationError(
                    f"The OpenSSL symbol {symbol!r} loaded by cffi does not match the expected OpenSSL symbols. "
                    "This would cause memory corruption. Ensure only one OpenSSL installation is active."
                )
    except AttributeError as e:
        raise SmartcardConfigurationError(
            "Failed to validate OpenSSL library binding: a required OpenSSL symbol was not "
            "found. This may indicate an unsupported platform or Python build. "
            "Use a standard CPython build."
        ) from e
    except OSError as e:
        raise SmartcardConfigurationError(
            "Failed to validate OpenSSL library binding: could not open library. "
            "This may indicate an unsupported platform or Python build. Use a standard CPython build."
        ) from e


def _ensure_provider_loaded(ffi: Any, lib: Any, module_path: Path) -> None:
    """Load the named OpenSSL provider once and keep it loaded for process lifetime.

    The provider must outlive every SSLContext built from it: the EVP_PKEY installed
    by SSL_CTX_use_PrivateKey dispatches signing through the provider on every TLS
    handshake.  Calling OSSL_PROVIDER_unload while an SSLContext is still in use
    removes the provider from OpenSSL's algorithm dispatch table and causes those
    handshakes to fail.
    """
    global _loaded_provider
    with _provider_lock:
        if _loaded_provider is not None:
            return
        os.environ.setdefault(_PKCS11_MODULE_ENV_VAR, str(module_path))

        provider = lib.OSSL_PROVIDER_load(ffi.NULL, _PROVIDER_NAME.encode())
        if provider == ffi.NULL:
            err = _get_openssl_error(ffi, lib)
            raise SmartcardConfigurationError(
                f"Failed to load OpenSSL provider {_PROVIDER_NAME!r}: {err}. "
                "Install pkcs11-provider and ensure it is on the OpenSSL providers search path."
            )
        _loaded_provider = provider


def _get_openssl_error(ffi: Any, lib: Any) -> str:
    err = lib.ERR_get_error()
    if err == 0:
        return "unknown error"
    buf = ffi.new("char[256]")
    lib.ERR_error_string_n(err, buf, 256)
    return str(ffi.string(buf).decode("utf-8", errors="replace"))


def _raise_store_error(err: str, context: str) -> None:
    """Raise the most specific PIN or configuration error for an OSSL_STORE failure."""
    if _CKR_PIN_LOCKED in err:
        raise SmartcardPinLockedError(f"{context}: {err}")
    if _CKR_PIN_INCORRECT in err:
        raise SmartcardPinError(f"{context}: {err}")
    if _CKR_PIN_LEN_RANGE in err:
        raise SmartcardPinError(f"{context}: {err}")
    raise SmartcardProviderError(f"{context}: {err}")


def _get_ssl_ctx_ptr(ffi: Any, ssl_context: ssl.SSLContext) -> Any:
    """Extract the SSL_CTX* pointer from a Python ssl.SSLContext.

    CPython's PySSLContext struct layout (GIL-enabled release builds):
      offset 0:  ob_refcnt  (Py_ssize_t, 8 bytes on 64-bit)
      offset 8:  ob_type    (PyTypeObject *, 8 bytes on 64-bit)
      offset 16: ctx        (SSL_CTX *)

    Two non-standard configurations break this layout and are explicitly rejected:

    * Python 3.13+ free-threaded builds (sys.flags.nogil): PyObject_HEAD grows
      due to per-thread refcount fields (ob_tid, ob_ref_local, ob_ref_shared),
      shifting ctx to an unknown offset.
    * Debug builds compiled with --with-pydebug (Py_TRACE_REFS): two extra
      _ob_prev/_ob_next trace pointers are prepended before ob_refcnt, also
      shifting ctx by 2 * ptr_size.

    sys.gettotalrefcount is only present in --with-pydebug builds, so its
    presence reliably distinguishes them from release builds.
    """
    if getattr(sys.flags, "nogil", False):
        raise SmartcardConfigurationError(
            "Smartcard TLS is not supported under Python 3.13+ free-threaded mode: "
            "the PyObject header layout changes make the SSL_CTX* offset unpredictable. "
            "Use a standard (GIL-enabled) Python build."
        )
    if hasattr(sys, "gettotalrefcount"):
        raise SmartcardConfigurationError(
            "Smartcard TLS is not supported under Python debug builds: "
            "Py_TRACE_REFS prepends extra pointers to PyObject_HEAD, shifting the "
            "SSL_CTX* to an unpredictable offset. Use a standard release build."
        )
    ptr_size = ctypes.sizeof(ctypes.c_void_p)
    raw = ctypes.c_void_p.from_address(id(ssl_context) + 2 * ptr_size).value
    if not raw:
        raise SmartcardConfigurationError(
            "Failed to extract SSL_CTX* from ssl.SSLContext: null pointer. "
            "This may indicate an incompatible CPython version."
        )
    return ffi.cast("SSL_CTX *", raw)


def _load_pkey_from_store(ffi: Any, lib: Any, private_key_uri: str) -> Any:
    """Open a PKCS#11 URI via OSSL_STORE and return the first EVP_PKEY found."""
    uri_bytes = private_key_uri.encode()
    uri_buf = ffi.new("char[]", uri_bytes)
    store = lib.OSSL_STORE_open(uri_buf, ffi.NULL, ffi.NULL, ffi.NULL, ffi.NULL)

    if store == ffi.NULL:
        err = _get_openssl_error(ffi, lib)
        _raise_store_error(
            err,
            f"OSSL_STORE_open failed for URI {private_key_uri!r}. "
            "Verify pkcs11-provider is installed and the PKCS#11 module path is correct",
        )

    pkey = ffi.NULL
    try:
        while not lib.OSSL_STORE_eof(store):
            info = lib.OSSL_STORE_load(store)
            if info == ffi.NULL:
                if lib.OSSL_STORE_error(store):
                    err = _get_openssl_error(ffi, lib)
                    _raise_store_error(err, "OSSL_STORE_load error")
                continue
            if lib.OSSL_STORE_INFO_get_type(info) == _OSSL_STORE_INFO_PKEY:
                pkey = lib.OSSL_STORE_INFO_get1_PKEY(info)
                lib.OSSL_STORE_INFO_free(info)
                break
            lib.OSSL_STORE_INFO_free(info)
    finally:
        lib.OSSL_STORE_close(store)

    if pkey == ffi.NULL:
        raise SmartcardConfigurationError(
            f"No private key found at PKCS#11 URI {private_key_uri!r}. "
            "Verify the token is present and the object ID is correct."
        )
    return pkey


def _load_x509_from_der(ffi: Any, lib: Any, der_cert: bytes) -> Any:
    """Load an X509* from DER-encoded certificate bytes."""
    buf = ffi.new("unsigned char[]", der_cert)
    ptr = ffi.new("const unsigned char *[1]", [buf])
    x509 = lib.d2i_X509(ffi.NULL, ptr, len(der_cert))
    if x509 == ffi.NULL:
        err = _get_openssl_error(ffi, lib)
        raise SmartcardConfigurationError(f"Failed to parse DER certificate: {err}")
    return x509


class _LockedSSLContext(ssl.SSLContext):
    """ssl.SSLContext subclass that serializes concurrent wrap_socket calls.

    A single SSL_CTX* with a PKCS#11-backed EVP_PKEY is shared across all threads.
    Concurrent TLS handshakes invoke signing operations through the same PKCS#11
    session, which is not thread-safe. Serializing wrap_socket (which runs the
    full handshake) prevents concurrent access to the underlying token session.
    """

    _wrap_lock: threading.Lock

    def __new__(cls, *args: Any, **kwargs: Any) -> _LockedSSLContext:
        instance: _LockedSSLContext = super().__new__(cls, *args, **kwargs)
        instance._wrap_lock = threading.Lock()
        return instance

    def wrap_socket(self, *args: Any, **kwargs: Any) -> ssl.SSLSocket:
        with self._wrap_lock:
            return super().wrap_socket(*args, **kwargs)


@dataclass(frozen=True)
class OpenSslProviderBridge:
    """Bridge from Python ssl.SSLContext to OpenSSL's pkcs11-provider via cffi.

    Setup (Python-side, once per session):
      1. Load pkcs11-provider into the in-process libcrypto via OSSL_PROVIDER_load.
      2. Open the private-key PKCS#11 URI through OSSL_STORE; the returned EVP_PKEY
         is an opaque handle; signing operations remain on the card.
      3. Parse the DER certificate obtained from PyKCS11 into an X509*.
      4. Install both onto the SSL_CTX* extracted from a standard ssl.SSLContext.

    Every subsequent TLS handshake runs entirely in compiled C.
    """

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        ffi, lib = _load_ffi()
        _ensure_provider_loaded(ffi, lib, session.module_path)

        pkey = ffi.NULL
        x509 = ffi.NULL
        try:
            pkey = _load_pkey_from_store(ffi, lib, session.private_key_uri)
            x509 = _load_x509_from_der(ffi, lib, session.certificate.der_certificate)

            ssl_ctx = self._make_base_ssl_context()
            raw_ctx = _get_ssl_ctx_ptr(ffi, ssl_ctx)

            # Sentinel: a freshly-created SSL_CTX must carry no certificate.
            # A non-NULL return means we read the wrong memory offset and must
            # not proceed since the subsequent SSL_CTX_use_* calls would corrupt
            # whatever struct (if any) raw_ctx actually points at.
            if lib.SSL_CTX_get0_certificate(raw_ctx) != ffi.NULL:
                raise SmartcardConfigurationError(
                    "SSL_CTX* offset validation failed: unexpected certificate present on "
                    "a freshly-created ssl.SSLContext. This CPython build's PySSLContext "
                    "layout differs from the expected offset. Use a standard CPython release build."
                )

            if lib.SSL_CTX_use_certificate(raw_ctx, x509) != 1:
                err = _get_openssl_error(ffi, lib)
                raise SmartcardConfigurationError(f"SSL_CTX_use_certificate failed: {err}")

            if lib.SSL_CTX_use_PrivateKey(raw_ctx, pkey) != 1:
                err = _get_openssl_error(ffi, lib)
                raise SmartcardConfigurationError(f"SSL_CTX_use_PrivateKey failed: {err}")

            if lib.SSL_CTX_check_private_key(raw_ctx) != 1:
                err = _get_openssl_error(ffi, lib)
                raise SmartcardConfigurationError(
                    f"SSL_CTX_check_private_key failed: {err}. "
                    "The certificate and private key on the smartcard do not match."
                )

            return ssl_ctx
        finally:
            # SSL_CTX_use_* takes an up_ref internally; release our references here.
            if pkey != ffi.NULL:
                lib.EVP_PKEY_free(pkey)
            if x509 != ffi.NULL:
                lib.X509_free(x509)

    def _make_base_ssl_context(self) -> ssl.SSLContext:
        """Create a baseline ssl.SSLContext with hostname verification enabled.

        load_default_certs() loads OS-native CA certificates (Windows certificate
        store, macOS Keychain / system bundle, OpenSSL default paths on Linux).
        This matches the trust-store behaviour of the rest of the codebase and
        ensures enterprise/government root CAs added to the OS store are trusted
        without requiring cffi-level changes.
        """
        ctx = _LockedSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        return ctx
