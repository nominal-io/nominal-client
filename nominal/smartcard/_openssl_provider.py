from __future__ import annotations

import ctypes
import ssl
import sys
import threading
from dataclasses import dataclass
from typing import Any

from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._session import SmartcardSession
from nominal.smartcard.errors import SmartcardConfigurationError

# OSSL_STORE_INFO type constants (openssl/store.h)
_OSSL_STORE_INFO_CERT = 3
_OSSL_STORE_INFO_PKEY = 4

_PROVIDER_NAME = "pkcs11"

# Deferred at module import; initialised once by _load_ffi().
_ffi_lock: threading.Lock = threading.Lock()
_ffi: Any = None
_lib: Any = None

_provider_lock: threading.Lock = threading.Lock()
_loaded_provider: Any = None  # kept alive for the process lifetime once loaded
_loaded_provider_name: str | None = None


def _load_ffi() -> tuple[Any, Any]:
    """Lazily initialise the cffi bindings to libssl/libcrypto.

    We open the process symbol namespace (None) so we reuse the exact libssl
    instance already loaded by Python's ssl module.  Using a separate dlopen
    path would create a second library instance, causing memory-layout
    mismatches when we cast SSL_CTX* pointers across the boundary.
    """
    global _ffi, _lib
    with _ffi_lock:
        if _ffi is not None:
            return _ffi, _lib

        try:
            import cffi
        except ImportError as e:
            raise SmartcardConfigurationError("cffi is not installed. Run `pip install 'nominal[smartcard]'`.") from e

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

            /* Memory management */
            void EVP_PKEY_free(EVP_PKEY *pkey);
            void X509_free(X509 *a);

            /* Error reporting */
            unsigned long ERR_get_error(void);
            void          ERR_error_string_n(unsigned long e, char *buf, size_t len);
        """)

        # Load from the process namespace so we reuse Python's already-loaded libssl.
        lib = ffi.dlopen(None)
        _validate_library_binding(ffi, lib)

        _ffi = ffi
        _lib = lib
        return ffi, lib


def _validate_library_binding(ffi: Any, lib: Any) -> None:
    """Verify that our cffi handle resolves to the same libssl Python's ssl module uses.

    We compare the address of SSL_CTX_check_private_key as seen through ctypes
    (which uses Python's already-loaded symbols) against what cffi resolved.
    A mismatch means two incompatible libssl instances are in the process; passing
    pointers between them would corrupt memory.
    """
    try:
        ctypes_lib = ctypes.CDLL(None)
        ctypes_addr = ctypes.cast(ctypes_lib.SSL_CTX_check_private_key, ctypes.c_void_p).value
        cffi_addr = int(ffi.cast("uintptr_t", lib.SSL_CTX_check_private_key))
        if ctypes_addr != cffi_addr:
            raise SmartcardConfigurationError(
                "The libssl instance loaded by cffi does not match the one Python's ssl module uses. "
                "This would cause memory corruption. Ensure only one OpenSSL installation is active."
            )
    except AttributeError:
        # SSL_CTX_check_private_key not found in ctypes on this platform; skip validation.
        pass


def _ensure_provider_loaded(ffi: Any, lib: Any, provider_name: str) -> None:
    """Load the named OpenSSL provider once and keep it loaded for process lifetime.

    The provider must outlive every SSLContext built from it: the EVP_PKEY installed
    by SSL_CTX_use_PrivateKey dispatches signing through the provider on every TLS
    handshake.  Calling OSSL_PROVIDER_unload while an SSLContext is still in use
    removes the provider from OpenSSL's algorithm dispatch table and causes those
    handshakes to fail.
    """
    global _loaded_provider, _loaded_provider_name
    with _provider_lock:
        if _loaded_provider is not None:
            if _loaded_provider_name != provider_name:
                raise SmartcardConfigurationError(
                    f"OpenSSL provider {_loaded_provider_name!r} is already loaded in this process; "
                    f"cannot load a different provider {provider_name!r}. "
                    "All SmartcardConfig objects in the same process must use the same openssl_provider_path."
                )
            return
        provider = lib.OSSL_PROVIDER_load(ffi.NULL, provider_name.encode())
        if provider == ffi.NULL:
            err = _get_openssl_error(ffi, lib)
            raise SmartcardConfigurationError(
                f"Failed to load OpenSSL provider {provider_name!r}: {err}. "
                "Install pkcs11-provider (e.g. `brew install pkcs11-provider` on macOS, "
                "`apt install pkcs11-provider` on Ubuntu) and ensure it is on the OpenSSL "
                "providers search path."
            )
        _loaded_provider = provider
        _loaded_provider_name = provider_name


def _get_openssl_error(ffi: Any, lib: Any) -> str:
    err = lib.ERR_get_error()
    if err == 0:
        return "unknown error"
    buf = ffi.new("char[256]")
    lib.ERR_error_string_n(err, buf, 256)
    return ffi.string(buf).decode("utf-8", errors="replace")


def _get_ssl_ctx_ptr(ffi: Any, ssl_context: ssl.SSLContext) -> Any:
    """Extract the SSL_CTX* pointer from a Python ssl.SSLContext.

    CPython's PySSLContext struct layout (stable across 3.7–3.12 release builds):
      offset 0:  ob_refcnt  (Py_ssize_t, 8 bytes on 64-bit)
      offset 8:  ob_type    (PyTypeObject *, 8 bytes on 64-bit)
      offset 16: ctx        (SSL_CTX *)

    Python 3.13 free-threaded builds add extra per-thread refcount fields to
    PyObject_HEAD, shifting the SSL_CTX* to an unknown offset.  Reading the
    wrong offset would silently corrupt memory, so we refuse to proceed.
    """
    if getattr(sys.flags, "nogil", False):
        raise SmartcardConfigurationError(
            "Smartcard TLS is not supported under Python 3.13+ free-threaded mode: "
            "the PyObject header layout changes make the SSL_CTX* offset unpredictable. "
            "Use a standard (GIL-enabled) Python build."
        )
    ptr_size = ctypes.sizeof(ctypes.c_void_p)
    raw = ctypes.c_void_p.from_address(id(ssl_context) + 2 * ptr_size).value
    if not raw:
        raise SmartcardConfigurationError(
            "Failed to extract SSL_CTX* from ssl.SSLContext: null pointer. "
            "This may indicate an incompatible CPython version."
        )
    return ffi.cast("SSL_CTX *", raw)


def _pct_encode_pin(pin: str) -> str:
    """Percent-encode a PIN value for embedding in a PKCS#11 URI query component."""
    safe = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:[]@!$&'()*+,")
    parts = []
    for ch in pin:
        if ch in safe:
            parts.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                parts.append(f"%{byte:02x}")
    return "".join(parts)


def _load_pkey_from_store(ffi: Any, lib: Any, pkcs11_uri: str, pin: str | None = None) -> Any:
    """Open a PKCS#11 URI via OSSL_STORE and return the first EVP_PKEY found.

    When pin is provided it is embedded as ?pin-value=... so that pkcs11-provider
    can authenticate its own PKCS#11 session independently of any PyKCS11 session.
    This is necessary on real hardware tokens (DoD CAC, PIV) where C_Login state is
    not shared across separate C_Initialize call chains.  The cffi buffer holding
    the URI (including the PIN) is zeroed immediately after OSSL_STORE_open returns.
    """
    uri = pkcs11_uri if pin is None else f"{pkcs11_uri}?pin-value={_pct_encode_pin(pin)}"
    uri_bytes = uri.encode()
    uri_buf = ffi.new("char[]", uri_bytes)
    try:
        store = lib.OSSL_STORE_open(uri_buf, ffi.NULL, ffi.NULL, ffi.NULL, ffi.NULL)
    finally:
        ffi.buffer(uri_buf)[:] = bytes(len(uri_bytes) + 1)

    if store == ffi.NULL:
        err = _get_openssl_error(ffi, lib)
        raise SmartcardConfigurationError(
            f"OSSL_STORE_open failed for URI {pkcs11_uri!r}: {err}. "
            "Verify pkcs11-provider is installed and the PKCS#11 module path is correct."
        )

    pkey = ffi.NULL
    try:
        while not lib.OSSL_STORE_eof(store):
            info = lib.OSSL_STORE_load(store)
            if info == ffi.NULL:
                if lib.OSSL_STORE_error(store):
                    err = _get_openssl_error(ffi, lib)
                    raise SmartcardConfigurationError(f"OSSL_STORE_load error: {err}")
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
            f"No private key found at PKCS#11 URI {pkcs11_uri!r}. "
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


def _key_uri_from_cert_uri(cert_uri: str) -> str:
    """Derive the private-key PKCS#11 URI from a certificate URI.

    Strips any existing type= attribute and appends type=private.
    Handles type= appearing anywhere in the URI, including as the first attribute.
    """
    scheme = "pkcs11:"
    body = cert_uri[len(scheme) :] if cert_uri.startswith(scheme) else cert_uri
    attrs = [a for a in body.split(";") if not a.startswith("type=")]
    return scheme + ";".join(attrs) + ";type=private"


@dataclass(frozen=True)
class OpenSslProviderBridge:
    """Bridge from Python ssl.SSLContext to OpenSSL's pkcs11-provider via cffi.

    Setup (Python-side, once per session):
      1. Load pkcs11-provider into the in-process libcrypto via OSSL_PROVIDER_load.
      2. Open the private-key PKCS#11 URI through OSSL_STORE; the returned EVP_PKEY
         is an opaque handle — signing operations remain on the card.
      3. Parse the DER certificate obtained from PyKCS11 into an X509*.
      4. Install both onto the SSL_CTX* extracted from a standard ssl.SSLContext.

    Every subsequent TLS handshake runs entirely in compiled C; Python is not
    in the hot path.
    """

    config: SmartcardConfig

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        ffi, lib = _load_ffi()
        _ensure_provider_loaded(ffi, lib, self._provider_name())

        pkey = ffi.NULL
        x509 = ffi.NULL
        try:
            key_uri = _key_uri_from_cert_uri(session.pkcs11_uri)
            pkey = _load_pkey_from_store(ffi, lib, key_uri, session.pin)
            x509 = _load_x509_from_der(ffi, lib, session.certificate.der_certificate)

            ssl_ctx = self._make_base_ssl_context()
            raw_ctx = _get_ssl_ctx_ptr(ffi, ssl_ctx)

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

    def _provider_name(self) -> str:
        if self.config.openssl_provider_path is not None:
            return str(self.config.openssl_provider_path)
        return _PROVIDER_NAME

    def _make_base_ssl_context(self) -> ssl.SSLContext:
        """Create a baseline ssl.SSLContext with hostname verification enabled."""
        try:
            import truststore

            ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        except ImportError:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        return ctx
