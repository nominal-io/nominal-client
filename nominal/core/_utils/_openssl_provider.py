"""Thin cffi wrapper around the OpenSSL provider + store APIs used for smartcard / CAC TLS.

cryptography 48+ uses Rust bindings and intentionally does not expose `OSSL_PROVIDER_*` or `OSSL_STORE_*`
through its cffi binding, and pyOpenSSL never wrapped them. To drive libp11's `pkcs11prov` (or any other
RFC 7512 PKCS#11 provider) from Python we have to call into libcrypto directly. This module isolates that
FFI surface so the smartcard module reads like normal Python.

We use `cffi` rather than `ctypes` because it lets us declare the C surface once as a single typed block
(no per-call `argtypes`/`restype` plumbing) and gives us proper C-typed return values that can be passed
back to other C functions without manual `c_void_p` round-tripping.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import cffi

logger = logging.getLogger(__name__)

# OSSL_STORE_INFO_TYPE constants from openssl/store.h. Stable across OpenSSL 3.x.
_OSSL_STORE_INFO_PKEY = 3
_OSSL_STORE_INFO_CERT = 4
_X509_V_OK = 0

# Candidate library filenames per platform. Order matters: Linux/manylinux ships .so.3, macOS Homebrew
# ships .3.dylib, Windows ships libcrypto-3-x64.dll. We try each until one opens.
_LIBCRYPTO_CANDIDATES = (
    "libcrypto.so.3",
    "libcrypto.3.dylib",
    "libcrypto.dylib",
    "libcrypto-3-x64.dll",
    "libcrypto-3.dll",
)
_LIBSSL_CANDIDATES = (
    "libssl.so.3",
    "libssl.3.dylib",
    "libssl.dylib",
    "libssl-3-x64.dll",
    "libssl-3.dll",
)

_CDEF = """
typedef struct OSSL_PROVIDER OSSL_PROVIDER;
typedef struct OSSL_LIB_CTX OSSL_LIB_CTX;
typedef struct OSSL_STORE_CTX OSSL_STORE_CTX;
typedef struct OSSL_STORE_INFO OSSL_STORE_INFO;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct x509_st X509;
typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct X509_VERIFY_PARAM_st X509_VERIFY_PARAM;

OSSL_PROVIDER *OSSL_PROVIDER_load(OSSL_LIB_CTX *libctx, const char *name);
int OSSL_PROVIDER_set_default_search_path(OSSL_LIB_CTX *libctx, const char *path);

OSSL_STORE_CTX *OSSL_STORE_open(
    const char *uri, void *ui_method, void *ui_data, void *post_process, void *post_process_data
);
OSSL_STORE_INFO *OSSL_STORE_load(OSSL_STORE_CTX *ctx);
int OSSL_STORE_eof(OSSL_STORE_CTX *ctx);
int OSSL_STORE_close(OSSL_STORE_CTX *ctx);
int OSSL_STORE_INFO_get_type(const OSSL_STORE_INFO *info);
EVP_PKEY *OSSL_STORE_INFO_get1_PKEY(const OSSL_STORE_INFO *info);
X509 *OSSL_STORE_INFO_get1_CERT(const OSSL_STORE_INFO *info);
void OSSL_STORE_INFO_free(OSSL_STORE_INFO *info);

int SSL_CTX_use_PrivateKey(SSL_CTX *ctx, EVP_PKEY *pkey);
int SSL_CTX_use_certificate(SSL_CTX *ctx, X509 *x);
int SSL_CTX_check_private_key(const SSL_CTX *ctx);
X509_VERIFY_PARAM *SSL_get0_param(SSL *ssl);
long SSL_get_verify_result(const SSL *ssl);

int X509_VERIFY_PARAM_set1_host(X509_VERIFY_PARAM *param, const char *name, size_t namelen);
int X509_VERIFY_PARAM_set1_ip_asc(X509_VERIFY_PARAM *param, const char *ipasc);

void EVP_PKEY_free(EVP_PKEY *pkey);
void X509_free(X509 *x);

unsigned long ERR_get_error(void);
void ERR_error_string_n(unsigned long e, char *buf, size_t len);
"""


class OpenSSLProviderError(RuntimeError):
    """Raised for any FFI-level failure that prevents the smartcard TLS path from completing."""


class _LibHandle:
    """A pair of dlopen'd handles (libcrypto + libssl) sharing a single FFI."""

    def __init__(self, ffi: cffi.FFI, libcrypto: Any, libssl: Any) -> None:
        self.ffi = ffi
        self.libcrypto = libcrypto
        self.libssl = libssl


_handle_lock = threading.Lock()
_cached_handle: _LibHandle | None = None


def _open_one(ffi: cffi.FFI, candidates: tuple[str, ...]) -> Any:
    last_err: Exception | None = None
    for name in candidates:
        try:
            return ffi.dlopen(name)
        except OSError as e:  # cffi raises OSError when dlopen fails
            last_err = e
    raise OpenSSLProviderError(
        f"Could not dlopen any of {candidates!r} — is OpenSSL 3.x installed? Last error: {last_err}"
    )


def lib_handle() -> _LibHandle:
    """Return a process-wide handle to libcrypto + libssl, opened by name (so we don't depend on whether
    pyOpenSSL has been imported yet to seed the process symbol table).

    Both libraries are opened with the same FFI instance, so cdata pointers returned by libcrypto (e.g.
    EVP_PKEY*) flow into libssl calls (SSL_CTX_use_PrivateKey) without manual casting.
    """
    global _cached_handle
    with _handle_lock:
        if _cached_handle is not None:
            return _cached_handle
        ffi = cffi.FFI()
        ffi.cdef(_CDEF)
        libcrypto = _open_one(ffi, _LIBCRYPTO_CANDIDATES)
        libssl = _open_one(ffi, _LIBSSL_CANDIDATES)
        _cached_handle = _LibHandle(ffi, libcrypto, libssl)
        return _cached_handle


def reset_for_test() -> None:
    """Drop the cached handle so tests can inject a mock via `monkeypatch`."""
    global _cached_handle
    with _handle_lock:
        _cached_handle = None


def drain_errors(handle: _LibHandle) -> str:
    """Drain OpenSSL's thread-local error queue into a single human-readable string.

    Without this, subsequent error messages would attribute prior failures to unrelated calls.
    """
    buf = handle.ffi.new("char[256]")
    parts: list[str] = []
    while True:
        code = handle.libcrypto.ERR_get_error()
        if not code:
            break
        handle.libcrypto.ERR_error_string_n(code, buf, 256)
        parts.append(handle.ffi.string(buf).decode("utf-8", errors="replace"))
    return "; ".join(parts) if parts else "(no OpenSSL error queued)"


_provider_load_lock = threading.Lock()
_loaded_providers: dict[str, Any] = {}


def load_provider(handle: _LibHandle, name: str, search_dir: str | None = None) -> None:
    """Load an OpenSSL provider into the default library context, once per process.

    `search_dir` (when set) is added to the default modules search path before the load attempt — useful
    for picking up providers installed outside `/usr/lib/.../ossl-modules`.
    """
    with _provider_load_lock:
        if name in _loaded_providers:
            return
        if search_dir is not None:
            rc = handle.libcrypto.OSSL_PROVIDER_set_default_search_path(handle.ffi.NULL, search_dir.encode())
            if rc != 1:
                logger.debug(
                    "OSSL_PROVIDER_set_default_search_path(%r) returned %s — provider lookup will use the "
                    "OpenSSL default modules dir instead",
                    search_dir,
                    rc,
                )

        prov = handle.libcrypto.OSSL_PROVIDER_load(handle.ffi.NULL, name.encode())
        if prov == handle.ffi.NULL:
            raise OpenSSLProviderError(
                f"OSSL_PROVIDER_load({name!r}) returned NULL. Install the provider's shared library where "
                f"libcrypto can find it (typically <prefix>/lib/ossl-modules), or set the search dir "
                f"explicitly. OpenSSL error: {drain_errors(handle)}"
            )
        _loaded_providers[name] = prov


def load_cert_and_key(handle: _LibHandle, uri: str) -> tuple[Any, Any]:
    """Open an `OSSL_STORE` against `uri`, walk it, and return the first (cert, key) pair found.

    On any failure or partial result, frees what we hold and raises `OpenSSLProviderError`. Cert and key
    cdata are owned by the caller and must be freed with `EVP_PKEY_free` / `X509_free` after install (or
    on error).
    """
    null = handle.ffi.NULL
    store = handle.libcrypto.OSSL_STORE_open(uri.encode(), null, null, null, null)
    if store == null:
        raise OpenSSLProviderError(f"OSSL_STORE_open failed: {drain_errors(handle)}")

    pkey = null
    cert = null
    try:
        while not handle.libcrypto.OSSL_STORE_eof(store):
            info = handle.libcrypto.OSSL_STORE_load(store)
            if info == null:
                if handle.libcrypto.OSSL_STORE_eof(store):
                    break
                raise OpenSSLProviderError(f"OSSL_STORE_load failed before EOF: {drain_errors(handle)}")
            try:
                info_type = handle.libcrypto.OSSL_STORE_INFO_get_type(info)
                if info_type == _OSSL_STORE_INFO_PKEY and pkey == null:
                    pkey = handle.libcrypto.OSSL_STORE_INFO_get1_PKEY(info)
                elif info_type == _OSSL_STORE_INFO_CERT and cert == null:
                    cert = handle.libcrypto.OSSL_STORE_INFO_get1_CERT(info)
            finally:
                handle.libcrypto.OSSL_STORE_INFO_free(info)
            # Stop the walk as soon as we have a usable pair — every subsequent OSSL_STORE_load is wasted
            # work that still talks to the smartcard.
            if pkey != null and cert != null:  # noqa: PLR1714 - cdata isn't hashable, can't use `in`
                break
    finally:
        handle.libcrypto.OSSL_STORE_close(store)

    if pkey == null or cert == null:  # noqa: PLR1714 - cdata isn't hashable, can't use `in`
        if pkey != null:
            handle.libcrypto.EVP_PKEY_free(pkey)
        if cert != null:
            handle.libcrypto.X509_free(cert)
        raise OpenSSLProviderError(
            f"OSSL_STORE walk did not yield both a private key and a certificate. "
            f"OpenSSL errors: {drain_errors(handle)}"
        )
    return cert, pkey


def install_on_ssl_context(handle: _LibHandle, ssl_ctx_cdata: Any, cert: Any, pkey: Any) -> None:
    """Install cert + key onto an SSL_CTX (passed as the same FFI's cdata), verify the pair matches, then
    free our local references.

    `ssl_ctx_cdata` is the `SSL_CTX*` cdata from pyOpenSSL's `Context._context`, cast into this FFI's
    pointer type.
    """
    try:
        if handle.libssl.SSL_CTX_use_certificate(ssl_ctx_cdata, cert) != 1:
            raise OpenSSLProviderError(f"SSL_CTX_use_certificate failed: {drain_errors(handle)}")
        if handle.libssl.SSL_CTX_use_PrivateKey(ssl_ctx_cdata, pkey) != 1:
            raise OpenSSLProviderError(f"SSL_CTX_use_PrivateKey failed: {drain_errors(handle)}")
        if handle.libssl.SSL_CTX_check_private_key(ssl_ctx_cdata) != 1:
            raise OpenSSLProviderError(
                f"SSL_CTX_check_private_key failed — the token returned a private key that does not "
                f"match the certificate. OpenSSL error: {drain_errors(handle)}"
            )
    finally:
        # SSL_CTX_use_* bumps the refcount on success, so freeing our local ref is correct in both
        # branches.
        handle.libcrypto.EVP_PKEY_free(pkey)
        handle.libcrypto.X509_free(cert)


def cast_pyopenssl_ssl_ctx(handle: _LibHandle, pyopenssl_context: Any) -> Any:
    """Reinterpret pyOpenSSL's `Context._context` cdata as this FFI's `SSL_CTX*`.

    Both FFIs are ABI-compatible (they both bind to the same libssl symbols), but cdata is FFI-scoped, so
    we round-trip through an integer address. This is the one place we couple to a pyOpenSSL internal —
    `_context` has been a public-ish attribute since pyOpenSSL 0.13 and is documented in the project as a
    stable hook for native interop.
    """
    raw_addr = int(pyopenssl_context._ffi.cast("uintptr_t", pyopenssl_context._context))
    return handle.ffi.cast("SSL_CTX *", raw_addr)


def cast_pyopenssl_ssl(handle: _LibHandle, pyopenssl_connection: Any) -> Any:
    """Reinterpret pyOpenSSL's `Connection._ssl` cdata as this FFI's `SSL*`."""
    raw_addr = int(pyopenssl_connection._ffi.cast("uintptr_t", pyopenssl_connection._ssl))
    return handle.ffi.cast("SSL *", raw_addr)


def configure_hostname_verification(handle: _LibHandle, ssl_cdata: Any, host: str) -> None:
    """Tell OpenSSL to verify the peer certificate against `host` during handshake.

    pyOpenSSL exposes chain verification, but not the hostname/IP helpers on `X509_VERIFY_PARAM`. Without this
    call a valid certificate for another hostname would be accepted.
    """
    import ipaddress

    normalized_host = host.removeprefix("[").removesuffix("]")
    param = handle.libssl.SSL_get0_param(ssl_cdata)
    if param == handle.ffi.NULL:
        raise OpenSSLProviderError("SSL_get0_param returned NULL; cannot configure hostname verification")

    try:
        ipaddress.ip_address(normalized_host)
    except ValueError:
        encoded = normalized_host.encode("idna")
        if handle.libcrypto.X509_VERIFY_PARAM_set1_host(param, encoded, len(encoded)) != 1:
            raise OpenSSLProviderError(
                f"X509_VERIFY_PARAM_set1_host({normalized_host!r}) failed: {drain_errors(handle)}"
            )
    else:
        if handle.libcrypto.X509_VERIFY_PARAM_set1_ip_asc(param, normalized_host.encode("ascii")) != 1:
            raise OpenSSLProviderError(
                f"X509_VERIFY_PARAM_set1_ip_asc({normalized_host!r}) failed: {drain_errors(handle)}"
            )


def assert_verify_ok(handle: _LibHandle, ssl_cdata: Any, host: str) -> None:
    """Fail if OpenSSL did not verify the server certificate chain and hostname."""
    result = handle.libssl.SSL_get_verify_result(ssl_cdata)
    if result != _X509_V_OK:
        raise OpenSSLProviderError(
            f"TLS peer certificate verification failed for {host!r}; OpenSSL verify result={result}. "
            f"OpenSSL error: {drain_errors(handle)}"
        )


def _c_function_addr(ffi: cffi.FFI, lib: Any, symbol: str) -> int | None:
    try:
        return int(ffi.cast("uintptr_t", getattr(lib, symbol)))
    except Exception:  # pragma: no cover - defensive against pyOpenSSL/cryptography internals changing
        logger.debug("Could not resolve function pointer for %s", symbol, exc_info=True)
        return None


def validate_pyopenssl_context(handle: _LibHandle, pyopenssl_context: Any) -> None:
    """Fail fast if our dlopen'd libssl is visibly different from pyOpenSSL's libssl.

    Passing pyOpenSSL-owned `SSL_CTX*` pointers to functions from another OpenSSL build is undefined
    behavior. When pyOpenSSL exposes the same function symbols through its binding, compare addresses before
    we cast and install provider-loaded key material.
    """
    try:
        from OpenSSL import _util as pyopenssl_util
    except Exception as e:  # pragma: no cover - pyOpenSSL import is already guarded by caller
        raise OpenSSLProviderError("Could not inspect pyOpenSSL's OpenSSL binding") from e

    checks = ("SSL_CTX_use_PrivateKey", "SSL_CTX_use_certificate", "SSL_CTX_check_private_key")
    for symbol in checks:
        ours = _c_function_addr(handle.ffi, handle.libssl, symbol)
        theirs = _c_function_addr(pyopenssl_context._ffi, pyopenssl_util.lib, symbol)
        if ours is not None and theirs is not None and ours != theirs:
            raise OpenSSLProviderError(
                "pyOpenSSL is bound to a different libssl than the one opened for smartcard provider calls. "
                "Refusing to pass SSL_CTX pointers across OpenSSL library boundaries."
            )


__all__ = [
    "OpenSSLProviderError",
    "assert_verify_ok",
    "cast_pyopenssl_ssl_ctx",
    "cast_pyopenssl_ssl",
    "configure_hostname_verification",
    "drain_errors",
    "install_on_ssl_context",
    "lib_handle",
    "load_cert_and_key",
    "load_provider",
    "reset_for_test",
    "validate_pyopenssl_context",
]