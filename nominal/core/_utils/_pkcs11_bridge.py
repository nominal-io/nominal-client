"""cffi bridge that lets us drive a smartcard / CAC for TLS without an OpenSSL provider.

Background
----------
The legacy approach (libp11 / `pkcs11prov` + `OSSL_STORE`) needs a system-level OpenSSL provider
installed where libcrypto can find it. That's a non-trivial install footprint to ask users for. This
module replaces that path with a self-contained cffi bridge: PyKCS11 talks to the card from Python,
and we install a custom `EVP_PKEY_METHOD` whose sign callback delegates back to PyKCS11. The private
key never leaves the card. The only deps are `PyKCS11`, `pyOpenSSL`, and `cffi` (the latter is a
transitive dependency of `cryptography`, already in `nominal[smartcard]`).

Design notes
------------
- We use the OpenSSL 3.x legacy `EVP_PKEY_METHOD` dispatch (`EVP_PKEY_meth_new` /
  `EVP_PKEY_meth_set_sign` / `EVP_PKEY_meth_add0`). For legacy EVP_PKEYs (those created via
  `EVP_PKEY_set1_RSA`), libssl routes the TLS CertificateVerify signing call through this method
  rather than the provider system.
- `EVP_PKEY_meth_add0` is a process-global override. To avoid breaking other RSA operations in the
  process, we look up the default RSA method via `EVP_PKEY_meth_find`, copy it with
  `EVP_PKEY_meth_copy`, and override ONLY the `sign` function pointer. Every other RSA op
  (sign_init, verify, encrypt, decrypt, derive) keeps the built-in implementation.
- The `_sign` callback is defined at module level so its cffi cdata function pointer has a stable
  lifetime — if it were GC'd, OpenSSL would call a dangling pointer mid-handshake.
- The bridge holds a Python-level sign callable in module-level state. The cffi callback cannot
  capture closures (it's a C function pointer), so the SmartcardSession installs its bound `_sign`
  method into module-level state before the EVP_PKEY ever reaches OpenSSL. Only one active key at a
  time — sufficient for the single-smartcard-per-process model.
- We deliberately do NOT call `SSL_CTX_check_private_key`. That check tries to derive the public key
  from the EVP_PKEY's private components and compare it to the cert's pubkey; our EVP_PKEY has no
  private material (it lives on the card), so the check would unconditionally fail. Any real
  cert/key mismatch surfaces during the actual TLS handshake when the card returns a bad signature.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

import cffi
from cryptography.hazmat.primitives.asymmetric import rsa as _crypto_rsa
from cryptography.x509 import load_der_x509_certificate

logger = logging.getLogger(__name__)

# EVP_PKEY type id for RSA. Stable across OpenSSL versions (it's an OpenSSL NID).
_EVP_PKEY_RSA = 6

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
typedef struct evp_pkey_method_st EVP_PKEY_METHOD;
typedef struct evp_pkey_ctx_st EVP_PKEY_CTX;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct x509_st X509;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct rsa_st RSA;
typedef struct bignum_st BIGNUM;

EVP_PKEY_METHOD *EVP_PKEY_meth_new(int id, int flags);
const EVP_PKEY_METHOD *EVP_PKEY_meth_find(int type);
void EVP_PKEY_meth_copy(EVP_PKEY_METHOD *dst, const EVP_PKEY_METHOD *src);
void EVP_PKEY_meth_set_sign(
    EVP_PKEY_METHOD *pmeth,
    int (*sign_init)(EVP_PKEY_CTX *ctx),
    int (*sign)(EVP_PKEY_CTX *ctx,
                unsigned char *sig, size_t *siglen,
                const unsigned char *tbs, size_t tbslen)
);
int EVP_PKEY_meth_add0(const EVP_PKEY_METHOD *pmeth);

EVP_PKEY *EVP_PKEY_new(void);
void EVP_PKEY_free(EVP_PKEY *pkey);
int EVP_PKEY_set1_RSA(EVP_PKEY *pkey, RSA *rsa);

RSA *RSA_new(void);
void RSA_free(RSA *rsa);
int RSA_set0_key(RSA *r, BIGNUM *n, BIGNUM *e, BIGNUM *d);

BIGNUM *BN_new(void);
void BN_free(BIGNUM *a);
BIGNUM *BN_bin2bn(const unsigned char *s, int len, BIGNUM *ret);

int SSL_CTX_use_PrivateKey(SSL_CTX *ctx, EVP_PKEY *pkey);
int SSL_CTX_use_certificate(SSL_CTX *ctx, X509 *x);
void X509_free(X509 *x);

unsigned long ERR_get_error(void);
void ERR_error_string_n(unsigned long e, char *buf, size_t len);
"""


class PKCS11BridgeError(RuntimeError):
    """Raised for any FFI-level failure that prevents the smartcard TLS path from completing."""


# A single module-level FFI instance is created at import time. Only `cdef` runs at import — `dlopen`
# is deferred to `lib_handle()` so importing this module never forces libcrypto to load.
_ffi = cffi.FFI()
_ffi.cdef(_CDEF)


class _LibHandle:
    """A pair of dlopen'd handles (libcrypto + libssl) sharing the module-level FFI."""

    def __init__(self, libcrypto: Any, libssl: Any) -> None:
        self.ffi = _ffi
        self.libcrypto = libcrypto
        self.libssl = libssl


_handle_lock = threading.Lock()
_cached_handle: _LibHandle | None = None

_method_lock = threading.Lock()
_method_registered = False

# State the sign callback reads. Set by `install_pkcs11_key` before the EVP_PKEY ever reaches OpenSSL.
# Only one active key at a time — sufficient for the single-smartcard-per-process model.
_active_lock = threading.Lock()
_sign_callable: Callable[[bytes], bytes] | None = None
_active_sig_len: int = 0


@_ffi.callback("int(EVP_PKEY_CTX *, unsigned char *, size_t *, const unsigned char *, size_t)")  # type: ignore[misc]
def _sign(ctx: Any, sig: Any, siglen: Any, tbs: Any, tbslen: int) -> int:
    """OpenSSL EVP_PKEY_METHOD sign callback. Routes the signing op to PyKCS11.

    OpenSSL calls this twice per signature in some code paths: first with `sig == NULL` to ask for the
    required buffer size, then with a real `sig` buffer. RSA-PKCS1 signature length equals the modulus
    byte length, which we cache in `_active_sig_len` when the key is installed.
    """
    with _active_lock:
        cb = _sign_callable
        sig_len = _active_sig_len
    if cb is None:
        return 0
    if sig == _ffi.NULL:
        siglen[0] = sig_len
        return 1
    if siglen[0] < sig_len:
        siglen[0] = sig_len
        return 0
    try:
        data = bytes(_ffi.buffer(tbs, tbslen))
        result = cb(data)
    except Exception:  # pragma: no cover - last-resort guard; cffi cannot propagate Python exceptions
        logger.exception("smartcard signing callback failed")
        return 0
    if len(result) > siglen[0]:
        return 0
    _ffi.buffer(sig, len(result))[:] = result
    siglen[0] = len(result)
    return 1


def _open_one(candidates: tuple[str, ...]) -> Any:
    last_err: Exception | None = None
    for name in candidates:
        try:
            return _ffi.dlopen(name)
        except OSError as e:
            last_err = e
    raise PKCS11BridgeError(
        f"Could not dlopen any of {candidates!r} — is OpenSSL 3.x installed? Last error: {last_err}"
    )


def lib_handle() -> _LibHandle:
    """Return a process-wide handle to libcrypto + libssl, opened by name on first use."""
    global _cached_handle
    with _handle_lock:
        if _cached_handle is not None:
            return _cached_handle
        libcrypto = _open_one(_LIBCRYPTO_CANDIDATES)
        libssl = _open_one(_LIBSSL_CANDIDATES)
        _cached_handle = _LibHandle(libcrypto, libssl)
        return _cached_handle


def reset_for_test() -> None:
    """Drop cached handle, registered-method flag, and active-key state so tests can re-stub."""
    global _cached_handle, _method_registered, _sign_callable, _active_sig_len
    with _handle_lock:
        _cached_handle = None
    with _method_lock:
        _method_registered = False
    with _active_lock:
        _sign_callable = None
        _active_sig_len = 0


def drain_errors(handle: _LibHandle) -> str:
    """Drain OpenSSL's thread-local error queue into a single human-readable string."""
    buf = handle.ffi.new("char[256]")
    parts: list[str] = []
    while True:
        code = handle.libcrypto.ERR_get_error()
        if not code:
            break
        handle.libcrypto.ERR_error_string_n(code, buf, 256)
        parts.append(handle.ffi.string(buf).decode("utf-8", errors="replace"))
    return "; ".join(parts) if parts else "(no OpenSSL error queued)"


def _ensure_method_registered(handle: _LibHandle) -> None:
    """Build the EVP_PKEY_METHOD with our sign callback once per process, then add it to OpenSSL's
    method registry.

    To avoid breaking other RSA operations in the process (sign_init step, verify on other RSA keys,
    etc.), we copy the default RSA method into our new struct and override ONLY the `sign` function
    pointer. Everything else — sign_init, verify, encrypt, decrypt, derive — keeps the default.
    """
    global _method_registered
    with _method_lock:
        if _method_registered:
            return
        default = handle.libcrypto.EVP_PKEY_meth_find(_EVP_PKEY_RSA)
        if default == handle.ffi.NULL:
            raise PKCS11BridgeError(
                f"EVP_PKEY_meth_find(EVP_PKEY_RSA) returned NULL — libcrypto has no RSA method "
                f"registered. {drain_errors(handle)}"
            )
        pmeth = handle.libcrypto.EVP_PKEY_meth_new(_EVP_PKEY_RSA, 0)
        if pmeth == handle.ffi.NULL:
            raise PKCS11BridgeError(f"EVP_PKEY_meth_new failed: {drain_errors(handle)}")
        handle.libcrypto.EVP_PKEY_meth_copy(pmeth, default)
        handle.libcrypto.EVP_PKEY_meth_set_sign(pmeth, handle.ffi.NULL, _sign)
        if handle.libcrypto.EVP_PKEY_meth_add0(pmeth) != 1:
            raise PKCS11BridgeError(f"EVP_PKEY_meth_add0 failed: {drain_errors(handle)}")
        _method_registered = True


def install_pkcs11_key(
    handle: _LibHandle, sign_callable: Callable[[bytes], bytes], cert_der: bytes
) -> Any:
    """Register the sign callback (once) and build an EVP_PKEY whose signing dispatches via
    `sign_callable`.

    Returns owned `EVP_PKEY*` cdata. The caller must hand it to `install_on_ssl_context`, which frees
    our reference after SSL_CTX takes its own. Also sets the module-level active-key state that
    `_sign` reads.

    Args:
        handle: lib_handle() result.
        sign_callable: `bytes -> bytes` performing the PKCS#1 v1.5 RSA signing op on the smartcard.
        cert_der: the X.509 certificate DER bytes (used to extract the public modulus + exponent).
    """
    global _sign_callable, _active_sig_len

    _ensure_method_registered(handle)

    cert = load_der_x509_certificate(cert_der)
    public_key = cert.public_key()
    if not isinstance(public_key, _crypto_rsa.RSAPublicKey):
        raise PKCS11BridgeError(
            f"Expected RSA public key on smartcard certificate, got {type(public_key).__name__}. "
            f"ECDSA / other algorithms are not yet supported by this bridge."
        )
    pub_numbers = public_key.public_numbers()
    n_int = pub_numbers.n
    e_int = pub_numbers.e
    n_bytes = n_int.to_bytes((n_int.bit_length() + 7) // 8, "big")
    e_bytes = e_int.to_bytes((e_int.bit_length() + 7) // 8, "big")

    null = handle.ffi.NULL
    bn_n = handle.libcrypto.BN_bin2bn(n_bytes, len(n_bytes), null)
    bn_e = handle.libcrypto.BN_bin2bn(e_bytes, len(e_bytes), null)
    if bn_n == null or bn_e == null:  # noqa: PLR1714 - cdata isn't hashable
        if bn_n != null:
            handle.libcrypto.BN_free(bn_n)
        if bn_e != null:
            handle.libcrypto.BN_free(bn_e)
        raise PKCS11BridgeError(f"BN_bin2bn failed: {drain_errors(handle)}")

    rsa = handle.libcrypto.RSA_new()
    if rsa == null:
        handle.libcrypto.BN_free(bn_n)
        handle.libcrypto.BN_free(bn_e)
        raise PKCS11BridgeError(f"RSA_new failed: {drain_errors(handle)}")
    # RSA_set0_key transfers ownership of bn_n and bn_e to rsa on success; d=NULL is legal (no priv).
    if handle.libcrypto.RSA_set0_key(rsa, bn_n, bn_e, null) != 1:
        handle.libcrypto.BN_free(bn_n)
        handle.libcrypto.BN_free(bn_e)
        handle.libcrypto.RSA_free(rsa)
        raise PKCS11BridgeError(f"RSA_set0_key failed: {drain_errors(handle)}")

    pkey = handle.libcrypto.EVP_PKEY_new()
    if pkey == null:
        handle.libcrypto.RSA_free(rsa)
        raise PKCS11BridgeError(f"EVP_PKEY_new failed: {drain_errors(handle)}")
    if handle.libcrypto.EVP_PKEY_set1_RSA(pkey, rsa) != 1:
        handle.libcrypto.EVP_PKEY_free(pkey)
        handle.libcrypto.RSA_free(rsa)
        raise PKCS11BridgeError(f"EVP_PKEY_set1_RSA failed: {drain_errors(handle)}")
    # set1 took its own reference; drop ours.
    handle.libcrypto.RSA_free(rsa)

    with _active_lock:
        _sign_callable = sign_callable
        _active_sig_len = len(n_bytes)

    return pkey


def cast_ssl_ctx(pyopenssl_context: Any) -> Any:
    """Reinterpret pyOpenSSL's `Context._context` cdata as our FFI's `SSL_CTX*`.

    pyOpenSSL has its own FFI instance; cdata is FFI-scoped, so we round-trip via integer address.
    `_context` has been a stable pyOpenSSL hook for native interop since 0.13.
    """
    raw_addr = int(pyopenssl_context._ffi.cast("uintptr_t", pyopenssl_context._context))
    return _ffi.cast("SSL_CTX *", raw_addr)


def _cast_pyopenssl_x509(handle: _LibHandle, pyopenssl_cert: Any) -> Any:
    """Reinterpret a pyOpenSSL `X509` wrapper's underlying cdata into our FFI's `X509*`."""
    raw_addr = int(pyopenssl_cert._ffi.cast("uintptr_t", pyopenssl_cert._x509))
    return handle.ffi.cast("X509 *", raw_addr)


def install_on_ssl_context(handle: _LibHandle, ssl_ctx_cdata: Any, cert: Any, pkey: Any) -> None:
    """Install cert + key onto an SSL_CTX, then drop our pkey reference.

    `cert` is a pyOpenSSL `X509` (owned by Python, not by us); `pkey` is the cdata returned by
    `install_pkcs11_key` (owned by us until SSL_CTX takes its reference).

    We deliberately do NOT call `SSL_CTX_check_private_key` here — see the module docstring for why.
    """
    x509_cdata = _cast_pyopenssl_x509(handle, cert)
    try:
        if handle.libssl.SSL_CTX_use_certificate(ssl_ctx_cdata, x509_cdata) != 1:
            raise PKCS11BridgeError(f"SSL_CTX_use_certificate failed: {drain_errors(handle)}")
        if handle.libssl.SSL_CTX_use_PrivateKey(ssl_ctx_cdata, pkey) != 1:
            raise PKCS11BridgeError(f"SSL_CTX_use_PrivateKey failed: {drain_errors(handle)}")
    finally:
        # SSL_CTX_use_PrivateKey bumps the refcount on success; freeing our local ref is correct in
        # both branches. The cert wrapper is still owned by Python — do not free it here.
        handle.libcrypto.EVP_PKEY_free(pkey)


__all__ = [
    "PKCS11BridgeError",
    "cast_ssl_ctx",
    "drain_errors",
    "install_on_ssl_context",
    "install_pkcs11_key",
    "lib_handle",
    "reset_for_test",
]
