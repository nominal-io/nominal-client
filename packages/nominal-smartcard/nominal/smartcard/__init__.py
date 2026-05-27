"""Smartcard / PIV authentication for the Nominal Python client.

All functionality is consolidated in this single module for environments where
copying individual files is impractical. Public API is at the bottom in __all__.
"""
from __future__ import annotations

import ctypes
import getpass
import os
import platform
import ssl
import sys
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cffi as _cffi_module
import grpc.experimental
import pkcs11
import pkcs11.exceptions as _pkcs11_exc
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import ExtendedKeyUsageOID
from grpc.experimental import ssl_channel_credentials_with_custom_signer
from pkcs11 import ObjectClass
from pkcs11.mechanisms import MGF, Mechanism

from nominal.core._utils.networking import SslContextProvider
from nominal.core.exceptions import NominalError

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SmartcardError(NominalError):
    """Base class for smartcard authentication errors."""


class SmartcardConfigurationError(SmartcardError):
    """Smartcard configuration or local machine setup is invalid."""


class SmartcardProviderError(SmartcardError):
    """The PKCS#11 provider returned an unexpected error that could not be classified."""


class SmartcardCertificateSelectionError(SmartcardError):
    """The PIV Authentication certificate could not be selected deterministically."""


class SmartcardPinError(SmartcardError):
    """The PIN was rejected by the smartcard."""


class SmartcardPinLockedError(SmartcardPinError):
    """The PIN is locked due to too many incorrect attempts."""


# ---------------------------------------------------------------------------
# Certificate selection
# ---------------------------------------------------------------------------

"""Slot 9A is reserved for PIV Authentication keys on the smartcard."""
PIV_AUTHENTICATION_SLOT = "9A"


@dataclass(frozen=True)
class CertificateCandidate:
    """A certificate/key pair discovered on a PKCS#11 token."""

    label: str | None
    slot: str | None
    certificate_uri: str
    private_key_uri: str
    der_certificate: bytes = b""
    token_label: str = ""
    object_id_bytes: bytes | None = None

    @property
    def is_piv_authentication_candidate(self) -> bool:
        return self.slot is not None and self.slot.upper() == PIV_AUTHENTICATION_SLOT


def _assert_client_auth_eku(candidate: CertificateCandidate) -> None:
    """Raise SmartcardCertificateSelectionError if the certificate lacks clientAuth EKU.

    RFC 5280 and TLS 1.3 (RFC 8446 §4.4.2.1) require id-kp-clientAuth
    (OID 1.3.6.1.5.5.7.3.2) for certificates used in client authentication.
    A server that enforces EKU will reject a cert missing this OID; catching
    it here produces a clear diagnostic instead of a cryptic TLS handshake failure.
    """
    if not candidate.der_certificate:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} has no DER data; "
            "cannot verify ExtendedKeyUsage."
        )
    cert = x509.load_der_x509_certificate(candidate.der_certificate)
    try:
        eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
    except x509.ExtensionNotFound:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} in PIV Authentication slot "
            "has no ExtendedKeyUsage extension and cannot be used for client authentication."
        )
    if ExtendedKeyUsageOID.CLIENT_AUTH not in eku.value:
        raise SmartcardCertificateSelectionError(
            f"Certificate {candidate.label or candidate.certificate_uri!r} in PIV Authentication slot "
            "does not include clientAuth (OID 1.3.6.1.5.5.7.3.2) in its ExtendedKeyUsage."
        )


def select_piv_authentication_certificate(
    candidates: list[CertificateCandidate],
) -> CertificateCandidate:
    """Select the PIV Authentication cert/key pair from discovered candidates."""
    if not candidates:
        raise SmartcardCertificateSelectionError("No certificates were found on the smartcard token.")

    piv_auth_candidates = [candidate for candidate in candidates if candidate.is_piv_authentication_candidate]
    if len(piv_auth_candidates) == 1:
        _assert_client_auth_eku(piv_auth_candidates[0])
        return piv_auth_candidates[0]

    if not piv_auth_candidates:
        raise SmartcardCertificateSelectionError(
            "Could not find a PIV Authentication certificate on the smartcard token."
        )

    labels = ", ".join(candidate.label or candidate.certificate_uri for candidate in piv_auth_candidates)
    raise SmartcardCertificateSelectionError(f"Multiple PIV Authentication certificate candidates were found: {labels}")


# ---------------------------------------------------------------------------
# PKCS#11 module discovery and backend
# ---------------------------------------------------------------------------

NOMINAL_PKCS11_MODULE_ENV_VAR = "NOMINAL_PKCS11_MODULE"

_LINUX_OPENSC_PATHS = (
    "/usr/lib64/opensc-pkcs11.so",
    "/usr/lib/x86_64-linux-gnu/opensc-pkcs11.so",
    "/usr/lib/aarch64-linux-gnu/opensc-pkcs11.so",
    "/usr/lib/opensc-pkcs11.so",
)
_MACOS_OPENSC_PATHS = (
    "/Library/OpenSC/lib/opensc-pkcs11.so",
    "/opt/homebrew/lib/opensc-pkcs11.so",
    "/usr/local/lib/opensc-pkcs11.so",
)
_WINDOWS_OPENSC_PATHS = (
    r"C:\Program Files\OpenSC Project\OpenSC\pkcs11\opensc-pkcs11.dll",
    r"C:\Program Files (x86)\OpenSC Project\OpenSC\pkcs11\opensc-pkcs11.dll",
)

# Maps PKCS#11 CKA_ID (hex string) to PIV key reference slot label.
# Per NIST SP 800-73-4 and OpenSC conventions.
_OBJECT_ID_TO_PIV_SLOT: dict[str, str] = {
    "01": PIV_AUTHENTICATION_SLOT,  # PIV Authentication
    "02": "9C",  # Digital Signature
    "03": "9D",  # Key Management
    "04": "9E",  # Card Authentication
}


def discover_pkcs11_module() -> Path:
    """Find the OpenSC PKCS#11 module used to communicate with the smartcard."""
    env_path = os.environ.get(NOMINAL_PKCS11_MODULE_ENV_VAR)
    configured_path = Path(env_path) if env_path else None

    if configured_path is not None:
        module_path = configured_path.expanduser()
        if module_path.exists():
            return module_path
        raise SmartcardConfigurationError(f"Configured PKCS#11 module does not exist: {module_path}")

    for candidate in _platform_default_paths():
        path = Path(candidate)
        if path.exists():
            return path

    raise SmartcardConfigurationError(
        "Could not find an OpenSC PKCS#11 module. Install OpenSC or set "
        f"{NOMINAL_PKCS11_MODULE_ENV_VAR} to the module path."
    )


def _platform_default_paths() -> tuple[str, ...]:
    system = platform.system()
    if system == "Darwin":
        return _MACOS_OPENSC_PATHS
    if system == "Windows":
        return _WINDOWS_OPENSC_PATHS
    if system == "Linux":
        return _LINUX_OPENSC_PATHS

    raise SmartcardConfigurationError(f"Unsupported platform: {system}")


class Pkcs11Backend(ABC):
    """Backend responsible for direct PKCS#11 token discovery."""

    def __init__(self, module_path: Path) -> None:
        self.module_path = module_path

    @abstractmethod
    def list_certificate_candidates(self) -> list[CertificateCandidate]: ...

    @abstractmethod
    def close(self) -> None: ...


# Characters allowed unencoded in a pk11-pchar value (RFC 7512 §2.3):
# unreserved (RFC 3986) + ":" / "[" / "]" / "@" / "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / ","
# Notably absent: ";" (path separator), "=" (name-value separator), "%" (must be part of pct-encoded).
_PK11_PCHAR_SAFE: frozenset[str] = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:[]@!$&'()*+,"
)


def _pct_encode_pk11_pchar(value: str) -> str:
    """Percent-encode every character outside the pk11-pchar set (RFC 7512 §2.3)."""
    parts = []
    for ch in value:
        if ch in _PK11_PCHAR_SAFE:
            parts.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                parts.append(f"%{byte:02x}")
    return "".join(parts)


def _build_pkcs11_uri(token_label: str, object_id_bytes: bytes, *, object_type: str | None = None) -> str:
    """Build a PKCS#11 URI for a token + object identifier.

    Format: pkcs11:token=TOKEN_LABEL;id=%XX%YY...;type=OBJECT_TYPE
    Both the token label and the id bytes are percent-encoded per RFC 7512.
    """
    pct_id = "".join(f"%{b:02x}" for b in object_id_bytes)
    pct_label = _pct_encode_pk11_pchar(token_label)
    uri = f"pkcs11:token={pct_label};id={pct_id}"
    if object_type is not None:
        uri += f";type={object_type}"
    return uri


class DefaultPkcs11Backend(Pkcs11Backend):
    """PKCS#11 token backend backed by the python-pkcs11 library."""

    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)
        self._lib: Any = None

    def _get_lib(self) -> Any:
        if self._lib is not None:
            return self._lib

        try:
            lib = pkcs11.lib(str(self.module_path))
        except Exception as e:
            raise SmartcardConfigurationError(f"Failed to load PKCS#11 module {self.module_path}: {e}") from e

        self._lib = lib
        return lib

    def list_certificate_candidates(self) -> list[CertificateCandidate]:
        lib = self._get_lib()

        try:
            slots = lib.get_slots(token_present=True)
        except pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        candidates: list[CertificateCandidate] = []
        for slot in slots:
            try:
                token = slot.get_token()
                token_label = token.label.strip()
                with token.open() as session:
                    for cert_obj in session.get_objects(
                        {
                            pkcs11.Attribute.CLASS: pkcs11.ObjectClass.CERTIFICATE,
                            pkcs11.Attribute.CERTIFICATE_TYPE: pkcs11.CertificateType.X_509,
                        }
                    ):
                        try:
                            label_raw = cert_obj[pkcs11.Attribute.LABEL]
                            label = label_raw.strip() if isinstance(label_raw, str) else None
                        except pkcs11.exceptions.PKCS11Error:
                            label = None

                        try:
                            object_id_bytes = cert_obj[pkcs11.Attribute.ID]
                            if not isinstance(object_id_bytes, (bytes, bytearray)):
                                object_id_bytes = bytes(object_id_bytes)
                        except pkcs11.exceptions.PKCS11Error:
                            object_id_bytes = None

                        try:
                            der_certificate = cert_obj[pkcs11.Attribute.VALUE]
                            if not isinstance(der_certificate, (bytes, bytearray)):
                                der_certificate = bytes(der_certificate)
                        except pkcs11.exceptions.PKCS11Error:
                            der_certificate = b""

                        if object_id_bytes is None:
                            continue

                        object_id_str = object_id_bytes.hex()
                        piv_slot = _OBJECT_ID_TO_PIV_SLOT.get(object_id_str)
                        certificate_uri = _build_pkcs11_uri(token_label, object_id_bytes, object_type="cert")
                        private_key_uri = _build_pkcs11_uri(token_label, object_id_bytes, object_type="private")

                        candidates.append(
                            CertificateCandidate(
                                label=label,
                                slot=piv_slot,
                                certificate_uri=certificate_uri,
                                private_key_uri=private_key_uri,
                                der_certificate=der_certificate,
                                token_label=token_label,
                                object_id_bytes=object_id_bytes,
                            )
                        )
            except pkcs11.exceptions.PKCS11Error:
                pass  # Skip slots we can't access or that don't conform to expected structure

        return candidates

    def close(self) -> None:
        self._lib = None


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SmartcardSession:
    """Selected smartcard module and PIV Authentication certificate."""

    module_path: Path
    certificate: CertificateCandidate

    @property
    def certificate_uri(self) -> str:
        return self.certificate.certificate_uri

    @property
    def private_key_uri(self) -> str:
        return self.certificate.private_key_uri


class SmartcardSessionManager:
    """Create and cache selected smartcard certificate metadata.

    One manager discovers the PKCS#11 certificate at most once. The shared manager gives profile-created clients the
    desired process-wide behavior while tests and other callers can still inject a dedicated manager.
    """

    _shared_lock = threading.Lock()
    _shared_manager: SmartcardSessionManager | None = None

    def __init__(
        self,
        *,
        backend_factory: Callable[[Path], Pkcs11Backend] = DefaultPkcs11Backend,
    ) -> None:
        self._backend_factory = backend_factory
        self._lock = threading.Lock()
        self._session: SmartcardSession | None = None

    @classmethod
    def shared(cls) -> SmartcardSessionManager:
        with cls._shared_lock:
            if cls._shared_manager is None:
                cls._shared_manager = cls()
            return cls._shared_manager

    def get_session(self) -> SmartcardSession:
        with self._lock:
            if self._session is None:
                self._session = self._open_session()
            return self._session

    def close(self) -> None:
        with self._lock:
            self._session = None

    def _open_session(self) -> SmartcardSession:
        module_path = discover_pkcs11_module()
        backend = self._backend_factory(module_path)
        try:
            certificate = select_piv_authentication_certificate(
                backend.list_certificate_candidates(),
            )
        finally:
            backend.close()
        return SmartcardSession(module_path=module_path, certificate=certificate)


# ---------------------------------------------------------------------------
# OpenSSL provider bridge (cffi)
# ---------------------------------------------------------------------------

# OSSL_STORE_INFO_get_type returns this value for private keys.
_OSSL_STORE_INFO_PKEY = 4
_PROVIDER_NAME = "pkcs11"
_OPENSSL_VALIDATION_SYMBOLS = (
    "SSL_CTX_check_private_key",  # libssl
    "OSSL_PROVIDER_load",  # libcrypto, OpenSSL 3 provider API
    "ERR_get_error",  # libcrypto error queue
)
_CKR_PIN_LOCKED = "CKR_PIN_LOCKED"
_CKR_PIN_INCORRECT = "CKR_PIN_INCORRECT"

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

    This matters most on macOS: ``dlopen(None)`` may resolve libssl symbols from
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

        ffi = _cffi_module.FFI()
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


def _ensure_provider_loaded(ffi: Any, lib: Any) -> None:
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
    try:
        store = lib.OSSL_STORE_open(uri_buf, ffi.NULL, ffi.NULL, ffi.NULL, ffi.NULL)
    finally:
        ffi.buffer(uri_buf)[:] = bytes(len(uri_bytes) + 1)

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
    x509_obj = lib.d2i_X509(ffi.NULL, ptr, len(der_cert))
    if x509_obj == ffi.NULL:
        err = _get_openssl_error(ffi, lib)
        raise SmartcardConfigurationError(f"Failed to parse DER certificate: {err}")
    return x509_obj


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
        _ensure_provider_loaded(ffi, lib)

        pkey = ffi.NULL
        x509_cert = ffi.NULL
        try:
            pkey = _load_pkey_from_store(ffi, lib, session.private_key_uri)
            x509_cert = _load_x509_from_der(ffi, lib, session.certificate.der_certificate)

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

            if lib.SSL_CTX_use_certificate(raw_ctx, x509_cert) != 1:
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
            if x509_cert != ffi.NULL:
                lib.X509_free(x509_cert)

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


# ---------------------------------------------------------------------------
# gRPC signer
# ---------------------------------------------------------------------------

_Algorithm = grpc.experimental.PrivateKeySignatureAlgorithm

_ECDSA_ALGORITHMS: frozenset[grpc.experimental.PrivateKeySignatureAlgorithm] = frozenset(
    {
        _Algorithm.ECDSA_SECP256R1_SHA256,
        _Algorithm.ECDSA_SECP384R1_SHA384,
        _Algorithm.ECDSA_SECP521R1_SHA512,
    }
)


def _prompt_for_pin(prompt: str) -> str:
    return getpass.getpass(prompt)


# Session-invalidating PKCS#11 errors that warrant clearing cached session state so that
# the next sign() attempt can re-establish a fresh session (e.g. after card removal/reinsert).
_SESSION_INVALIDATING_ERRORS: tuple[str, ...] = (
    "DeviceRemoved",
    "TokenNotPresent",
    "SessionHandleInvalid",
    "SessionClosed",
)

# Mapping from PrivateKeySignatureAlgorithm → (pkcs11.Mechanism, mechanism_param).
# mechanism_param for RSA PSS is (hash_mechanism, mgf, salt_length) per python-pkcs11 conventions.
# TLS 1.3 mandates salt length == hash length (RFC 8446 §4.2.3).
# For all other mechanisms, mechanism_param is None.
_MECHANISM_TABLE: dict[grpc.experimental.PrivateKeySignatureAlgorithm, tuple[Mechanism, Any]] = {
    _Algorithm.RSA_PKCS1_SHA256: (Mechanism.SHA256_RSA_PKCS, None),
    _Algorithm.RSA_PKCS1_SHA384: (Mechanism.SHA384_RSA_PKCS, None),
    _Algorithm.RSA_PKCS1_SHA512: (Mechanism.SHA512_RSA_PKCS, None),
    _Algorithm.RSA_PSS_RSAE_SHA256: (Mechanism.SHA256_RSA_PKCS_PSS, (Mechanism.SHA256, MGF.SHA256, 32)),
    _Algorithm.RSA_PSS_RSAE_SHA384: (Mechanism.SHA384_RSA_PKCS_PSS, (Mechanism.SHA384, MGF.SHA384, 48)),
    _Algorithm.RSA_PSS_RSAE_SHA512: (Mechanism.SHA512_RSA_PKCS_PSS, (Mechanism.SHA512, MGF.SHA512, 64)),
    _Algorithm.ECDSA_SECP256R1_SHA256: (Mechanism.ECDSA_SHA256, None),
    _Algorithm.ECDSA_SECP384R1_SHA384: (Mechanism.ECDSA_SHA384, None),
    _Algorithm.ECDSA_SECP521R1_SHA512: (Mechanism.ECDSA_SHA512, None),
}


def _encode_ecdsa_der(raw_sig: bytes) -> bytes:
    """Convert a PKCS#11 raw ECDSA signature (r||s big-endian, equal halves) to DER ASN.1.

    BoringSSL expects DER-encoded SEQUENCE { INTEGER r, INTEGER s } in the TLS CertificateVerify
    message. PKCS#11 returns the two integers as equal-length concatenated big-endian byte strings.
    """
    if len(raw_sig) == 0 or len(raw_sig) % 2 != 0:
        raise SmartcardConfigurationError(
            f"Unexpected ECDSA signature length {len(raw_sig)}; expected a non-empty even number of bytes."
        )
    half = len(raw_sig) // 2
    r = int.from_bytes(raw_sig[:half], "big")
    s = int.from_bytes(raw_sig[half:], "big")
    return encode_dss_signature(r, s)


class SmartcardPrivateKeySigner:
    """PKCS#11 signing callback for gRPC's custom signer TLS credentials.

    Holds a persistent PKCS#11 session with C_Login state for the lifetime of the
    associated gRPC channel. The private key never leaves the card; the only output is
    the signature produced by the token during each TLS handshake.

    Pass ``signer.sign`` as ``private_key_sign_fn`` to
    ``grpc.experimental.ssl_channel_credentials_with_custom_signer``.

    The PIN is retained in memory until :meth:`close` is called, enabling automatic
    session recovery if the card is briefly removed and reinserted.
    """

    def __init__(
        self,
        *,
        module_path: Path,
        token_label: str,
        object_id_bytes: bytes,
        pin_provider: Callable[[str], str] | None = None,
    ) -> None:
        self._module_path = module_path
        self._token_label = token_label
        self._object_id_bytes = object_id_bytes
        self._pin_provider = pin_provider
        self._session: Any = None
        self._key: Any = None
        self._lock = threading.Lock()

    def _ensure_session_and_key(self) -> tuple[Any, Any]:
        """Open a PKCS#11 session, log in, and locate the private key object.

        Idempotent: returns cached (session, key) after first successful call.
        Must be called under self._lock.
        """
        if self._session is not None:
            return self._session, self._key

        try:
            lib = pkcs11.lib(str(self._module_path))
        except Exception as e:
            raise SmartcardConfigurationError(f"Failed to load PKCS#11 module {self._module_path}: {e}") from e

        try:
            slots = lib.get_slots(token_present=True)
        except _pkcs11_exc.PKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        token = None
        for slot in slots:
            try:
                t = slot.get_token()
                if t.label.strip() == self._token_label:
                    token = t
                    break
            except _pkcs11_exc.PKCS11Error:
                continue

        if token is None:
            raise SmartcardConfigurationError(
                f"PKCS#11 token {self._token_label!r} not found. "
                "Verify the smartcard is inserted and the token label is correct."
            )

        pin_fn = self._pin_provider if self._pin_provider is not None else _prompt_for_pin
        try:
            session = token.open(user_pin=pin_fn("Card PIN: "))
        except _pkcs11_exc.PinIncorrect:
            raise SmartcardPinError(f"Incorrect PIN for token {self._token_label!r}.") from None
        except _pkcs11_exc.PinLocked:
            raise SmartcardPinLockedError(
                f"PIN is locked for token {self._token_label!r}. Too many incorrect attempts have been made."
            ) from None
        except _pkcs11_exc.PKCS11Error as e:
            raise SmartcardConfigurationError(
                f"Failed to open PKCS#11 session on token {self._token_label!r}: {e}"
            ) from e

        try:
            key = session.get_key(object_class=ObjectClass.PRIVATE_KEY, id=self._object_id_bytes)
        except Exception as e:
            session.close()
            raise SmartcardConfigurationError(
                f"Private key with id={self._object_id_bytes.hex()!r} not found on token {self._token_label!r}: {e}"
            ) from e

        self._session = session
        self._key = key
        return session, key

    def sign(
        self,
        data_to_sign: bytes,
        signature_algorithm: grpc.experimental.PrivateKeySignatureAlgorithm,
        on_complete: Any,
    ) -> bytes:
        """Sign ``data_to_sign`` on the smartcard and return raw signature bytes.

        This is the synchronous form of the gRPC ``CustomPrivateKeySign`` callback.
        ``on_complete`` is intentionally unused — gRPC only calls it for the async form
        (where the function returns a cancel callable instead of bytes).

        Raises ``SmartcardConfigurationError`` on PKCS#11 errors, which gRPC treats as a
        TLS handshake failure.
        """
        entry = _MECHANISM_TABLE.get(signature_algorithm)
        if entry is None:
            raise SmartcardConfigurationError(
                f"Unsupported TLS signature algorithm {signature_algorithm!r}. "
                "The smartcard signer supports RSA PKCS#1, RSA PSS, and ECDSA with SHA-256/384/512 "
                "over TLS 1.3."
            )
        mechanism, mechanism_param = entry

        with self._lock:
            _session, key = self._ensure_session_and_key()
            try:
                raw_sig: bytes = key.sign(data_to_sign, mechanism=mechanism, mechanism_param=mechanism_param)
            except _pkcs11_exc.PKCS11Error as e:
                # Clear the cached session if the card was removed or the session became invalid,
                # so the next sign() attempt can re-establish a fresh authenticated session.
                if type(e).__name__ in _SESSION_INVALIDATING_ERRORS:
                    self._session = None
                    self._key = None
                raise SmartcardConfigurationError(f"PKCS#11 signing failed ({signature_algorithm!r}): {e}") from e

        # PKCS#11 ECDSA returns raw r||s bytes; gRPC/BoringSSL expects DER-encoded ASN.1.
        if signature_algorithm in _ECDSA_ALGORITHMS:
            return _encode_ecdsa_der(raw_sig)
        return raw_sig

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                try:
                    self._session.close()
                except Exception:
                    pass
                self._session = None
                self._key = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Transport / main provider
# ---------------------------------------------------------------------------

MAX_PIN_ATTEMPTS = 3


@dataclass
class SmartcardSslContextProvider(SslContextProvider):
    """ssl.SSLContext and gRPC ChannelCredentials provider for smartcard-backed mTLS.

    HTTP path: call ``create_ssl_context()`` to get an ``ssl.SSLContext`` backed by the
    OpenSSL pkcs11-provider.

    gRPC path: call ``create_grpc_channel_credentials()`` to get a ``grpc.ChannelCredentials``
    that uses a PKCS#11 signing callback so the private key never leaves the card.

    Both paths share the same session discovery and PIN prompt, each caching their result
    after the first successful call.
    """

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)
    _cached_grpc_credentials: Any | None = field(default=None, repr=False, compare=False)
    _signer: SmartcardPrivateKeySigner | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls) -> SmartcardSslContextProvider:
        return cls()

    @property
    def session_manager(self) -> SmartcardSessionManager:
        if self._session_manager is not None:
            return self._session_manager
        return SmartcardSessionManager.shared()

    @property
    def openssl_bridge(self) -> OpenSslProviderBridge:
        if self._openssl_bridge is not None:
            return self._openssl_bridge
        return OpenSslProviderBridge()

    def create_ssl_context(self) -> ssl.SSLContext:
        with self._lock:
            if self._cached_ctx is None:
                session = self.session_manager.get_session()
                for attempt in range(MAX_PIN_ATTEMPTS):
                    remaining = MAX_PIN_ATTEMPTS - attempt - 1
                    try:
                        self._cached_ctx = self.openssl_bridge.build_ssl_context(session=session)
                        break
                    except SmartcardPinLockedError:
                        raise SystemExit("Card PIN is locked. Contact your security administrator.")
                    except SmartcardPinError:
                        base_message = "Incorrect PIN."
                        if remaining == 0:
                            raise SystemExit(f"{base_message} No attempts remaining.")
                        print(f"{base_message} {remaining} attempt(s) remaining, please try again.")
                    except SmartcardProviderError as exc:
                        raise SystemExit(
                            "Authentication failed. PIN entry may have been cancelled, or an unexpected "
                            "smartcard provider error occurred."
                        ) from exc
            assert self._cached_ctx is not None
            return self._cached_ctx

    def create_grpc_channel_credentials(
        self,
        *,
        root_certificates: bytes | None = None,
        certificate_chain_pem: bytes | None = None,
    ) -> Any:
        """Return ``grpc.ChannelCredentials`` for smartcard-backed mTLS over gRPC.

        ``root_certificates`` is forwarded to gRPC as the trusted CA bundle. ``None`` causes
        gRPC to use system roots. ``certificate_chain_pem`` allows supplying additional
        intermediate certificates in PEM format. When ``None`` (the default), only the leaf
        certificate from the card is used.
        """
        with self._lock:
            if self._cached_grpc_credentials is not None:
                return self._cached_grpc_credentials

            session = self.session_manager.get_session()

            token_label = session.certificate.token_label
            object_id_bytes = session.certificate.object_id_bytes

            if not token_label:
                raise SmartcardConfigurationError(
                    "Could not determine token label for the selected certificate. "
                    "The PKCS#11 token may not have reported a label."
                )
            if object_id_bytes is None:
                raise SmartcardConfigurationError(
                    "Could not determine object ID for the selected certificate. "
                    "The PKCS#11 token may not have reported a CKA_ID attribute."
                )

            signer = SmartcardPrivateKeySigner(
                module_path=session.module_path,
                token_label=token_label,
                object_id_bytes=object_id_bytes,
            )

            if certificate_chain_pem is None:
                if not session.certificate.der_certificate:
                    raise SmartcardConfigurationError(
                        "Certificate DER data is empty; cannot build PEM chain for gRPC credentials. "
                        "The PKCS#11 token may not have returned a certificate value."
                    )
                cert = x509.load_der_x509_certificate(session.certificate.der_certificate)
                certificate_chain_pem = cert.public_bytes(Encoding.PEM)

            self._signer = signer
            self._cached_grpc_credentials = ssl_channel_credentials_with_custom_signer(
                private_key_sign_fn=signer.sign,
                root_certificates=root_certificates,
                certificate_chain=certificate_chain_pem,
            )
            return self._cached_grpc_credentials

    def close(self) -> None:
        """Release PKCS#11 session resources held by the gRPC signer."""
        with self._lock:
            if self._signer is not None:
                self._signer.close()
                self._signer = None
            self._cached_grpc_credentials = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "SmartcardCertificateSelectionError",
    "SmartcardConfigurationError",
    "SmartcardError",
    "SmartcardPinError",
    "SmartcardPinLockedError",
    "SmartcardProviderError",
    "SmartcardSslContextProvider",
]
