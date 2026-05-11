"""Smartcard / CAC client-cert TLS support for Nominal Core.

This module wires an HTTPS adapter that performs client-cert TLS using a PKCS#11 token (e.g. a DoD CAC). The
session prompts the user for a PIN exactly once per process, loads the OpenSSL `pkcs11` provider (libp11's
`pkcs11prov`, or any RFC 7512 compatible provider) into the in-process libcrypto, and uses OSSL_STORE to
resolve the token-resident certificate and private key by URI. The resulting EVP_PKEY / X509 references are
installed onto a shared pyOpenSSL `SSL.Context`. Subsequent TLS handshakes (to Nominal API endpoints and to S3
presigned URLs) reuse that context, and the smartcard signs handshake messages via the provider — no further
PIN prompts.

Why a provider rather than the legacy pkcs11 engine
---------------------------------------------------
OpenSSL ENGINEs are deprecated in OpenSSL 3.x. The replacement model is OpenSSL Providers. libp11 ships
`pkcs11prov` as the provider-shaped replacement for its legacy `pkcs11` engine; alternative implementations
(notably `latchset/pkcs11-provider`) follow the same provider name and RFC 7512 URI scheme so the integration
is portable. We load the provider explicitly via `OSSL_PROVIDER_load(NULL, "pkcs11")` and pass the PKCS#11
module path (e.g. opensc-pkcs11.so) inside the URI as a `module-path=` query attribute, so no system-wide
OPENSSL_CONF edits are required.

PIN handling
------------
The PIN is prompted interactively via `getpass.getpass` and is NEVER persisted anywhere recoverable: no
keyring, no environment variable, no log. It is passed to the provider exactly once, inside the OSSL_STORE
URI, while the certificate and private key are loaded. The local Python references are dropped immediately
afterward. Strings are immutable in CPython so the bytes may briefly linger in heap memory, but no reference
survives this scope.

Optional dependencies
---------------------
Install with `pip install 'nominal[smartcard]'`. The OS must also have:
- A PKCS#11 module (typically OpenSC's `opensc-pkcs11`). Discovered automatically; `NOMINAL_PKCS11_MODULE`
  overrides discovery if set.
- An OpenSSL `pkcs11` provider (libp11's `pkcs11prov` or `latchset/pkcs11-provider`) installed where the
  in-process libcrypto can find it. Override the search path with `NOMINAL_OSSL_MODULES_DIR` if needed.

FFI surface lives in `_openssl_provider.py` so the orchestration here reads like ordinary Python.
"""

from __future__ import annotations

import getpass
import logging
import os
import platform
import threading
import urllib.parse
from typing import TYPE_CHECKING, Any

from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.poolmanager import PoolManager

from nominal.core._utils import _openssl_provider as openssl_provider

if TYPE_CHECKING:  # pragma: no cover - typing-only import
    import OpenSSL.SSL

logger = logging.getLogger(__name__)


_DEFAULT_PKCS11_MODULE_PATHS = {
    "Linux": [
        "/usr/lib/x86_64-linux-gnu/opensc-pkcs11.so",
        "/usr/lib64/opensc-pkcs11.so",
        "/usr/lib/opensc-pkcs11.so",
        "/usr/local/lib/opensc-pkcs11.so",
    ],
    "Darwin": [
        "/Library/OpenSC/lib/opensc-pkcs11.so",
        "/usr/local/lib/opensc-pkcs11.so",
        "/opt/homebrew/lib/opensc-pkcs11.so",
    ],
    "Windows": [
        r"C:\Windows\System32\opensc-pkcs11.dll",
        r"C:\Program Files\OpenSC Project\OpenSC\pkcs11\opensc-pkcs11.dll",
    ],
}

_DEFAULT_OSSL_MODULES_DIRS = {
    "Linux": [
        "/usr/lib/x86_64-linux-gnu/ossl-modules",
        "/usr/lib64/ossl-modules",
        "/usr/lib/ossl-modules",
        "/usr/local/lib/ossl-modules",
    ],
    "Darwin": [
        "/opt/homebrew/lib/ossl-modules",
        "/usr/local/lib/ossl-modules",
    ],
    "Windows": [
        r"C:\Program Files\OpenSSL\bin\ossl-modules",
        r"C:\Program Files\OpenSSL-Win64\bin\ossl-modules",
    ],
}

_PROVIDER_NAME = "pkcs11"


class SmartcardError(RuntimeError):
    """Raised for any smartcard configuration / runtime failure that prevents TLS client auth."""


def discover_pkcs11_module() -> str:
    """Find the path to a PKCS#11 module on the local system.

    `NOMINAL_PKCS11_MODULE` takes precedence. Otherwise, OS-specific standard locations for OpenSC are tried.
    """
    override = os.environ.get("NOMINAL_PKCS11_MODULE")
    if override:
        if not os.path.exists(override):
            raise SmartcardError(f"NOMINAL_PKCS11_MODULE={override!r} does not exist on disk")
        return override

    system = platform.system()
    for candidate in _DEFAULT_PKCS11_MODULE_PATHS.get(system, []):
        if os.path.exists(candidate):
            return candidate
    raise SmartcardError(
        f"No PKCS#11 module found on {system}. Install OpenSC (https://github.com/OpenSC/OpenSC) "
        f"or set NOMINAL_PKCS11_MODULE to the absolute path of your module "
        f"(e.g. opensc-pkcs11.so / opensc-pkcs11.dll)."
    )


def _discover_ossl_modules_dir() -> str | None:
    """Return the directory to add to OpenSSL's provider search path, or None to use the default."""
    override = os.environ.get("NOMINAL_OSSL_MODULES_DIR")
    if override:
        return override
    for candidate in _DEFAULT_OSSL_MODULES_DIRS.get(platform.system(), []):
        if os.path.isdir(candidate):
            return candidate
    return None


def _prompt_pin(token_label: str) -> str:
    """Prompt the user for the smartcard PIN. Interactive only — never persisted."""
    return getpass.getpass(f"Enter PIN for smartcard token {token_label!r}: ")


def _probe_token_label(module_path: str) -> str:
    """Open the PKCS#11 module just long enough to confirm a token is present and read its label.

    No login is performed — the PIN is only handed to the OpenSSL pkcs11 provider during URI-driven key
    loading. This early probe lets us fail fast with a clear error before touching OpenSSL providers, and
    gives the PIN prompt a recognizable token name.
    """
    try:
        import PyKCS11
    except ImportError as e:
        raise SmartcardError(
            "PyKCS11 is required for smartcard auth. Install with: pip install 'nominal[smartcard]'"
        ) from e

    lib = PyKCS11.PyKCS11Lib()
    try:
        lib.load(module_path)
    except PyKCS11.PyKCS11Error as e:
        raise SmartcardError(f"failed to load PKCS#11 module at {module_path!r}: {e}") from e

    slots = lib.getSlotList(tokenPresent=True)
    if not slots:
        raise SmartcardError("No smartcard tokens detected. Insert a CAC into your reader and retry.")
    slot = slots[0]
    token_info = lib.getTokenInfo(slot)
    label = (token_info.label or "").strip() if token_info.label else ""
    return label or f"slot-{slot}"


def _cert_score_for_tls_client_auth(cert_der: bytes) -> int:
    """Score a token certificate for client-TLS use.

    CAC/PIV tokens commonly expose several cert/key pairs. Prefer a cert that explicitly carries the
    id-kp-clientAuth EKU, then a cert with a digital-signature key usage. Return 0 for unparsable certs so
    they can still be used as a last resort if the token metadata is sparse.
    """
    try:
        from cryptography import x509
        from cryptography.x509.oid import ExtendedKeyUsageOID

        cert = x509.load_der_x509_certificate(cert_der)
    except Exception:
        logger.debug("Could not parse smartcard certificate while selecting client-auth object", exc_info=True)
        return 0

    score = 0
    try:
        eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        pass
    else:
        if ExtendedKeyUsageOID.CLIENT_AUTH in eku:
            score += 100

    try:
        key_usage = cert.extensions.get_extension_for_class(x509.KeyUsage).value
    except x509.ExtensionNotFound:
        pass
    else:
        if key_usage.digital_signature:
            score += 10

    return score


def _select_client_auth_cert_id(module_path: str) -> bytes | None:
    """Return the CKA_ID of the best visible certificate for TLS client auth.

    The provider path can enumerate the token too, but constraining the PKCS#11 URI by `id=` avoids pairing
    arbitrary "first" objects on multi-certificate CAC/PIV tokens. Certificate objects are public, so this
    probe does not require the PIN.
    """
    try:
        import PyKCS11
    except ImportError as e:
        raise SmartcardError(
            "PyKCS11 is required for smartcard auth. Install with: pip install 'nominal[smartcard]'"
        ) from e

    lib = PyKCS11.PyKCS11Lib()
    try:
        lib.load(module_path)
    except PyKCS11.PyKCS11Error as e:
        raise SmartcardError(f"failed to load PKCS#11 module at {module_path!r}: {e}") from e

    slots = lib.getSlotList(tokenPresent=True)
    if not slots:
        raise SmartcardError("No smartcard tokens detected. Insert a CAC into your reader and retry.")

    session = lib.openSession(slots[0])
    try:
        cert_objects = session.findObjects([(PyKCS11.CKA_CLASS, PyKCS11.CKO_CERTIFICATE)])
        best_id: bytes | None = None
        best_score = -1
        for cert_obj in cert_objects:
            try:
                raw_cert, raw_id = session.getAttributeValue(cert_obj, [PyKCS11.CKA_VALUE, PyKCS11.CKA_ID])
            except Exception:
                logger.debug("Could not read certificate attributes from smartcard", exc_info=True)
                continue
            if not raw_cert or not raw_id:
                continue
            cert_id = bytes(bytearray(raw_id))
            score = _cert_score_for_tls_client_auth(bytes(bytearray(raw_cert)))
            if score > best_score:
                best_id = cert_id
                best_score = score
        return best_id
    finally:
        try:
            session.closeSession()
        except Exception:
            logger.debug("PyKCS11 session.closeSession() failed after certificate probe", exc_info=True)


def _build_pkcs11_uri(module_path: str, pin: str, cert_id: bytes | None = None) -> str:
    """Build an RFC 7512 PKCS#11 URI for provider-backed smartcard loading.

    When `cert_id` is known, the URI is scoped to objects with that CKA_ID so the provider resolves a matching
    cert/key pair instead of whatever object happens to enumerate first.
    """
    path = f"id={urllib.parse.quote_from_bytes(cert_id, safe='')}" if cert_id else ""
    query = f"module-path={urllib.parse.quote(module_path, safe='')}&pin-value={urllib.parse.quote(pin, safe='')}"
    return f"pkcs11:{path}?{query}"


def _verify_callback(_conn: Any, _cert: Any, _errnum: int, _depth: int, preverify_ok: int) -> bool:
    """PyOpenSSL verify callback: return OpenSSL's own verification verdict."""

    return bool(preverify_ok)


class SmartcardSession:
    """Process-wide singleton that owns the smartcard-backed pyOpenSSL `SSL.Context`.

    `SmartcardSession.get()` lazily prompts for a PIN and loads the cert + key on first use. Subsequent
    calls return the same instance with the same context, which is shared across every HTTPS connection
    produced by `SmartcardPoolManager`.

    Thread safety: pyOpenSSL's `SSL.Context` is safe to share across threads as long as each handshake
    gets its own `SSL.Connection`. The provider's per-token PKCS#11 session is serialized internally;
    concurrent handshakes therefore queue at the signing call, which is fine for typical client workloads.
    """

    _instance: SmartcardSession | None = None
    _instance_lock = threading.Lock()

    def __init__(self, module_path: str) -> None:
        self._module_path = module_path
        self._token_label = _probe_token_label(module_path)
        self._cert_id = _select_client_auth_cert_id(module_path)
        self._ctx_lock = threading.Lock()
        self._ssl_context_cache: OpenSSL.SSL.Context | None = None
        self._closed = False

    @property
    def ssl_context(self) -> OpenSSL.SSL.Context:
        """Return the SSL context, building it lazily on first access.

        Building lazily means `NominalClient` creation can still successfully discover the token before any
        PIN prompt happens. The prompt fires on the first network call, or eagerly if the caller does
        `SmartcardSession.get().ssl_context` immediately after construction.
        """
        with self._ctx_lock:
            if self._ssl_context_cache is None:
                self._ssl_context_cache = self._build_ssl_context()
            return self._ssl_context_cache

    @property
    def token_label(self) -> str:
        return self._token_label

    def _build_ssl_context(self) -> OpenSSL.SSL.Context:
        """Resolve the smartcard cert + private key via OpenSSL's pkcs11 provider, then install them on a
        fresh pyOpenSSL `SSL.Context`.
        """
        try:
            from OpenSSL import SSL
        except ImportError as e:
            raise SmartcardError(
                "pyOpenSSL is required for smartcard auth. Install with: pip install 'nominal[smartcard]'"
            ) from e

        try:
            handle = openssl_provider.lib_handle()
            openssl_provider.load_provider(handle, _PROVIDER_NAME, _discover_ossl_modules_dir())

            pin = _prompt_pin(self._token_label)
            try:
                uri = _build_pkcs11_uri(self._module_path, pin, self._cert_id)
            finally:
                # CPython strings are immutable so we cannot zero memory, but no Python reference survives.
                del pin

            try:
                cert, pkey = openssl_provider.load_cert_and_key(handle, uri)
            finally:
                # `uri` carries the PIN; drop our reference now that the provider has consumed it.
                del uri

            ctx = SSL.Context(SSL.TLS_CLIENT_METHOD)
            openssl_provider.validate_pyopenssl_context(handle, ctx)
            ssl_ctx_cdata = openssl_provider.cast_pyopenssl_ssl_ctx(handle, ctx)
            openssl_provider.install_on_ssl_context(handle, ssl_ctx_cdata, cert, pkey)
        except openssl_provider.OpenSSLProviderError as e:
            raise SmartcardError(f"Failed to build smartcard SSL context for token {self._token_label!r}: {e}") from e

        ctx.set_default_verify_paths()
        ctx.set_verify(SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT, _verify_callback)
        return ctx

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Nothing to actively tear down: OSSL_STORE was closed at the end of _build_ssl_context, and the
        # provider stays loaded for the rest of the process. The SSL.Context goes away with its refcount.

    @classmethod
    def get(cls, module_path: str | None = None) -> SmartcardSession:
        """Return the process-wide smartcard session, creating it on first call.

        On first call: discovers the PKCS#11 module and probes for a token. The PIN prompt happens on the
        first access of `.ssl_context`, not here — token-only probing avoids an unnecessary prompt when
        callers just want to confirm a token is plugged in.
        """
        with cls._instance_lock:
            if cls._instance is None:
                resolved_path = module_path or discover_pkcs11_module()
                cls._instance = cls(resolved_path)
            return cls._instance

    @classmethod
    def reset_for_test(cls) -> None:
        """Tear down the singleton. Intended for tests only."""
        with cls._instance_lock:
            inst = cls._instance
            cls._instance = None
        if inst is not None:
            inst.close()
        openssl_provider.reset_for_test()


class SmartcardHTTPSConnection(HTTPSConnection):
    """urllib3 HTTPSConnection that performs TLS via pyOpenSSL backed by the smartcard."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Strip stdlib-ssl kwargs the parent will fight with; we replace the TLS layer entirely.
        for k in (
            "ssl_context",
            "ssl_minimum_version",
            "ssl_maximum_version",
            "ssl_version",
            "cert_reqs",
            "ca_certs",
            "ca_cert_dir",
            "ca_cert_data",
            "server_hostname",
            "assert_hostname",
            "assert_fingerprint",
        ):
            kwargs.pop(k, None)
        super().__init__(*args, **kwargs)

    def connect(self) -> None:
        try:
            from OpenSSL import SSL
        except ImportError as e:  # pragma: no cover - guarded earlier
            raise SmartcardError("pyOpenSSL is required for smartcard auth") from e

        sock = self._new_conn()
        if getattr(self, "_tunnel_host", None):
            self.sock = sock
            self._tunnel()

        ctx = SmartcardSession.get().ssl_context
        ssl_conn = SSL.Connection(ctx, sock)
        host = str(getattr(self, "_tunnel_host", None) or self.host).removeprefix("[").removesuffix("]")
        try:
            import ipaddress

            ipaddress.ip_address(host)
        except ValueError:
            ssl_conn.set_tlsext_host_name(host.encode("idna"))
        handle = openssl_provider.lib_handle()
        ssl_cdata = openssl_provider.cast_pyopenssl_ssl(handle, ssl_conn)
        openssl_provider.configure_hostname_verification(handle, ssl_cdata, host)
        ssl_conn.set_connect_state()
        try:
            ssl_conn.do_handshake()
            openssl_provider.assert_verify_ok(handle, ssl_cdata, host)
        except SSL.Error as e:
            ssl_conn.close()
            raise SmartcardError(f"TLS handshake failed against {self.host!r}: {e}") from e
        except openssl_provider.OpenSSLProviderError as e:
            ssl_conn.close()
            raise SmartcardError(f"TLS verification failed against {self.host!r}: {e}") from e
        self.sock = ssl_conn


class SmartcardHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = SmartcardHTTPSConnection


class SmartcardPoolManager(PoolManager):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Override only the https scheme — http should still use the stdlib pool.
        self.pool_classes_by_scheme = {
            "http": self.pool_classes_by_scheme["http"],
            "https": SmartcardHTTPSConnectionPool,
        }


__all__ = [
    "SmartcardError",
    "SmartcardHTTPSConnection",
    "SmartcardHTTPSConnectionPool",
    "SmartcardPoolManager",
    "SmartcardSession",
    "discover_pkcs11_module",
]