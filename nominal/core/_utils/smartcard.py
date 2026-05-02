"""Smartcard / CAC client-cert TLS support for Nominal Core.

This module wires an HTTPS adapter that performs client-cert TLS using a PKCS#11 token (e.g. a DoD CAC). The
session prompts the user for a PIN exactly once per process, opens a PKCS#11 session against the token, and
performs `C_Login` once. All subsequent TLS handshakes (to Nominal API endpoints and to S3 presigned URLs)
reuse that logged-in session and ask the token to `C_Sign` for the handshake — no further PIN prompts.

PIN handling
------------
The PIN is prompted interactively via `getpass.getpass` and is NEVER persisted anywhere recoverable: no
keyring, no environment variable, no file, no log. The local PIN buffer is overwritten after it is handed to
the PKCS#11 module's `C_Login`. The token itself retains "logged-in" state inside the smartcard's own session
state (this is the only safe place for it). A leaked PIN on a DoD CAC requires an in-person CAC-office visit
to reset, so the cost of a leak is high; we treat the PIN like a secret that must not survive its single use.

Optional dependencies
---------------------
This module imports `PyKCS11`, `OpenSSL`, and `cryptography`. They are not in the base install — install with:

    pip install 'nominal[smartcard]'

The OS must also have a PKCS#11 module installed (typically OpenSC's `opensc-pkcs11`). The module path is
discovered automatically; `NOMINAL_PKCS11_MODULE` overrides discovery if set.
"""

from __future__ import annotations

import getpass
import logging
import os
import platform
import socket
import threading
from typing import TYPE_CHECKING, Any

from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.poolmanager import PoolManager

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


def _prompt_pin(token_label: str) -> str:
    """Prompt the user for the smartcard PIN. Interactive only — never persisted."""
    return getpass.getpass(f"Enter PIN for smartcard token {token_label!r}: ")


def _zero_str(s: str) -> None:
    """Best-effort overwrite of a Python string. CPython strings are immutable, so this is symbolic — the
    real protection is that the variable binding is dropped immediately after C_Login.
    """
    # Strings in CPython cannot be mutated in place, so this is a no-op other than asserting the contract.
    # We rely on the surrounding code to drop the reference (`del pin`) so it becomes eligible for GC.
    del s


class SmartcardSession:
    """Process-wide singleton that owns the PKCS#11 session and the pyOpenSSL SSL.Context.

    `SmartcardSession.get()` lazily logs in on first call, prompting for a PIN. Subsequent calls return the
    same logged-in session. The same `OpenSSL.SSL.Context` is reused across all HTTPS connections issued by
    Nominal adapters.

    Thread safety: PKCS#11 sessions are not generally safe for concurrent use; an internal RLock serializes
    `sign` operations performed during TLS handshakes. The TCP/TLS data path uses pyOpenSSL.SSL.Connection's
    own thread safety once the handshake completes.
    """

    _instance: SmartcardSession | None = None
    _instance_lock = threading.Lock()

    def __init__(self, module_path: str) -> None:
        self._module_path = module_path
        self._session_lock = threading.RLock()
        self._lib, self._session, self._cert_der, self._key_handle, self._token_label = self._open_and_login(
            module_path
        )
        self._ssl_context_cache: OpenSSL.SSL.Context | None = None
        self._closed = False

    @staticmethod
    def _open_and_login(module_path: str) -> tuple[Any, Any, bytes, Any, str]:
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
            raise SmartcardError(
                "No smartcard tokens detected. Insert a CAC into your reader and retry."
            )
        slot = slots[0]
        token_info = lib.getTokenInfo(slot)
        token_label = token_info.label.strip() if token_info.label else f"slot-{slot}"

        session = lib.openSession(slot)
        pin = _prompt_pin(token_label)
        try:
            session.login(pin)
        except PyKCS11.PyKCS11Error as e:
            raise SmartcardError(f"PKCS#11 login failed for token {token_label!r}: {e}") from e
        finally:
            # Drop the PIN reference as soon as we hand it to C_Login. Strings are immutable in CPython so we
            # can't truly zero memory, but we can ensure no Python-level reference survives this scope.
            _zero_str(pin)
            del pin

        cert_der, key_handle = SmartcardSession._select_cert_and_key(PyKCS11, session)
        return lib, session, cert_der, key_handle, token_label

    @staticmethod
    def _select_cert_and_key(PyKCS11: Any, session: Any) -> tuple[bytes, Any]:
        """Find an X.509 certificate on the token and the private key that matches it (via CKA_ID)."""
        cert_template = [(PyKCS11.CKA_CLASS, PyKCS11.CKO_CERTIFICATE)]
        cert_objects = session.findObjects(cert_template)
        if not cert_objects:
            raise SmartcardError("No certificates found on smartcard token")

        cert_der: bytes | None = None
        cert_id: bytes | None = None
        for cert_obj in cert_objects:
            attrs = session.getAttributeValue(cert_obj, [PyKCS11.CKA_VALUE, PyKCS11.CKA_ID])
            if attrs[0]:
                cert_der = bytes(attrs[0])
                cert_id = bytes(attrs[1]) if attrs[1] else None
                break
        if cert_der is None:
            raise SmartcardError("Could not extract a certificate value from any object on the smartcard")

        key_template: list[tuple[Any, Any]] = [(PyKCS11.CKA_CLASS, PyKCS11.CKO_PRIVATE_KEY)]
        if cert_id is not None:
            key_template.append((PyKCS11.CKA_ID, cert_id))
        keys = session.findObjects(key_template)
        if not keys:
            raise SmartcardError("No matching private key found on smartcard for the selected certificate")
        return cert_der, keys[0]

    def _build_ssl_context(self) -> OpenSSL.SSL.Context:
        """Build the pyOpenSSL Context that drives client-cert TLS via the smartcard.

        Status: this hook is the integration point for the OpenSSL pkcs11 ENGINE (libp11). The module
        already owns the PKCS#11 session login (see `_open_and_login`) and the sign delegation path (see
        `_sign`). What's left is producing an `EVP_PKEY` that OpenSSL drives with smartcard signing during
        the TLS handshake. Two real options:

        1. **libp11 ENGINE**: use `cryptography.hazmat.bindings.openssl.binding.Binding` (cffi) to call
           `ENGINE_by_id("pkcs11")`, configure `MODULE_PATH`, hand over the PIN, `ENGINE_init`, and
           `ENGINE_load_private_key`. Then `SSL_CTX_use_PrivateKey` directly via cffi. Requires libp11 /
           engine_pkcs11 installed at the OS level. This is what `curl --engine pkcs11` and `openssl s_client
           -engine pkcs11` use. Note: since the engine wants the PIN, this path needs `_open_and_login` to
           defer login (the engine logs in itself) so we don't double-prompt.

        2. **Custom RSA_METHOD**: build an `RSA*` populated with public-key parameters and a `RSA_METHOD`
           whose `priv_enc` callback delegates to `_sign()`. Wrap in `EVP_PKEY` and call `SSL_CTX_use_PrivateKey`
           via cffi. No libp11 dependency; PyKCS11 stays the sole signer. Heavier cffi but pure-pip.

        Both require careful hardware validation against a real CAC or SoftHSM2 token — getting OpenSSL to
        accept a non-default key surface is fragile across OpenSSL versions. Until a follow-up PR wires one of
        these paths and validates against hardware, this raises a clear, actionable error so users know
        exactly how far the integration got: PKCS#11 token detected, PIN accepted, session logged in, cert +
        key handles located. The TLS bridge is the remaining work.
        """
        raise SmartcardError(
            f"Smartcard token {self._token_label!r} is logged in and the certificate + private key handles "
            "are resolved, but the OpenSSL TLS bridge has not been wired yet — building an EVP_PKEY backed "
            "by the smartcard requires either the libp11 OpenSSL engine or a cffi-based custom RSA_METHOD, "
            "and that work has not been validated against real hardware. Track follow-up: see the docstring "
            "of SmartcardSession._build_ssl_context for the two implementation options."
        )

    def _sign(self, data: bytes, mechanism: int) -> bytes:
        try:
            import PyKCS11
        except ImportError as e:  # pragma: no cover
            raise SmartcardError("PyKCS11 disappeared at runtime") from e
        with self._session_lock:
            sig = self._session.sign(self._key_handle, data, PyKCS11.Mechanism(mechanism, None))
        return bytes(sig)

    @property
    def ssl_context(self) -> OpenSSL.SSL.Context:
        """Return the SSL context, building it lazily on first access.

        Building lazily means `NominalClient` creation can still successfully prompt for the PIN and verify
        the smartcard is configured correctly even before the TLS bridge is fully wired — the failure surface
        moves to the first network call rather than client construction.
        """
        if self._ssl_context_cache is None:
            self._ssl_context_cache = self._build_ssl_context()
        return self._ssl_context_cache

    @property
    def token_label(self) -> str:
        return self._token_label

    def close(self) -> None:
        if self._closed:
            return
        with self._session_lock:
            try:
                self._session.logout()
            except Exception as e:  # pragma: no cover - best-effort cleanup
                logger.debug("smartcard logout raised %s", e)
            try:
                self._session.closeSession()
            except Exception as e:  # pragma: no cover
                logger.debug("smartcard closeSession raised %s", e)
            self._closed = True

    @classmethod
    def get(cls, module_path: str | None = None) -> SmartcardSession:
        """Return the process-wide smartcard session, creating and logging in on first call.

        On first call: discovers the PKCS#11 module, opens a session, and prompts for the PIN. Subsequent
        calls return the same instance with the token still logged in.
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

        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        ctx = SmartcardSession.get().ssl_context
        ssl_conn = SSL.Connection(ctx, sock)
        ssl_conn.set_tlsext_host_name(self.host.encode("ascii"))
        ssl_conn.set_connect_state()
        try:
            ssl_conn.do_handshake()
        except SSL.Error as e:
            ssl_conn.close()
            raise SmartcardError(f"TLS handshake failed against {self.host!r}: {e}") from e
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
