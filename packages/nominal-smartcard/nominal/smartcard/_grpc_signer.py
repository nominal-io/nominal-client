from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from nominal.smartcard._errors import SmartcardConfigurationError, SmartcardPinError, SmartcardPinLockedError

# TLS 1.3 SignatureScheme values (RFC 8446 §4.2.3).
# grpc.experimental.PrivateKeySignatureAlgorithm is a Cython IntEnum backed by these constants,
# so comparing int(algorithm) against them works without importing grpcio here.
_RSA_PKCS1_SHA256 = 0x0401
_RSA_PKCS1_SHA384 = 0x0501
_RSA_PKCS1_SHA512 = 0x0601
_ECDSA_SECP256R1_SHA256 = 0x0403
_ECDSA_SECP384R1_SHA384 = 0x0503
_ECDSA_SECP521R1_SHA512 = 0x0603
_RSA_PSS_RSAE_SHA256 = 0x0804
_RSA_PSS_RSAE_SHA384 = 0x0805
_RSA_PSS_RSAE_SHA512 = 0x0806

_ECDSA_ALGORITHMS: frozenset[int] = frozenset(
    {_ECDSA_SECP256R1_SHA256, _ECDSA_SECP384R1_SHA384, _ECDSA_SECP521R1_SHA512}
)

# Session-invalidating PKCS#11 errors that warrant clearing cached session state so that
# the next sign() attempt can re-establish a fresh session (e.g. after card removal/reinsert).
_SESSION_INVALIDATING_ERRORS: tuple[str, ...] = (
    "DeviceRemoved",
    "TokenNotPresent",
    "SessionHandleInvalid",
    "SessionClosed",
)

_mechanism_table_lock = threading.Lock()
_mechanism_table: dict[int, tuple[Any, Any]] | None = None


def _get_mechanism_table() -> dict[int, tuple[Any, Any]]:
    """Return a mapping from TLS SignatureScheme integer → (pkcs11.Mechanism, mechanism_param).

    Built lazily so the pkcs11 import is deferred until first use.
    mechanism_param for RSA PSS is (hash_mechanism, mgf, salt_length) per python-pkcs11 conventions.
    For all other mechanisms it is None.
    """
    global _mechanism_table
    with _mechanism_table_lock:
        if _mechanism_table is not None:
            return _mechanism_table
        from pkcs11.mechanisms import MGF, Mechanism

        _mechanism_table = {
            _RSA_PKCS1_SHA256: (Mechanism.SHA256_RSA_PKCS, None),
            _RSA_PKCS1_SHA384: (Mechanism.SHA384_RSA_PKCS, None),
            _RSA_PKCS1_SHA512: (Mechanism.SHA512_RSA_PKCS, None),
            # PSS combined mechanisms: mechanism_param specifies hash, MGF, and salt length.
            # TLS 1.3 mandates salt length == hash length (RFC 8446 §4.2.3).
            _RSA_PSS_RSAE_SHA256: (Mechanism.SHA256_RSA_PKCS_PSS, (Mechanism.SHA256, MGF.SHA256, 32)),
            _RSA_PSS_RSAE_SHA384: (Mechanism.SHA384_RSA_PKCS_PSS, (Mechanism.SHA384, MGF.SHA384, 48)),
            _RSA_PSS_RSAE_SHA512: (Mechanism.SHA512_RSA_PKCS_PSS, (Mechanism.SHA512, MGF.SHA512, 64)),
            _ECDSA_SECP256R1_SHA256: (Mechanism.ECDSA_SHA256, None),
            _ECDSA_SECP384R1_SHA384: (Mechanism.ECDSA_SHA384, None),
            _ECDSA_SECP521R1_SHA512: (Mechanism.ECDSA_SHA512, None),
        }
        return _mechanism_table


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
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

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
        pin: str,
    ) -> None:
        self._module_path = module_path
        self._token_label = token_label
        self._object_id_bytes = object_id_bytes
        self._pin = pin
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
            import pkcs11 as _pkcs11
            from pkcs11 import ObjectClass
        except ImportError as e:
            raise SmartcardConfigurationError(
                "python-pkcs11 is not installed. Run `pip install nominal-smartcard`."
            ) from e

        try:
            lib = _pkcs11.lib(str(self._module_path))
        except Exception as e:
            raise SmartcardConfigurationError(
                f"Failed to load PKCS#11 module {self._module_path}: {e}"
            ) from e

        try:
            slots = lib.get_slots(token_present=True)
        except _pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        token = None
        for slot in slots:
            try:
                t = slot.get_token()
                if t.label.strip() == self._token_label:
                    token = t
                    break
            except _pkcs11.exceptions.PKCS11Error:
                continue

        if token is None:
            raise SmartcardConfigurationError(
                f"PKCS#11 token {self._token_label!r} not found. "
                "Verify the smartcard is inserted and the token label is correct."
            )

        try:
            session = token.open(user_pin=self._pin)
        except _pkcs11.exceptions.PinIncorrect:
            raise SmartcardPinError(f"Incorrect PIN for token {self._token_label!r}.") from None
        except _pkcs11.exceptions.PinLocked:
            raise SmartcardPinLockedError(
                f"PIN is locked for token {self._token_label!r}. "
                "Too many incorrect attempts have been made."
            ) from None
        except _pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(
                f"Failed to open PKCS#11 session on token {self._token_label!r}: {e}"
            ) from e

        try:
            key = session.get_key(object_class=ObjectClass.PRIVATE_KEY, id=self._object_id_bytes)
        except Exception as e:
            session.close()
            raise SmartcardConfigurationError(
                f"Private key with id={self._object_id_bytes.hex()!r} not found on token "
                f"{self._token_label!r}: {e}"
            ) from e

        self._session = session
        self._key = key
        return session, key

    def sign(
        self,
        data_to_sign: bytes,
        signature_algorithm: Any,
        on_complete: Any,
    ) -> bytes:
        """Sign ``data_to_sign`` on the smartcard and return raw signature bytes.

        This is the synchronous form of the gRPC ``CustomPrivateKeySign`` callback.
        ``on_complete`` is intentionally unused — gRPC only calls it for the async form
        (where the function returns a cancel callable instead of bytes).

        Raises ``SmartcardConfigurationError`` on PKCS#11 errors, which gRPC treats as a
        TLS handshake failure.
        """
        try:
            algo = int(signature_algorithm)
        except (TypeError, ValueError) as e:
            raise SmartcardConfigurationError(
                f"Invalid signature algorithm: expected a PrivateKeySignatureAlgorithm enum value, "
                f"got {type(signature_algorithm).__name__!r}."
            ) from e

        try:
            import pkcs11 as _pkcs11
        except ImportError as e:
            raise SmartcardConfigurationError(
                "python-pkcs11 is not installed. Run `pip install nominal-smartcard`."
            ) from e

        table = _get_mechanism_table()
        entry = table.get(algo)
        if entry is None:
            raise SmartcardConfigurationError(
                f"Unsupported TLS signature algorithm 0x{algo:04x}. "
                "The smartcard signer supports RSA PKCS#1, RSA PSS, and ECDSA with SHA-256/384/512 "
                "over TLS 1.3."
            )
        mechanism, mechanism_param = entry

        with self._lock:
            _session, key = self._ensure_session_and_key()
            try:
                raw_sig: bytes = key.sign(data_to_sign, mechanism=mechanism, mechanism_param=mechanism_param)
            except _pkcs11.exceptions.PKCS11Error as e:
                # Clear the cached session if the card was removed or the session became invalid,
                # so the next sign() attempt can re-establish a fresh authenticated session.
                if type(e).__name__ in _SESSION_INVALIDATING_ERRORS:
                    self._session = None
                    self._key = None
                raise SmartcardConfigurationError(
                    f"PKCS#11 signing failed (algorithm 0x{algo:04x}): {e}"
                ) from e

        # PKCS#11 ECDSA returns raw r||s bytes; gRPC/BoringSSL expects DER-encoded ASN.1.
        if algo in _ECDSA_ALGORITHMS:
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
            self._pin = ""

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
