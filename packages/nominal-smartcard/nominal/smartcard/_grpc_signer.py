from __future__ import annotations

import getpass
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import grpc.experimental
import pkcs11 as _pkcs11
import pkcs11.exceptions as _pkcs11_exc
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from pkcs11 import ObjectClass
from pkcs11.mechanisms import MGF, Mechanism

from nominal.smartcard._errors import SmartcardConfigurationError

_Algorithm = grpc.experimental.PrivateKeySignatureAlgorithm

_ECDSA_ALGORITHMS: frozenset[grpc.experimental.PrivateKeySignatureAlgorithm] = frozenset(
    {
        _Algorithm.ECDSA_SECP256R1_SHA256,
        _Algorithm.ECDSA_SECP384R1_SHA384,
        _Algorithm.ECDSA_SECP521R1_SHA512,
    }
)

MAX_PIN_ATTEMPTS = 3


def _prompt_for_pin(prompt: str) -> str:
    return getpass.getpass(prompt)


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

    Holds a persistent PKCS#11 session for the lifetime of the associated gRPC channel.
    The private key never leaves the card; the only output is the signature produced by
    the token during each TLS handshake.

    Pass ``signer.sign`` as ``private_key_sign_fn`` to
    ``grpc.experimental.ssl_channel_credentials_with_custom_signer``.

    The authenticated session handle is cached after the first successful login,
    enabling automatic session recovery if the card is briefly removed and reinserted.
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
            lib = _pkcs11.lib(str(self._module_path))
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
        session: Any = None
        for attempt in range(MAX_PIN_ATTEMPTS):
            remaining = MAX_PIN_ATTEMPTS - attempt - 1
            try:
                session = token.open(user_pin=pin_fn("Card PIN: "))
                break
            except _pkcs11_exc.PinLocked:
                raise SystemExit("Card PIN is locked. Contact your security administrator.")
            except _pkcs11_exc.PinIncorrect:
                if remaining == 0:
                    raise SystemExit("Incorrect PIN. No attempts remaining.")
                print(f"Incorrect PIN. {remaining} attempt(s) remaining, please try again.", flush=True)
            except _pkcs11_exc.PKCS11Error as e:
                raise SmartcardConfigurationError(
                    f"Failed to open PKCS#11 session on token {self._token_label!r}: {e}"
                ) from e
        assert session is not None

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
