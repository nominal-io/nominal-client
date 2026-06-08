from __future__ import annotations

import getpass
import threading
from pathlib import Path
from typing import Any

import grpc.experimental
import pkcs11
import pkcs11.exceptions
from pkcs11.mechanisms import MGF, Mechanism

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._signature_utils import encode_ecdsa_der

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


def _pin_prompt(token: Any, token_label: str) -> str:
    """Build a PIN prompt that identifies the token and its slot."""
    location = ""
    try:
        slot = token.slot
        description = (slot.slot_description or "").strip()
        location = f" (Slot {slot.slot_id} - {description})" if description else f" (Slot {slot.slot_id})"
    except Exception:
        pass
    return f"Enter PIN for {token_label!r}{location}: "


# Mapping from PrivateKeySignatureAlgorithm to (pkcs11.Mechanism, mechanism_param).
# mechanism_param for RSA PSS is (hash_mechanism, mgf, salt_length) per python-pkcs11 conventions.
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


class SmartcardPrivateKeySigner:
    """PKCS#11 signing callback for gRPC's custom signer TLS credentials.

    Holds a persistent PKCS#11 session for the lifetime of the associated gRPC channel.
    """

    def __init__(
        self,
        *,
        module_path: Path,
        token_label: str,
        object_id_bytes: bytes,
    ) -> None:
        self._module_path = module_path
        self._token_label = token_label
        self._object_id_bytes = object_id_bytes
        self._session: Any = None
        self._key: Any = None
        self._lock = threading.Lock()

    def _open_authenticated_session(self, token: Any) -> Any:
        """Open a User session on ``token``. Propagates PinIncorrect and PinLocked to the caller."""
        try:
            return token.open(user_pin=_prompt_for_pin(_pin_prompt(token, self._token_label)))
        except (pkcs11.exceptions.PinIncorrect, pkcs11.exceptions.PinLocked, pkcs11.exceptions.PinLenRange):
            raise
        except pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(
                f"Failed to open PKCS#11 session on token {self._token_label!r}: {e}"
            ) from e

    def connect(self) -> None:
        """Establish the authenticated PKCS#11 session, prompting for PIN if needed.

        Must be called before the signer is handed to gRPC. The signing callback
        (sign()) is invoked on every TLS handshake and must not block.

        Retries up to MAX_PIN_ATTEMPTS times on incorrect PIN.
        """
        with self._lock:
            for attempt in range(MAX_PIN_ATTEMPTS):
                remaining = MAX_PIN_ATTEMPTS - attempt - 1
                try:
                    self._ensure_session_and_key()
                    return
                except pkcs11.exceptions.PinLocked:
                    raise SystemExit("Card PIN is locked. Contact your security administrator.")
                except (pkcs11.exceptions.PinIncorrect, pkcs11.exceptions.PinLenRange):
                    self._session = None
                    self._key = None
                    message = "Incorrect PIN."
                    if remaining == 0:
                        raise SystemExit(f"{message} No attempts remaining.")
                    print(f"{message} {remaining} attempt(s) remaining, please try again.", flush=True)

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
        except pkcs11.exceptions.PKCS11Error as e:
            raise SmartcardConfigurationError(f"Failed to list PKCS#11 slots: {e}") from e

        token = None
        for slot in slots:
            try:
                t = slot.get_token()
                if t.label.strip() == self._token_label:
                    token = t
                    break
            except pkcs11.exceptions.PKCS11Error:
                continue

        if token is None:
            raise SmartcardConfigurationError(
                f"PKCS#11 token {self._token_label!r} not found. "
                "Verify the smartcard is inserted and the token label is correct."
            )

        session = self._open_authenticated_session(token)

        try:
            key = session.get_key(object_class=pkcs11.ObjectClass.PRIVATE_KEY, id=self._object_id_bytes)
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
        _on_complete: grpc.experimental.PrivateKeySignOnComplete,
    ) -> bytes:
        """Sign ``data_to_sign`` on the smartcard and return raw signature bytes.

        Implements gRPC's ``CustomPrivateKeySign`` contract. We sign synchronously and return the
        bytes directly, so ``_on_complete`` (the async-completion callback) is accepted but unused.
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
            _, key = self._ensure_session_and_key()
            try:
                raw_sig: bytes = key.sign(data_to_sign, mechanism=mechanism, mechanism_param=mechanism_param)
            except pkcs11.exceptions.PKCS11Error as e:
                raise SmartcardConfigurationError(f"PKCS#11 signing failed ({signature_algorithm!r}): {e}") from e

        if signature_algorithm in _ECDSA_ALGORITHMS:
            return encode_ecdsa_der(raw_sig)
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
