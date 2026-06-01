from __future__ import annotations

import os
import threading
from typing import Any

import grpc.experimental
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from nominal.smartcard._errors import SmartcardConfigurationError

NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR = "NOMINAL_WINDOWS_CERT_THUMBPRINT"

_Algorithm = grpc.experimental.PrivateKeySignatureAlgorithm

_ECDSA_ALGORITHMS: frozenset[grpc.experimental.PrivateKeySignatureAlgorithm] = frozenset(
    {
        _Algorithm.ECDSA_SECP256R1_SHA256,
        _Algorithm.ECDSA_SECP384R1_SHA384,
        _Algorithm.ECDSA_SECP521R1_SHA512,
    }
)

# RSA public-key OID (X.509 AlgorithmIdentifier)
_OID_RSA = "1.2.840.113549.1.1.1"
# EC public-key OID
_OID_EC = "1.2.840.10045.2.1"


def _encode_ecdsa_der(raw_sig: bytes) -> bytes:
    """Convert Windows CNG ECDSA P1363 (r||s) signature to DER ASN.1.

    .NET's ``ECDsa.SignData`` returns raw r||s bytes (IEEE P1363).
    gRPC/BoringSSL expects DER-encoded SEQUENCE { INTEGER r, INTEGER s }.
    """
    if len(raw_sig) == 0 or len(raw_sig) % 2 != 0:
        raise SmartcardConfigurationError(
            f"Unexpected ECDSA signature length {len(raw_sig)}; expected a non-empty even number of bytes."
        )
    half = len(raw_sig) // 2
    r = int.from_bytes(raw_sig[:half], "big")
    s = int.from_bytes(raw_sig[half:], "big")
    return encode_dss_signature(r, s)


class WindowsCngSigner:
    r"""Windows CNG-backed signing for gRPC's custom signer TLS credentials.

    Uses pythonnet to access the certificate's private key through the Windows CNG
    (Cryptography Next Generation) API. Works with any key in ``CurrentUser\My``
    including smart card keys backed by ``Microsoft Smart Card Key Storage Provider``
    — no PKCS#11 or OpenSC DLL required.

    PIN prompting is handled natively by Windows (via the smart card UI) the first
    time the private key is used for signing.

    Certificate selection:
        By default the first unexpired cert with a private key found in
        ``CurrentUser\My`` is used. Set the ``NOMINAL_WINDOWS_CERT_THUMBPRINT``
        environment variable (or pass ``cert_thumbprint`` directly) to pin a
        specific certificate.
    """

    def __init__(self, *, cert_thumbprint: str | None = None) -> None:
        self._cert_thumbprint = cert_thumbprint
        self._lock = threading.Lock()
        self._cert: Any = None
        self._der_bytes: bytes | None = None
        self._pub_key_oid: str | None = None

    @classmethod
    def from_environment(cls) -> WindowsCngSigner:
        """Construct using ``NOMINAL_WINDOWS_CERT_THUMBPRINT`` env var (may be unset)."""
        thumbprint = os.environ.get(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR) or None
        return cls(cert_thumbprint=thumbprint)

    def connect(self) -> None:
        r"""Load the signing certificate from ``CurrentUser\My`` and prime the CNG key.

        Must be called before ``sign()``. Idempotent.

        Performs a warmup sign to trigger PIN entry (if required) and initialize the CNG
        key handle before gRPC calls ``sign()`` from its internal handshake thread.
        Windows may not be able to show the CAC PIN dialog from gRPC's native thread, so
        we do it here in the caller's thread instead.
        """
        with self._lock:
            if self._cert is not None:
                return
            self._cert, self._der_bytes, self._pub_key_oid = _load_cert(self._cert_thumbprint)
            _warmup_sign(self._cert, self._pub_key_oid)

    @property
    def der_certificate(self) -> bytes:
        """DER-encoded certificate bytes for building the gRPC credential chain."""
        if self._der_bytes is None:
            raise SmartcardConfigurationError("WindowsCngSigner.connect() has not been called yet.")
        return self._der_bytes

    def sign(
        self,
        data_to_sign: bytes,
        signature_algorithm: grpc.experimental.PrivateKeySignatureAlgorithm,
        on_complete: Any,
    ) -> bytes:
        """Sign ``data_to_sign`` on the smart card via Windows CNG and return raw signature bytes."""
        with self._lock:
            cert = self._cert
            pub_key_oid = self._pub_key_oid
        if cert is None or pub_key_oid is None:
            raise SmartcardConfigurationError("WindowsCngSigner.connect() has not been called yet.")
        try:
            return _sign_with_cert(cert, pub_key_oid, data_to_sign, signature_algorithm)
        except SmartcardConfigurationError:
            raise
        except Exception as exc:
            # Wrap .NET exceptions so the real cause is visible — gRPC would otherwise
            # silently swallow them as SSL_ERROR_SSL/PRIVATE_KEY_OPERATION_FAILED.
            raise SmartcardConfigurationError(
                f"Windows CNG signing failed (algorithm={signature_algorithm!r}): {type(exc).__name__}: {exc}"
            ) from exc

    def close(self) -> None:
        with self._lock:
            if self._cert is not None:
                try:
                    self._cert.Dispose()
                except Exception:
                    pass
                self._cert = None
                self._der_bytes = None
                self._pub_key_oid = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


def _warmup_sign(cert: Any, pub_key_oid: str) -> None:
    r"""Perform a test sign to prime the CNG key and trigger the Windows PIN prompt (if required).

    gRPC calls ``sign()`` from an internal native thread where the Windows smart card PIN
    dialog may not appear (or the key handle may not be initialized). Calling this in
    ``connect()`` — on the caller's thread — ensures PIN entry happens before the first
    TLS handshake.

    Raises ``SmartcardConfigurationError`` if the key is inaccessible.
    """
    algo = _Algorithm.RSA_PKCS1_SHA256 if pub_key_oid == _OID_RSA else _Algorithm.ECDSA_SECP256R1_SHA256
    try:
        _sign_with_cert(cert, pub_key_oid, b"\x00" * 32, algo)
    except SmartcardConfigurationError:
        raise
    except Exception as exc:
        raise SmartcardConfigurationError(
            f"Windows CNG key warmup failed — the private key may be inaccessible or "
            f"PIN entry was cancelled: {type(exc).__name__}: {exc}"
        ) from exc


def _load_cert(cert_thumbprint: str | None) -> tuple[Any, bytes, str]:
    r"""Open ``CurrentUser\My``, find the certificate, and return ``(cert, der_bytes, pub_key_oid)``.

    Raises ``SmartcardConfigurationError`` when no matching certificate is found.
    All .NET imports happen here so this module can be imported on non-Windows platforms.
    """
    import clr  # noqa: PLC0415

    clr.AddReference("System.Security")

    from System import DateTime  # type: ignore[import]
    from System.Security.Cryptography.X509Certificates import (  # type: ignore[import]
        OpenFlags,
        StoreLocation,
        StoreName,
        X509FindType,
        X509Store,
    )

    store = X509Store(StoreName.My, StoreLocation.CurrentUser)
    store.Open(OpenFlags.ReadOnly)
    try:
        if cert_thumbprint:
            thumbprint = cert_thumbprint.replace(" ", "").upper()
            matches = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, False)
            if matches.Count < 1:
                raise SmartcardConfigurationError(
                    f"Certificate with thumbprint {thumbprint!r} not found in CurrentUser\\My. "
                    "Verify the smart card is inserted and the middleware is running."
                )
            cert = matches[0]
        else:
            now = DateTime.Now
            cert = None
            for c in store.Certificates:
                if c.HasPrivateKey and c.NotBefore <= now <= c.NotAfter:
                    cert = c
                    break
            if cert is None:
                raise SmartcardConfigurationError(
                    "No unexpired certificate with a private key found in CurrentUser\\My. "
                    "Insert your CAC/smart card and ensure the Windows Smart Card service is running."
                )

        der_bytes = bytes(cert.RawData)
        pub_key_oid = str(cert.PublicKey.Oid.Value)
        return cert, der_bytes, pub_key_oid
    finally:
        store.Close()


def _sign_with_cert(
    cert: Any,
    pub_key_oid: str,
    data_to_sign: bytes,
    signature_algorithm: grpc.experimental.PrivateKeySignatureAlgorithm,
) -> bytes:
    """Dispatch signing to the RSA or ECDSA CNG key attached to ``cert``."""
    from System import Array, Byte  # type: ignore[import]
    from System.Security.Cryptography import HashAlgorithmName, RSASignaturePadding  # type: ignore[import]

    data_net = Array[Byte](data_to_sign)

    if pub_key_oid == _OID_RSA:
        # GetRSAPrivateKey() returns RSACng for CNG-backed keys (e.g. smart card KSP).
        # cert.PrivateKey returns the legacy RSACryptoServiceProvider which fails with
        # "Invalid provider type specified" for CNG storage providers.
        key = cert.GetRSAPrivateKey()
        if key is None:
            raise SmartcardConfigurationError(
                "Certificate has no RSA private key accessible via CNG. "
                "Ensure the smart card is inserted and the Windows Smart Card service is running."
            )
        try:
            return _sign_rsa(key, data_net, signature_algorithm, HashAlgorithmName, RSASignaturePadding)
        finally:
            key.Dispose()

    if pub_key_oid == _OID_EC:
        key = cert.GetECDsaPrivateKey()
        if key is None:
            raise SmartcardConfigurationError(
                "Certificate has no ECDSA private key accessible via CNG. "
                "Ensure the smart card is inserted and the Windows Smart Card service is running."
            )
        try:
            return _sign_ecdsa(key, data_net, signature_algorithm, HashAlgorithmName)
        finally:
            key.Dispose()

    raise SmartcardConfigurationError(
        f"Unsupported public key algorithm OID {pub_key_oid!r}. Only RSA and ECDSA certificates are supported."
    )


def _sign_rsa(
    key: Any,
    data_net: Any,
    alg: grpc.experimental.PrivateKeySignatureAlgorithm,
    HashAlgorithmName: Any,
    RSASignaturePadding: Any,
) -> bytes:
    if alg == _Algorithm.RSA_PKCS1_SHA256:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
    if alg == _Algorithm.RSA_PKCS1_SHA384:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA384, RSASignaturePadding.Pkcs1))
    if alg == _Algorithm.RSA_PKCS1_SHA512:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA512, RSASignaturePadding.Pkcs1))
    if alg == _Algorithm.RSA_PSS_RSAE_SHA256:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA256, RSASignaturePadding.Pss))
    if alg == _Algorithm.RSA_PSS_RSAE_SHA384:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA384, RSASignaturePadding.Pss))
    if alg == _Algorithm.RSA_PSS_RSAE_SHA512:
        return bytes(key.SignData(data_net, HashAlgorithmName.SHA512, RSASignaturePadding.Pss))
    raise SmartcardConfigurationError(
        f"Algorithm {alg!r} is not an RSA algorithm. "
        "Check that the gRPC server advertises algorithms compatible with your RSA certificate."
    )


def _sign_ecdsa(
    key: Any,
    data_net: Any,
    alg: grpc.experimental.PrivateKeySignatureAlgorithm,
    HashAlgorithmName: Any,
) -> bytes:
    if alg == _Algorithm.ECDSA_SECP256R1_SHA256:
        raw = bytes(key.SignData(data_net, HashAlgorithmName.SHA256))
    elif alg == _Algorithm.ECDSA_SECP384R1_SHA384:
        raw = bytes(key.SignData(data_net, HashAlgorithmName.SHA384))
    elif alg == _Algorithm.ECDSA_SECP521R1_SHA512:
        raw = bytes(key.SignData(data_net, HashAlgorithmName.SHA512))
    else:
        raise SmartcardConfigurationError(
            f"Algorithm {alg!r} is not an ECDSA algorithm. "
            "Check that the gRPC server advertises algorithms compatible with your ECDSA certificate."
        )
    # .NET returns IEEE P1363 (r||s); gRPC/BoringSSL expects DER ASN.1.
    return _encode_ecdsa_der(raw)
