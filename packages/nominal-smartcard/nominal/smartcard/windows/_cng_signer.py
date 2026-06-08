from __future__ import annotations

import threading
from typing import Any

import grpc.experimental

from nominal.smartcard._errors import SmartcardConfigurationError, SmartcardRuntimeError
from nominal.smartcard._signature_utils import encode_ecdsa_der
from nominal.smartcard.windows._cert_store import WindowsCertificateIdentity

_Algorithm = grpc.experimental.PrivateKeySignatureAlgorithm

# RSA public-key OID (X.509 AlgorithmIdentifier)
_OID_RSA = "1.2.840.113549.1.1.1"
# EC public-key OID
_OID_EC = "1.2.840.10045.2.1"


class WindowsCngSigner:
    r"""Windows CNG-backed signing for gRPC custom-signer TLS credentials.

    The selected ``X509Certificate2`` stays in the Windows certificate store and
    its private key remains managed by Windows/CNG, including smart-card PIN UI.
    """

    def __init__(self, *, identity: WindowsCertificateIdentity) -> None:
        self._identity = identity
        self._lock = threading.Lock()
        self._connected = False

    def connect(self) -> None:
        """Prime the CNG key on the caller's thread so Windows can show the PIN UI."""
        with self._lock:
            if self._connected:
                return
            _warmup_sign(self._identity.certificate, self._identity.public_key_oid)
            self._connected = True

    @property
    def der_certificate(self) -> bytes:
        return self._identity.der_certificate

    def sign(
        self,
        data_to_sign: bytes,
        signature_algorithm: grpc.experimental.PrivateKeySignatureAlgorithm,
        on_complete: Any,
    ) -> bytes:
        """Sign ``data_to_sign`` on the smart card via Windows CNG."""
        del on_complete
        with self._lock:
            if not self._connected:
                raise SmartcardRuntimeError("WindowsCngSigner.connect() has not been called yet.")
            try:
                return _sign_with_cert(
                    self._identity.certificate,
                    self._identity.public_key_oid,
                    data_to_sign,
                    signature_algorithm,
                )
            except SmartcardConfigurationError:
                raise
            except Exception as exc:
                raise SmartcardConfigurationError(
                    f"Windows CNG signing failed (algorithm={signature_algorithm!r}): {type(exc).__name__}: {exc}"
                ) from exc

    def close(self) -> None:
        with self._lock:
            self._connected = False


def _warmup_sign(cert: Any, public_key_oid: str) -> None:
    r"""Perform a test sign to trigger Windows PIN entry before gRPC's handshake thread."""
    algo = _Algorithm.RSA_PKCS1_SHA256 if public_key_oid == _OID_RSA else _Algorithm.ECDSA_SECP256R1_SHA256
    try:
        _sign_with_cert(cert, public_key_oid, b"\x00" * 32, algo)
    except SmartcardConfigurationError:
        raise
    except Exception as exc:
        raise SmartcardConfigurationError(
            "Windows CNG key warmup failed; the private key may be inaccessible or PIN entry was cancelled: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def _sign_with_cert(
    cert: Any,
    public_key_oid: str,
    data_to_sign: bytes,
    signature_algorithm: grpc.experimental.PrivateKeySignatureAlgorithm,
) -> bytes:
    """Dispatch signing to the RSA or ECDSA CNG key attached to ``cert``."""
    import clr  # type: ignore[import-untyped]  # noqa: PLC0415

    clr.AddReference("System.Core")

    from System import Array, Byte  # type: ignore[import-not-found]
    from System.Security.Cryptography import HashAlgorithmName, RSASignaturePadding  # type: ignore[import-not-found]
    from System.Security.Cryptography.X509Certificates import (  # type: ignore[import-not-found]
        ECDsaCertificateExtensions,
        RSACertificateExtensions,
    )

    data_net = Array[Byte](data_to_sign)

    if public_key_oid == _OID_RSA:
        # GetRSAPrivateKey is a C# extension method; pythonnet does not expose extension
        # methods as instance methods, so call it statically with the cert as first arg.
        key = RSACertificateExtensions.GetRSAPrivateKey(cert)
        if key is None:
            raise SmartcardConfigurationError(
                "Certificate has no RSA private key accessible via CNG. "
                "Ensure the smart card is inserted and the Windows Smart Card service is running."
            )
        try:
            return _sign_rsa(key, data_net, signature_algorithm, HashAlgorithmName, RSASignaturePadding)
        finally:
            key.Dispose()

    if public_key_oid == _OID_EC:
        key = ECDsaCertificateExtensions.GetECDsaPrivateKey(cert)
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
        f"Unsupported public key algorithm OID {public_key_oid!r}. Only RSA and ECDSA certificates are supported."
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
    return encode_ecdsa_der(raw)
