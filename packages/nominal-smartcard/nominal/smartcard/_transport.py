from __future__ import annotations

import platform
import ssl
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding
from grpc.experimental import ssl_channel_credentials_with_custom_signer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nominal.core._utils.networking import NominalRequestsAdapter, TransportProvider
from nominal.smartcard._errors import (
    SmartcardConfigurationError,
    SmartcardPinError,
    SmartcardPinLockedError,
    SmartcardProviderError,
)

if TYPE_CHECKING:
    from nominal.smartcard._openssl_provider import OpenSslProviderBridge
    from nominal.smartcard._session import SmartcardSessionManager
    from nominal.smartcard._windows_cert_store import WindowsCertificateIdentity

MAX_PIN_ATTEMPTS = 3


@dataclass
class SmartcardTransportProvider(TransportProvider):
    r"""Transport provider that attaches smartcard-backed mTLS to Nominal API and gRPC traffic.

    HTTP path: ``create_http_adapter()`` branches on platform:
      - Windows: selects one certificate from ``CurrentUser\My`` and returns a
        ``WindowsCacAdapter`` (Schannel) using that exact identity.
      - Linux/macOS: returns a ``NominalRequestsAdapter`` backed by an OpenSSL+pkcs11
        ``ssl.SSLContext``. PIN prompting is handled at C-level by pkcs11-provider.

    gRPC path: ``create_grpc_channel_credentials()`` returns ``grpc.ChannelCredentials`` that
    use the same Windows certificate through CNG on Windows, and a PKCS#11 signing
    callback on Linux/macOS.

    Multipart path: inherits a plain ``NominalSslRequestsAdapter`` with no client certificate,
    since S3 presigned URLs use AWS auth.

    Both overridden paths cache the selected identity and transport credentials after the
    first successful call.
    """

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _windows_cert_thumbprint: str | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)
    _cached_grpc_credentials: dict[tuple[bytes | None, bytes | None], Any] = field(
        default_factory=dict, repr=False, compare=False
    )
    _signers: list[Any] = field(default_factory=list, repr=False, compare=False)
    _windows_identity: WindowsCertificateIdentity | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls, *, windows_cert_thumbprint: str | None = None) -> SmartcardTransportProvider:
        return cls(_windows_cert_thumbprint=windows_cert_thumbprint)

    @property
    def session_manager(self) -> SmartcardSessionManager:
        if self._session_manager is not None:
            return self._session_manager
        from nominal.smartcard._session import SmartcardSessionManager

        return SmartcardSessionManager.shared()

    @property
    def openssl_bridge(self) -> OpenSslProviderBridge:
        if self._openssl_bridge is not None:
            return self._openssl_bridge
        from nominal.smartcard._openssl_provider import OpenSslProviderBridge

        return OpenSslProviderBridge()

    def create_http_adapter(self, *, max_retries: Retry) -> HTTPAdapter:
        """Return a ``WindowsCacAdapter`` on Windows, a pkcs11-backed ``NominalRequestsAdapter`` elsewhere."""
        if platform.system() == "Windows":
            from nominal.smartcard._windows_cac import WindowsCacAdapter

            identity = self._get_windows_identity()
            return WindowsCacAdapter(max_retries=max_retries, client_certificate=identity.certificate)

        return NominalRequestsAdapter(
            max_retries=max_retries,
            ssl_context=self._build_pkcs11_ssl_context(),
        )

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
        certificate from the selected certificate is used.
        """
        cache_key = (root_certificates, certificate_chain_pem)
        with self._lock:
            cached_credentials = self._cached_grpc_credentials.get(cache_key)
            if cached_credentials is not None:
                return cached_credentials

            if platform.system() == "Windows":
                credentials, signer = self._create_grpc_credentials_windows(
                    root_certificates=root_certificates,
                    certificate_chain_pem=certificate_chain_pem,
                )
            else:
                credentials, signer = self._create_grpc_credentials_pkcs11(
                    root_certificates=root_certificates,
                    certificate_chain_pem=certificate_chain_pem,
                )
            self._cached_grpc_credentials[cache_key] = credentials
            self._signers.append(signer)
            return credentials

    def close(self) -> None:
        """Release smartcard resources held by gRPC signers and cached Windows identities."""
        with self._lock:
            signers = list(self._signers)
            self._signers.clear()
            self._cached_grpc_credentials.clear()
            windows_identity = self._windows_identity
            self._windows_identity = None
        for signer in signers:
            signer.close()
        if windows_identity is not None:
            windows_identity.close()

    def _get_windows_identity(self) -> WindowsCertificateIdentity:
        with self._lock:
            if self._windows_identity is None:
                from nominal.smartcard._windows_cert_store import WindowsCertificateSelector

                self._windows_identity = WindowsCertificateSelector.from_environment(
                    cert_thumbprint=self._windows_cert_thumbprint
                ).select()
            return self._windows_identity

    def _create_grpc_credentials_windows(
        self,
        *,
        root_certificates: bytes | None,
        certificate_chain_pem: bytes | None,
    ) -> tuple[Any, Any]:
        from nominal.smartcard._windows_cng_signer import WindowsCngSigner

        identity = self._get_windows_identity()
        signer = WindowsCngSigner(identity=identity)
        signer.connect()
        try:
            if certificate_chain_pem is None:
                certificate_chain_pem = _pem_from_der_certificate(
                    identity.der_certificate,
                    empty_message=(
                        "Windows certificate DER data is empty; cannot build PEM chain for gRPC credentials."
                    ),
                )
            credentials = ssl_channel_credentials_with_custom_signer(
                private_key_sign_fn=signer.sign,
                root_certificates=root_certificates,
                certificate_chain=certificate_chain_pem,
            )
        except:
            signer.close()
            raise
        return credentials, signer

    def _create_grpc_credentials_pkcs11(
        self,
        *,
        root_certificates: bytes | None,
        certificate_chain_pem: bytes | None,
    ) -> tuple[Any, Any]:
        from nominal.smartcard._grpc_signer import SmartcardPrivateKeySigner

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
        signer.connect()
        try:
            if certificate_chain_pem is None:
                certificate_chain_pem = _pem_from_der_certificate(
                    session.certificate.der_certificate,
                    empty_message=(
                        "Certificate DER data is empty; cannot build PEM chain for gRPC credentials. "
                        "The PKCS#11 token may not have returned a certificate value."
                    ),
                )
            credentials = ssl_channel_credentials_with_custom_signer(
                private_key_sign_fn=signer.sign,
                root_certificates=root_certificates,
                certificate_chain=certificate_chain_pem,
            )
        except:
            signer.close()
            raise
        return credentials, signer

    def _build_pkcs11_ssl_context(self) -> ssl.SSLContext:
        """Lazily build (and cache) the OpenSSL+pkcs11 SSL context, prompting for PIN on first use."""
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


def _pem_from_der_certificate(der_certificate: bytes, *, empty_message: str) -> bytes:
    if not der_certificate:
        raise SmartcardConfigurationError(empty_message)
    cert = x509.load_der_x509_certificate(der_certificate)
    return cert.public_bytes(Encoding.PEM)
