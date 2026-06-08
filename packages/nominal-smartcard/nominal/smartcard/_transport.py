from __future__ import annotations

import platform
import ssl
import threading
from abc import abstractmethod
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

    Use ``SmartcardTransportProvider.create()`` to obtain an instance; the factory selects
    the platform-appropriate subclass automatically.

    Subclasses implement ``create_http_adapter()`` and ``_build_grpc_credentials()``.
    Shared gRPC credential caching (keyed on root-CA / certificate-chain pair) lives here.

    Multipart path: inherits a plain ``NominalSslRequestsAdapter`` with no client certificate,
    since S3 presigned URLs use AWS auth.
    """

    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)
    _cached_grpc_credentials: dict[tuple[bytes | None, bytes | None], Any] = field(
        default_factory=dict, repr=False, compare=False
    )
    _signers: list[Any] = field(default_factory=list, repr=False, compare=False)

    @classmethod
    def create(cls) -> SmartcardTransportProvider:
        """Return the platform-appropriate smartcard transport provider."""
        if platform.system() == "Windows":
            return _WindowsSmartcardTransportProvider()

        return _Pkcs11SmartcardTransportProvider()

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

        Credentials are cached per ``(root_certificates, certificate_chain_pem)`` pair.
        """
        cache_key = (root_certificates, certificate_chain_pem)
        with self._lock:
            cached = self._cached_grpc_credentials.get(cache_key)
            if cached is not None:
                return cached
            credentials, signer = self._build_grpc_credentials(
                root_certificates=root_certificates,
                certificate_chain_pem=certificate_chain_pem,
            )
            self._cached_grpc_credentials[cache_key] = credentials
            self._signers.append(signer)
            return credentials

    @abstractmethod
    def _build_grpc_credentials(
        self,
        *,
        root_certificates: bytes | None,
        certificate_chain_pem: bytes | None,
    ) -> tuple[Any, Any]:
        """Build and return ``(credentials, signer)`` for this platform. Called under ``_lock``."""

    def close(self) -> None:
        """Release smartcard resources held by gRPC signers."""
        with self._lock:
            signers = list(self._signers)
            self._signers.clear()
            self._cached_grpc_credentials.clear()
        for signer in signers:
            signer.close()


@dataclass
class _Pkcs11SmartcardTransportProvider(SmartcardTransportProvider):
    """PKCS#11 transport provider for Linux and macOS.

    HTTP path: ``create_http_adapter()`` returns a ``NominalRequestsAdapter`` backed by an
    OpenSSL+pkcs11 ``ssl.SSLContext``. PIN prompting is handled at C-level by pkcs11-provider.

    gRPC path: ``_build_grpc_credentials()`` returns credentials backed by a PKCS#11 signing
    callback.

    Both overridden paths cache the result after the first successful call.
    """

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)

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
        """Return a ``NominalRequestsAdapter`` backed by the smartcard ``ssl.SSLContext``."""
        return NominalRequestsAdapter(
            max_retries=max_retries,
            ssl_context=self._build_pkcs11_ssl_context(),
        )

    def _build_grpc_credentials(
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
        except Exception:
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


@dataclass
class _WindowsSmartcardTransportProvider(SmartcardTransportProvider):
    r"""Windows transport provider backed by Schannel (HTTP) and Windows CNG (gRPC).

    A single Windows client-auth certificate is selected once from ``CurrentUser\My``
    and shared by both transports so every connection presents the same CAC identity.

    HTTP path: ``create_http_adapter()`` returns a ``WindowsCacAdapter`` that drives the
    .NET ``HttpClient`` over Schannel. PIN prompting is handled by the Windows credential UI.

    gRPC path: ``_build_grpc_credentials()`` returns credentials backed by a
    ``WindowsCngSigner`` whose private key stays managed by Windows/CNG.

    Multipart path: inherits the default ``NominalSslRequestsAdapter`` (no client
    certificate), since S3 presigned URLs use AWS auth and validate against system trust.
    """

    _windows_identity: WindowsCertificateIdentity | None = field(default=None, repr=False, compare=False)

    @property
    def windows_identity(self) -> WindowsCertificateIdentity:
        """Lazily select (and cache) the shared Windows CAC certificate."""
        with self._lock:
            if self._windows_identity is None:
                from nominal.smartcard._windows_cert_store import select_windows_certificate

                self._windows_identity = select_windows_certificate()
            return self._windows_identity

    def close(self) -> None:
        """Release gRPC signers and dispose the shared Windows certificate handle."""
        super().close()
        with self._lock:
            identity = self._windows_identity
            self._windows_identity = None
        if identity is not None:
            identity.close()

    def create_http_adapter(self, *, max_retries: Retry) -> HTTPAdapter:
        """Return a ``WindowsCacAdapter`` backed by the shared Windows certificate."""
        from nominal.smartcard._windows_cac import WindowsCacAdapter

        return WindowsCacAdapter(
            max_retries=max_retries,
            client_certificate=self.windows_identity.certificate,
        )

    def _build_grpc_credentials(
        self,
        *,
        root_certificates: bytes | None,
        certificate_chain_pem: bytes | None,
    ) -> tuple[Any, Any]:
        from nominal.smartcard._windows_cng_signer import WindowsCngSigner

        signer = WindowsCngSigner(identity=self.windows_identity)
        signer.connect()
        try:
            if certificate_chain_pem is None:
                certificate_chain_pem = _pem_from_der_certificate(
                    signer.der_certificate,
                    empty_message=(
                        "Certificate DER data is empty; cannot build PEM chain for gRPC credentials. "
                        "The Windows certificate store may not have returned a certificate value."
                    ),
                )
            credentials = ssl_channel_credentials_with_custom_signer(
                private_key_sign_fn=signer.sign,
                root_certificates=root_certificates,
                certificate_chain=certificate_chain_pem,
            )
        except Exception:
            signer.close()
            raise
        return credentials, signer


def _pem_from_der_certificate(der_certificate: bytes, *, empty_message: str) -> bytes:
    if not der_certificate:
        raise SmartcardConfigurationError(empty_message)
    cert = x509.load_der_x509_certificate(der_certificate)
    return cert.public_bytes(Encoding.PEM)
