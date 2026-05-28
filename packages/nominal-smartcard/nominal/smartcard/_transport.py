from __future__ import annotations

import platform
import ssl
import threading
from dataclasses import dataclass, field
from typing import Any

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
from nominal.smartcard._grpc_signer import SmartcardPrivateKeySigner
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._session import SmartcardSessionManager
from nominal.smartcard._windows_cac import WindowsCacAdapter

MAX_PIN_ATTEMPTS = 3


@dataclass
class SmartcardTransportProvider(TransportProvider):
    """Transport provider that attaches smartcard-backed mTLS to Nominal API and gRPC traffic.

    HTTP path: ``create_http_adapter()`` branches on platform:
      - Windows: returns a ``WindowsCacAdapter`` (Schannel) so Windows handles CAC certificate
        selection and PIN prompting through the OS credential UI.
      - Linux/macOS: returns a ``NominalRequestsAdapter`` backed by an OpenSSL+pkcs11
        ``ssl.SSLContext``. PIN prompting is handled at C-level by pkcs11-provider.

    gRPC path: ``create_grpc_channel_credentials()`` returns ``grpc.ChannelCredentials`` that
    use a PKCS#11 signing callback so the private key never leaves the card. The same pkcs11
    path is used on all platforms since gRPC does not route through Schannel.

    Multipart path: inherits the base class default implementation with no client certificate
    since S3 presigned URLs use AWS auth.

    Both customised paths share certificate discovery (via ``SmartcardSessionManager``) and
    each caches its result after the first successful call.
    """

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)
    _cached_grpc_credentials: Any | None = field(default=None, repr=False, compare=False)
    _signer: SmartcardPrivateKeySigner | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls) -> SmartcardTransportProvider:
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

    def create_http_adapter(self, *, max_retries: Retry) -> HTTPAdapter:
        """Return a ``WindowsCacAdapter`` on Windows, a pkcs11-backed ``NominalRequestsAdapter`` elsewhere."""
        if platform.system() == "Windows":
            return WindowsCacAdapter(max_retries=max_retries)

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
            signer.connect()
            try:
                if certificate_chain_pem is None:
                    if not session.certificate.der_certificate:
                        raise SmartcardConfigurationError(
                            "Certificate DER data is empty; cannot build PEM chain for gRPC credentials. "
                            "The PKCS#11 token may not have returned a certificate value."
                        )
                    cert = x509.load_der_x509_certificate(session.certificate.der_certificate)
                    certificate_chain_pem = cert.public_bytes(Encoding.PEM)

                self._cached_grpc_credentials = ssl_channel_credentials_with_custom_signer(
                    private_key_sign_fn=signer.sign,
                    root_certificates=root_certificates,
                    certificate_chain=certificate_chain_pem,
                )
            except:
                signer.close()
                raise
            self._signer = signer
            return self._cached_grpc_credentials

    def close(self) -> None:
        """Release PKCS#11 session resources held by the gRPC signer."""
        with self._lock:
            if self._signer is not None:
                self._signer.close()
                self._signer = None
            self._cached_grpc_credentials = None

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
