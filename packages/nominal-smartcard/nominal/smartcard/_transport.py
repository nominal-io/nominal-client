from __future__ import annotations

import ssl
import threading
from dataclasses import dataclass, field

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nominal.core._utils.networking import NominalRequestsAdapter, TransportProvider
from nominal.smartcard._errors import SmartcardPinError, SmartcardPinLockedError, SmartcardProviderError
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._session import SmartcardSessionManager

MAX_PIN_ATTEMPTS = 3


@dataclass
class SmartcardTransportProvider(TransportProvider):
    """Transport provider that attaches smartcard-backed mTLS to Nominal API traffic.

    The smartcard ``ssl.SSLContext`` is built lazily (with PIN-retry) and reused for every
    API connection. Object-store multipart traffic inherits the default
    ``create_multipart_adapter()`` from the base class because S3 presigned URLs use AWS
    auth and do not need a client certificate.
    """

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)

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
        """Return a ``NominalRequestsAdapter`` backed by the smartcard ``ssl.SSLContext``."""
        return NominalRequestsAdapter(
            max_retries=max_retries,
            ssl_context=self._build_pkcs11_ssl_context(),
        )

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
