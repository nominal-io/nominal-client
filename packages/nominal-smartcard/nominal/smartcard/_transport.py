from __future__ import annotations

import ssl
import threading
from dataclasses import dataclass, field

from nominal.core._utils.networking import SslContextProvider
from nominal.smartcard._errors import SmartcardPinError, SmartcardPinLockedError, SmartcardProviderError
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._session import SmartcardSessionManager

MAX_PIN_ATTEMPTS = 3


@dataclass
class SmartcardSslContextProvider(SslContextProvider):
    """ssl.SSLContext provider that will attach smartcard-backed mTLS to all Nominal traffic."""

    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls) -> SmartcardSslContextProvider:
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

    def create_ssl_context(self) -> ssl.SSLContext:
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
            return self._cached_ctx

    def create_grpc_channel_credentials(self, *, root_certificates=None, certificate_chain_pem=None):
        raise NotImplementedError("gRPC channel credentials not yet implemented for smartcard auth")
