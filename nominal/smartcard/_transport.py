from __future__ import annotations

import getpass
import ssl
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from nominal.core._utils.networking import SslContextProvider
from nominal.smartcard._dependencies import assert_required_dependencies_available
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._session import SmartcardSessionManager

PinProvider = Callable[[str], str]


@dataclass
class SmartcardSslContextProvider(SslContextProvider):
    """ssl.SSLContext provider that will attach smartcard-backed mTLS to all Nominal traffic."""

    pin_provider: PinProvider = field(default=getpass.getpass, repr=False, compare=False)
    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _cached_ctx: ssl.SSLContext | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls) -> SmartcardSslContextProvider:
        assert_required_dependencies_available()
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
                self._cached_ctx = self.openssl_bridge.build_ssl_context(
                    session=session, pin=self.pin_provider("Card PIN: ")
                )
            return self._cached_ctx
