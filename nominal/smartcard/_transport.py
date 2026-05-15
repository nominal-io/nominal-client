from __future__ import annotations

import getpass
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field

from nominal.core._utils.networking import SslContextProvider
from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._dependencies import assert_required_dependencies_available
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._session import SmartcardSessionManager

PinProvider = Callable[[str], str]


@dataclass(frozen=True)
class SmartcardSslContextProvider(SslContextProvider):
    """ssl.SSLContext provider that will attach CAC-backed mTLS to all Nominal traffic."""

    config: SmartcardConfig = field(default_factory=SmartcardConfig)
    pin_provider: PinProvider = field(default=getpass.getpass, repr=False, compare=False)
    _session_manager: SmartcardSessionManager | None = field(default=None, repr=False, compare=False)
    _openssl_bridge: OpenSslProviderBridge | None = field(default=None, repr=False, compare=False)

    @classmethod
    def create(cls, config: SmartcardConfig | None = None) -> SmartcardSslContextProvider:
        assert_required_dependencies_available()
        return cls(config=SmartcardConfig() if config is None else config)

    @property
    def session_manager(self) -> SmartcardSessionManager:
        if self._session_manager is not None:
            return self._session_manager
        return SmartcardSessionManager.shared(self.config)

    @property
    def openssl_bridge(self) -> OpenSslProviderBridge:
        if self._openssl_bridge is not None:
            return self._openssl_bridge
        return OpenSslProviderBridge(self.config)

    def create_ssl_context(self) -> ssl.SSLContext:
        session = self.session_manager.get_session()
        return self.openssl_bridge.build_ssl_context(session=session, pin=self.pin_provider("CAC PIN: "))
