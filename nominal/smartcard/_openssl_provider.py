from __future__ import annotations

import ssl
from dataclasses import dataclass

from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._session import SmartcardSession
from nominal.smartcard.errors import SmartcardNotImplementedError


@dataclass(frozen=True)
class OpenSslProviderBridge:
    """Bridge from Python requests/urllib3 to OpenSSL's pkcs11-provider.

    The eventual implementation belongs here: load the OpenSSL provider, open the selected PKCS#11 URI through
    OSSL_STORE, attach the smartcard-backed cert/private key to SSL_CTX, and preserve hostname/chain verification.
    """

    config: SmartcardConfig

    def build_ssl_context(self, *, session: SmartcardSession) -> ssl.SSLContext:
        del session
        raise SmartcardNotImplementedError("OpenSSL pkcs11-provider TLS integration is not implemented yet.")
