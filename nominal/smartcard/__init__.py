from nominal.smartcard._cert_selection import CertificateCandidate, select_piv_authentication_certificate
from nominal.smartcard._config import SmartcardConfig
from nominal.smartcard._dependencies import assert_required_dependencies_available
from nominal.smartcard._openssl_provider import OpenSslProviderBridge
from nominal.smartcard._pkcs11 import (
    NOMINAL_PKCS11_MODULE_ENV_VAR,
    Pkcs11Backend,
    PyKCS11Backend,
    discover_pkcs11_module,
)
from nominal.smartcard._session import SmartcardSession, SmartcardSessionManager
from nominal.smartcard._transport import SmartcardSslContextProvider
from nominal.smartcard.errors import (
    SmartcardCertificateSelectionError,
    SmartcardConfigurationError,
    SmartcardDependencyError,
    SmartcardError,
    SmartcardNotImplementedError,
    SmartcardPinError,
    SmartcardPinLockedError,
)

__all__ = [
    "CertificateCandidate",
    "NOMINAL_PKCS11_MODULE_ENV_VAR",
    "OpenSslProviderBridge",
    "Pkcs11Backend",
    "PyKCS11Backend",
    "SmartcardCertificateSelectionError",
    "SmartcardConfig",
    "SmartcardConfigurationError",
    "SmartcardDependencyError",
    "SmartcardError",
    "SmartcardNotImplementedError",
    "SmartcardPinError",
    "SmartcardPinLockedError",
    "SmartcardSession",
    "SmartcardSessionManager",
    "SmartcardSslContextProvider",
    "assert_required_dependencies_available",
    "discover_pkcs11_module",
    "select_piv_authentication_certificate",
]
