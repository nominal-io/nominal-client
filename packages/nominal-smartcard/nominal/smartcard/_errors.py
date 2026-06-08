from __future__ import annotations

from nominal.core.exceptions import NominalError


class SmartcardError(NominalError):
    """Base class for smartcard authentication errors."""


class SmartcardConfigurationError(SmartcardError):
    """Smartcard configuration or local machine setup is invalid."""


class SmartcardRuntimeError(SmartcardError):
    """A smartcard transport object was used incorrectly, e.g. an operation invoked in an invalid state."""


class SmartcardProviderError(SmartcardError):
    """The PKCS#11 provider returned an unexpected error that could not be classified."""


class SmartcardCertificateSelectionError(SmartcardError):
    """The PIV Authentication certificate could not be selected deterministically."""


class SmartcardPinError(SmartcardError):
    """The PIN was rejected by the smartcard."""


class SmartcardPinLockedError(SmartcardPinError):
    """The PIN is locked due to too many incorrect attempts."""
