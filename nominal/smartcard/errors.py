from __future__ import annotations

from nominal.core.exceptions import NominalError


class SmartcardError(NominalError):
    """Base class for smartcard authentication errors."""


class SmartcardDependencyError(SmartcardError):
    """Required smartcard optional dependencies are not installed."""


class SmartcardConfigurationError(SmartcardError):
    """Smartcard configuration or local machine setup is invalid."""


class SmartcardCertificateSelectionError(SmartcardError):
    """The PIV Authentication certificate could not be selected deterministically."""


class SmartcardNotImplementedError(SmartcardError, NotImplementedError):
    """Smartcard hardware/provider integration is intentionally not implemented yet."""
