"""Shared exceptions that configuration can import without initializing nominal.core."""


class NominalError(Exception):
    """Base class for Nominal exceptions."""


class NominalConfigError(NominalError):
    """An error occurred reading or writing the configuration."""
