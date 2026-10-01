"""Shared base exceptions for Nominal packages.

Catch NominalError to handle Nominal-defined exceptions across components.
Component-specific exceptions live in each component's exceptions module.
"""


class NominalError(Exception):
    """Base class for Nominal exceptions."""
