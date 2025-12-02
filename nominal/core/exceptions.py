from typing import Mapping


class NominalError(Exception):
    """Base class for Nominal exceptions."""


class NominalIngestError(NominalError):
    """An error occurred during ingest."""


class NominalIngestMultiError(NominalError):
    """Error(s) occurred during ingest.

    Attributes:
    ----------
        errors: A mapping of dataset RIDs to the errors that occurred during ingest.

    """

    def __init__(self, rids_to_errors: Mapping[str, NominalIngestError]) -> None:
        """Initialize multi-error with the rids to error on."""
        self.errors = rids_to_errors

    def __str__(self) -> str:
        """String repr."""
        return f"{len(self.errors)} errors occurred during ingest: {self.errors}"


class NominalIngestFailed(NominalIngestError):
    """The ingest failed."""


class NominalMultipartUploadFailed(NominalError):
    """The multipart upload failed."""


class NominalConfigError(NominalError):
    """An error occurred reading or writing the configuration."""


class NominalMethodRemovedError(NominalError):
    """An error raised when a method has been deprecated and now removed.
    Error informs users of the new method to use instead.
    """

    def __init__(self, original_method: str, instructions: str | None = None):
        """Initialize error with the method removed and method to use instead."""
        self._original_method = original_method
        self._instructions = instructions

    def __str__(self) -> str:
        """String repr."""
        base_msg = f"'{self._original_method}' was deprecated and has now been removed."
        if self._instructions is None:
            return f"{base_msg} Contact your Nominal Representative if you need this functionality."
        else:
            return f"{base_msg} To fix: {self._instructions}"


class NominalParameterRemovedError(NominalError):
    """An error raised when an parameter has been deprecated and now removed."""

    def __init__(self, method_name: str, parameter_name: str, instructions: str | None = None):
        """Initialize error with the method removed and method to use instead."""
        self._method_name = method_name
        self._parameter_name = parameter_name
        self._instructions = instructions

    def __str__(self) -> str:
        """String repr."""
        base_msg = (
            f"Parameter '{self._parameter_name}' was deprecated and has now been removed from '{self._method_name}'."
        )
        if self._instructions is None:
            return f"{base_msg} Contact your Nominal Representative if you need this functionality."
        else:
            return f"{base_msg} To fix: {self._instructions}"
