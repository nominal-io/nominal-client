from typing import Mapping


class NominalError(Exception):
    """Base class for Nominal exceptions."""


class NominalIngestError(NominalError):
    """An error occurred during ingest."""


class NominalIngestMultiError(NominalError):
    """Error(s) occurred during ingest.

    Attributes:
        errors: A mapping of dataset RIDs to the errors that occurred during ingest.
    """

    def __init__(self, rids_to_errors: Mapping[str, NominalIngestError]) -> None:
        self.errors = rids_to_errors

    def __str__(self) -> str:
        return f"{len(self.errors)} errors occurred during ingest: {self.errors}"


class NominalIngestFailed(NominalIngestError):
    """The ingest failed."""


class NominalMultipartUploadFailed(NominalError):
    """The multipart upload failed."""


class NominalConfigError(NominalError):
    """An error occurred reading or writing the configuration."""
