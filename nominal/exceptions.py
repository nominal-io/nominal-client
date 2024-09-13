class NominalError(Exception):
    """Base class for Nominal exceptions."""


class NominalIngestError(NominalError):
    """An error occurred during ingest."""


class NominalIngestFailed(NominalIngestError):
    """The ingest failed."""


class NominalMultipartUploadFailed(NominalError):
    """The multipart upload failed."""


class NominalConfigError(NominalError):
    """An error occurred reading or writing the configuration."""
