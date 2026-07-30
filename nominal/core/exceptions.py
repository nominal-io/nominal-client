from typing import Mapping

# Remove this import once the minimum supported Python version is 3.11+.
from exceptiongroup import ExceptionGroup


class NominalError(Exception):
    """Base class for Nominal exceptions."""


class LegacyVideoDeprecationWarning(DeprecationWarning):
    """Emitted by the legacy standalone-video API, superseded by video channels on a dataset.

    Subclasses DeprecationWarning so it can be filtered on its own without muting every other
    deprecation, e.g. `warnings.filterwarnings("ignore", category=LegacyVideoDeprecationWarning)`.
    """


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


class NominalMultipartUploadError(NominalError):
    """A single failed multipart upload attempt."""


class NominalMultipartUploadFailed(NominalError, ExceptionGroup):
    """The multipart upload failed after retries."""


class NominalRequestThrottledError(NominalError):
    """The server throttled a request for longer than the caller's retry budget allowed."""


class NominalIngestUploadFailed(NominalError, ExceptionGroup):
    """One or more files in an ingest batch failed to upload; nothing was ingested.

    Each member exception names the file it belongs to and carries the underlying failure as
    its `__cause__`.
    """


class NominalConfigError(NominalError):
    """An error occurred reading or writing the configuration."""


class NominalPermissionDeniedError(NominalError):
    """The caller is authenticated but not authorized for the requested resource."""


class NominalAuthenticationError(NominalError):
    """The request was not authenticated (missing, expired, or invalid credentials)."""


class NominalNotFoundError(NominalError):
    """The requested resource does not exist."""


class NominalAlreadyExistsError(NominalError):
    """The resource being created already exists (e.g. a container image tag already registered)."""


class NominalInvalidArgumentError(NominalError):
    """The server rejected the request as malformed or invalid."""


class HeaderConflictError(NominalError):
    """A header provider attempted to override an explicit request header."""


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


class NominalComputeError(NominalError):
    """An error occurred during a compute request."""


class NominalContainerImageError(NominalError):
    """A containerized extractor's container image is in a failed or unusable state."""


class ExtractorError(NominalError):
    """Raised when the extractor contract is violated (missing input, wrong output count, ...).

    Exported as `nominal.experimental.extractor.ExtractorError`, which is where authors meet it.
    """


# Every surface that anchors a video offers the same choice under different names -- an absolute
# start, per-frame timestamps -- and accepts exactly one. Shared so the wording cannot drift between
# them, and used as the default message below.
ONE_TIMESTAMP_MODE_ERROR = "exactly one of 'start' or 'frame_timestamps' must be provided"


class NominalVideoTimestampModeError(NominalError, ValueError):
    """Neither or both of a video's timestamp modes were provided; exactly one is required.

    Also a `ValueError`, which is what these call sites raised before this type existed, so
    `except ValueError` keeps working.

    The default message names the two modes every video surface offers. The one surface that accepts
    a third, `_build_video_file_timestamp_manifest`, passes its own.
    """

    def __init__(self, message: str = ONE_TIMESTAMP_MODE_ERROR) -> None:
        """Initialize error."""
        super().__init__(message)


class NominalVideoScaleModeError(NominalError, ValueError):
    """More than one way of scaling a video's playback rate was given; at most one is allowed.

    `ending_timestamp`, `true_frame_rate`, and `scale_factor` express the same quantity three ways.
    Also a `ValueError`, which is what this raised before the type existed.
    """

    def __init__(
        self,
        message: str = (
            "Expected at most one of 'ending_timestamp', 'true_frame_rate', and 'scale_factor' to be present"
        ),
    ) -> None:
        """Initialize error."""
        super().__init__(message)


class NominalVideoStreamError(NominalError):
    """An error occurred during live video streaming."""


class NominalVideoStreamNotOpenError(NominalVideoStreamError):
    """The video stream is not open. Call open() first or use as a context manager."""

    def __init__(self) -> None:
        """Initialize error."""
        super().__init__("VideoStream is not open — call open() first or use as a context manager")


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
