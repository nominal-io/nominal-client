from __future__ import annotations

from enum import Enum

from nominal.exceptions import NominalError
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2


class FileStoreErrorCode(Enum):
    """Why a drive operation was rejected, by the backend or by this SDK before sending it.

    `UNKNOWN` covers an unset code and any code a newer server sends that this SDK
    does not yet model.
    """

    UNKNOWN = "UNKNOWN"
    DRIVE_NOT_FOUND = "DRIVE_NOT_FOUND"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_REVISION_NOT_FOUND = "FILE_REVISION_NOT_FOUND"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    PATH_ALREADY_EXISTS = "PATH_ALREADY_EXISTS"
    INVALID_LOGICAL_PATH = "INVALID_LOGICAL_PATH"
    REVISION_PRECONDITION_FAILED = "REVISION_PRECONDITION_FAILED"
    READ_ONLY_DRIVE = "READ_ONLY_DRIVE"
    UPLOADED_OBJECT_NOT_FOUND = "UPLOADED_OBJECT_NOT_FOUND"
    FILE_HISTORY_NOT_AVAILABLE = "FILE_HISTORY_NOT_AVAILABLE"
    """Raised by this SDK, never the backend: a virtual drive's files keep no revision history."""

    @classmethod
    def _from_failure(cls, failure: files_pb2.FileChangeFailure) -> FileStoreErrorCode:
        # The backend reports a failure from one of two enums: errors any operation can hit, or
        # errors specific to applying a change.
        match failure.WhichOneof("error"):
            case "common":
                return cls._from_common(failure.common)
            case "change":
                return cls._from_change(failure.change)
            case _:
                return cls.UNKNOWN

    @classmethod
    def _from_common(cls, value: file_store_pb2.FileStoreCommonError.ValueType) -> FileStoreErrorCode:
        match value:
            case file_store_pb2.FILE_STORE_COMMON_ERROR_DRIVE_NOT_FOUND:
                return cls.DRIVE_NOT_FOUND
            case file_store_pb2.FILE_STORE_COMMON_ERROR_FILE_NOT_FOUND:
                return cls.FILE_NOT_FOUND
            case file_store_pb2.FILE_STORE_COMMON_ERROR_FILE_REVISION_NOT_FOUND:
                return cls.FILE_REVISION_NOT_FOUND
            case file_store_pb2.FILE_STORE_COMMON_ERROR_PERMISSION_DENIED:
                return cls.PERMISSION_DENIED
            case _:
                return cls.UNKNOWN

    @classmethod
    def _from_change(cls, value: files_pb2.FileChangeError.ValueType) -> FileStoreErrorCode:
        match value:
            case files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS:
                return cls.PATH_ALREADY_EXISTS
            case files_pb2.FILE_CHANGE_ERROR_INVALID_LOGICAL_PATH:
                return cls.INVALID_LOGICAL_PATH
            case files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED:
                return cls.REVISION_PRECONDITION_FAILED
            case files_pb2.FILE_CHANGE_ERROR_READ_ONLY_DRIVE:
                return cls.READ_ONLY_DRIVE
            case files_pb2.FILE_CHANGE_ERROR_UPLOADED_OBJECT_NOT_FOUND:
                return cls.UPLOADED_OBJECT_NOT_FOUND
            case _:
                return cls.UNKNOWN


class NominalFileStoreError(NominalError):
    """A drive operation was rejected.

    Raised both for failures the backend reports in-band for a change (which carry no gRPC
    status of their own) and for checks this SDK makes before spending a request, so one
    `except NominalFileStoreError` covers both. Failures the backend reports as a gRPC
    status, such as a missing drive on lookup, raise the matching general error instead
    (for example `NominalNotFoundError`).
    """

    def __init__(self, code: FileStoreErrorCode, message: str) -> None:
        """Initialize error with the error code and message."""
        self.code = code
        self.message = message
        super().__init__(f"{code.value}: {message}")
