from __future__ import annotations

from enum import Enum

from nominal.exceptions import NominalError
from nominal.protos.file_store.v1 import file_store_pb2


class FileStoreErrorCode(Enum):
    """A File Store error code reported by the backend.

    `UNKNOWN` covers an unset code and any code a newer server sends that this SDK
    does not yet model.
    """

    UNKNOWN = "UNKNOWN"
    DRIVE_NOT_FOUND = "DRIVE_NOT_FOUND"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_REVISION_NOT_FOUND = "FILE_REVISION_NOT_FOUND"
    PATH_ALREADY_EXISTS = "PATH_ALREADY_EXISTS"
    INVALID_LOGICAL_PATH = "INVALID_LOGICAL_PATH"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    REVISION_PRECONDITION_FAILED = "REVISION_PRECONDITION_FAILED"
    READ_ONLY_DRIVE = "READ_ONLY_DRIVE"
    FILE_HISTORY_NOT_AVAILABLE = "FILE_HISTORY_NOT_AVAILABLE"
    REVISION_BYTES_RECLAIMED = "REVISION_BYTES_RECLAIMED"
    DRIVE_ID_ALREADY_EXISTS = "DRIVE_ID_ALREADY_EXISTS"
    UPLOADED_OBJECT_NOT_FOUND = "UPLOADED_OBJECT_NOT_FOUND"
    INVALID_OBJECT_KEY = "INVALID_OBJECT_KEY"

    @classmethod
    def _from_proto(cls, value: file_store_pb2.FileStoreError.ValueType) -> FileStoreErrorCode:  # noqa: PLR0912
        match value:
            case file_store_pb2.FILE_STORE_ERROR_DRIVE_NOT_FOUND:
                result = cls.DRIVE_NOT_FOUND
            case file_store_pb2.FILE_STORE_ERROR_FILE_NOT_FOUND:
                result = cls.FILE_NOT_FOUND
            case file_store_pb2.FILE_STORE_ERROR_FILE_REVISION_NOT_FOUND:
                result = cls.FILE_REVISION_NOT_FOUND
            case file_store_pb2.FILE_STORE_ERROR_PATH_ALREADY_EXISTS:
                result = cls.PATH_ALREADY_EXISTS
            case file_store_pb2.FILE_STORE_ERROR_INVALID_LOGICAL_PATH:
                result = cls.INVALID_LOGICAL_PATH
            case file_store_pb2.FILE_STORE_ERROR_PERMISSION_DENIED:
                result = cls.PERMISSION_DENIED
            case file_store_pb2.FILE_STORE_ERROR_REVISION_PRECONDITION_FAILED:
                result = cls.REVISION_PRECONDITION_FAILED
            case file_store_pb2.FILE_STORE_ERROR_READ_ONLY_DRIVE:
                result = cls.READ_ONLY_DRIVE
            case file_store_pb2.FILE_STORE_ERROR_FILE_HISTORY_NOT_AVAILABLE:
                result = cls.FILE_HISTORY_NOT_AVAILABLE
            case file_store_pb2.FILE_STORE_ERROR_REVISION_BYTES_RECLAIMED:
                result = cls.REVISION_BYTES_RECLAIMED
            case file_store_pb2.FILE_STORE_ERROR_DRIVE_ID_ALREADY_EXISTS:
                result = cls.DRIVE_ID_ALREADY_EXISTS
            case file_store_pb2.FILE_STORE_ERROR_UPLOADED_OBJECT_NOT_FOUND:
                result = cls.UPLOADED_OBJECT_NOT_FOUND
            case file_store_pb2.FILE_STORE_ERROR_INVALID_OBJECT_KEY:
                result = cls.INVALID_OBJECT_KEY
            case _:
                # Unset, or a code a newer server sent that this SDK doesn't model.
                result = cls.UNKNOWN
        return result


class NominalFileStoreError(NominalError):
    """A File Store operation failed.

    Raised both for failures the backend reports per-change (which carry no gRPC status of
    their own) and for capability checks this SDK makes before spending a request, so one
    `except NominalFileStoreError` covers both.
    """

    def __init__(self, code: FileStoreErrorCode, message: str) -> None:
        """Initialize error with the backend's error code and message."""
        self.code = code
        self.message = message
        super().__init__(f"{code.value}: {message}")
