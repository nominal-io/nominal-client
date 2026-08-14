from __future__ import annotations

from enum import Enum

from nominal.protos.file_store.v1 import file_store_pb2


class DriveState(Enum):
    """Lifecycle state of a drive."""

    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"
    UNKNOWN = "UNKNOWN"
    """Unset, or a state a newer server sent that this SDK doesn't know."""

    @classmethod
    def _from_proto(cls, value: file_store_pb2.DriveState.ValueType) -> DriveState:
        match value:
            case file_store_pb2.DRIVE_STATE_ACTIVE:
                return cls.ACTIVE
            case file_store_pb2.DRIVE_STATE_ARCHIVED:
                return cls.ARCHIVED
            case _:
                return cls.UNKNOWN


class DriveSource(Enum):
    """Where a drive's files come from: Nominal's own storage, or an external provider."""

    NOMINAL = "NOMINAL"
    S3 = "S3"
    GOOGLE_DRIVE = "GOOGLE_DRIVE"
    GCS = "GCS"
    UNKNOWN = "UNKNOWN"
    """Unset, or a provider a newer server sent that this SDK doesn't know."""

    @classmethod
    def _from_proto(cls, value: file_store_pb2.DriveSource.ValueType) -> DriveSource:
        match value:
            case file_store_pb2.DRIVE_SOURCE_NOMINAL:
                return cls.NOMINAL
            case file_store_pb2.DRIVE_SOURCE_S3:
                return cls.S3
            case file_store_pb2.DRIVE_SOURCE_GOOGLE_DRIVE:
                return cls.GOOGLE_DRIVE
            case file_store_pb2.DRIVE_SOURCE_GCS:
                return cls.GCS
            case _:
                return cls.UNKNOWN


class DriveMutability(Enum):
    """Whether a drive's contents can be modified through Nominal.

    Test writability as `is DriveMutability.WRITABLE`, never `is not READ_ONLY` — that
    keeps every future non-writable value covered.
    """

    WRITABLE = "WRITABLE"
    READ_ONLY = "READ_ONLY"
    UNKNOWN = "UNKNOWN"
    """Unset, or a value a newer server sent that this SDK doesn't know. Not writable."""

    @classmethod
    def _from_proto(cls, value: file_store_pb2.DriveMutability.ValueType) -> DriveMutability:
        match value:
            case file_store_pb2.DRIVE_MUTABILITY_WRITABLE:
                return cls.WRITABLE
            case file_store_pb2.DRIVE_MUTABILITY_READ_ONLY:
                return cls.READ_ONLY
            case _:
                return cls.UNKNOWN


class DriveFileState(Enum):
    """Whether a file is present or soft-deleted.

    On a revision this records the state that revision set: `ACTIVE` for a content
    revision, `REMOVED` for a removal marker.
    """

    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"
    UNKNOWN = "UNKNOWN"
    """Unset, or a state a newer server sent that this SDK doesn't know."""

    @classmethod
    def _from_proto(cls, value: file_store_pb2.FileState.ValueType) -> DriveFileState:
        match value:
            case file_store_pb2.FILE_STATE_ACTIVE:
                return cls.ACTIVE
            case file_store_pb2.FILE_STATE_REMOVED:
                return cls.REMOVED
            case _:
                return cls.UNKNOWN


class VirtualDriveState(Enum):
    """Connectivity of a virtual drive's backing provider."""

    ACTIVE = "ACTIVE"
    AUTH_ERROR = "AUTH_ERROR"
    UNREACHABLE = "UNREACHABLE"
    INVALID_CONFIGURATION = "INVALID_CONFIGURATION"
    UNKNOWN = "UNKNOWN"
    """Unset, or a state a newer server sent that this SDK doesn't know."""

    @classmethod
    def _from_proto(cls, value: file_store_pb2.VirtualDriveState.ValueType) -> VirtualDriveState:
        match value:
            case file_store_pb2.VIRTUAL_DRIVE_STATE_ACTIVE:
                result = cls.ACTIVE
            case file_store_pb2.VIRTUAL_DRIVE_STATE_AUTH_ERROR:
                result = cls.AUTH_ERROR
            case file_store_pb2.VIRTUAL_DRIVE_STATE_UNREACHABLE:
                result = cls.UNREACHABLE
            case file_store_pb2.VIRTUAL_DRIVE_STATE_INVALID_CONFIGURATION:
                result = cls.INVALID_CONFIGURATION
            case _:
                result = cls.UNKNOWN
        return result
