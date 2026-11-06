from __future__ import annotations

from nominal.core.fs.enums import (
    DriveFileState,
    DriveMutability,
    DriveSource,
    DriveState,
    VirtualDriveState,
)
from nominal.core.fs.errors import FileStoreErrorCode
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2


def test_unset_and_unrecognized_enum_values_become_unknown() -> None:
    """A value a newer server sent — or an unset field — must not crash the SDK."""
    assert DriveState._from_proto(0) is DriveState.UNKNOWN
    assert DriveState._from_proto(999) is DriveState.UNKNOWN
    assert DriveSource._from_proto(999) is DriveSource.UNKNOWN
    assert DriveMutability._from_proto(999) is DriveMutability.UNKNOWN
    assert DriveFileState._from_proto(999) is DriveFileState.UNKNOWN
    assert VirtualDriveState._from_proto(999) is VirtualDriveState.UNKNOWN
    assert FileStoreErrorCode._from_failure(files_pb2.FileChangeFailure(change=999)) is FileStoreErrorCode.UNKNOWN


def test_writability_is_tested_against_writable_not_read_only() -> None:
    """Every non-writable value must read as not-writable, including ones this SDK doesn't know."""
    assert DriveMutability._from_proto(file_store_pb2.DRIVE_MUTABILITY_WRITABLE) is DriveMutability.WRITABLE
    for value in (0, file_store_pb2.DRIVE_MUTABILITY_READ_ONLY, 999):
        assert DriveMutability._from_proto(value) is not DriveMutability.WRITABLE


def test_file_store_error_code_maps_both_failure_arms() -> None:
    """A change can fail with a common error or a change-specific one; both must keep their meaning."""
    common = files_pb2.FileChangeFailure(common=file_store_pb2.FILE_STORE_COMMON_ERROR_PERMISSION_DENIED)
    change = files_pb2.FileChangeFailure(change=files_pb2.FILE_CHANGE_ERROR_READ_ONLY_DRIVE)

    assert FileStoreErrorCode._from_failure(common) is FileStoreErrorCode.PERMISSION_DENIED
    assert FileStoreErrorCode._from_failure(change) is FileStoreErrorCode.READ_ONLY_DRIVE
    assert FileStoreErrorCode._from_failure(files_pb2.FileChangeFailure()) is FileStoreErrorCode.UNKNOWN
