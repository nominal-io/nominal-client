from __future__ import annotations

from nominal import core
from nominal.core.exceptions import FileStoreErrorCode, NominalError, NominalFileStoreError


def test_file_store_types_are_importable_from_nominal_core() -> None:
    """The stable import path users are told to use must actually export everything."""
    expected = {
        "Drive",
        "DriveDirectory",
        "DriveEntry",
        "DriveFile",
        "DriveFileRevision",
        "DriveFileState",
        "DriveMutability",
        "DriveSource",
        "DriveState",
        "FileChange",
        "FileChangeFailure",
        "FileChangeResult",
        "FileChangeSuccess",
        "FileDestination",
        "ManagedDriveFile",
        "MoveFile",
        "RemoveFile",
        "RestoreFile",
        "VirtualDrive",
        "VirtualDriveFile",
        "VirtualDriveState",
        "VirtualDriveStatus",
    }

    assert expected <= set(core.__all__)
    for name in expected:
        assert hasattr(core, name)


def test_file_store_errors_are_importable_from_exceptions() -> None:
    assert issubclass(NominalFileStoreError, NominalError)
    assert FileStoreErrorCode.UNKNOWN.value == "UNKNOWN"
