from __future__ import annotations

from nominal import core
from nominal.core.file_store import FileStoreErrorCode, NominalFileStoreError
from nominal.exceptions import NominalError


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
        "FileDestination",
        "FileStoreErrorCode",
        "ManagedDriveFile",
        "NominalFileStoreError",
        "VirtualDrive",
        "VirtualDriveFile",
        "VirtualDriveState",
        "VirtualDriveStatus",
    }

    assert expected <= set(core.__all__)
    for name in expected:
        assert hasattr(core, name)


def test_file_store_errors_subclass_the_sdk_base_error() -> None:
    """`except NominalError` must keep catching File Store failures now that they live in their own package."""
    assert issubclass(NominalFileStoreError, NominalError)
    assert FileStoreErrorCode.UNKNOWN.value == "UNKNOWN"
