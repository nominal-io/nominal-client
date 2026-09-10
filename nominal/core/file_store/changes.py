"""Batch file changes for a managed drive.

`Drive.apply_changes` sends many changes in one request. They are applied in order, and a
change that fails is reported in its own result without stopping the ones after it — so the
call returns a result per change rather than raising on the first failure. For a single
change, the methods on `ManagedDriveFile` and `DriveFileRevision` are simpler and do raise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypeAlias

from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store._clients import _Clients
from nominal.core.file_store.file import (
    DriveFileRevision,
    FileDestination,
    ManagedDriveFile,
    _apply,
    _destination_to_proto,
    _file_from_proto,
)
from nominal.protos.file_store.v1 import files_pb2

MAX_CHANGES_PER_REQUEST = 1000
"""Most changes the backend accepts in a single `Drive.apply_changes` call."""


@dataclass(frozen=True)
class MoveFile:
    """Move a file to a destination."""

    file: ManagedDriveFile
    destination: FileDestination

    def _to_proto(self, drive_rid: str) -> files_pb2.FileChange:
        return files_pb2.FileChange(
            move=files_pb2.MoveFile(
                source_revision_rid=self.file._require_current_revision(),
                destination=_destination_to_proto(self.destination, drive_rid),
            )
        )


@dataclass(frozen=True)
class RemoveFile:
    """Soft-delete a file, keeping its revisions restorable."""

    file: ManagedDriveFile

    def _to_proto(self, drive_rid: str) -> files_pb2.FileChange:
        return files_pb2.FileChange(remove=files_pb2.RemoveFile(revision_rid=self.file._require_current_revision()))


@dataclass(frozen=True)
class RestoreFile:
    """Reinstate a past revision at a destination."""

    revision: DriveFileRevision
    destination: FileDestination

    def _to_proto(self, drive_rid: str) -> files_pb2.FileChange:
        return files_pb2.FileChange(
            restore=files_pb2.RestoreFile(
                restore_revision_rid=self.revision.rid,
                destination=_destination_to_proto(self.destination, drive_rid),
            )
        )


FileChange: TypeAlias = MoveFile | RemoveFile | RestoreFile


@dataclass(frozen=True)
class FileChangeSuccess:
    """A change that was applied, with the file and the revision it produced."""

    file: ManagedDriveFile
    revision: DriveFileRevision


@dataclass(frozen=True)
class FileChangeFailure:
    """A change the backend rejected, with the reason it gave."""

    code: FileStoreErrorCode
    message: str


FileChangeResult: TypeAlias = FileChangeSuccess | FileChangeFailure


def _apply_changes(clients: _Clients, drive_rid: str, changes: Sequence[FileChange]) -> Sequence[FileChangeResult]:
    if len(changes) > MAX_CHANGES_PER_REQUEST:
        raise ValueError(f"at most {MAX_CHANGES_PER_REQUEST} changes may be applied in one call, got {len(changes)}")
    results = _apply(clients, drive_rid, [change._to_proto(drive_rid) for change in changes])
    return [_result_from_proto(clients, drive_rid, result) for result in results]


def _result_from_proto(clients: _Clients, drive_rid: str, result: files_pb2.FileChangeResult) -> FileChangeResult:
    if result.WhichOneof("result") == "failure":
        return FileChangeFailure(
            code=FileStoreErrorCode._from_proto(result.failure.code),
            message=result.failure.message,
        )
    file = _file_from_proto(clients, drive_rid, result.success.file)
    if not isinstance(file, ManagedDriveFile):
        # A change only ever succeeds against a managed drive, so this cannot happen; guarding
        # keeps the narrowing honest rather than asserting in library code.
        raise NominalFileStoreError(
            FileStoreErrorCode.UNKNOWN, "a change succeeded but returned a file that is not a managed file"
        )
    return FileChangeSuccess(
        file=file,
        revision=DriveFileRevision._from_proto(clients, drive_rid, result.success.file_revision),
    )
