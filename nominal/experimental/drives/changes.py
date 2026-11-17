"""Changes and ordered results for a managed drive's batch operation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import TYPE_CHECKING, Callable, Self, TypeAlias

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.experimental.drives.errors import FileStoreErrorCode
from nominal.experimental.drives.file import (
    DriveFileRevision,
    FileDestination,
    ManagedDriveFile,
    _move_change,
    _remove_change,
    _restore_change,
)
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2

if TYPE_CHECKING:
    from nominal.experimental.drives.drive import Drive


class FailurePolicy(Enum):
    """Whether a failed change leaves successful changes applied."""

    CONTINUE = "CONTINUE"
    """Apply changes independently, continuing after failures. At most 1,000 changes."""
    ABORT = "ABORT"
    """Apply all changes or none. At most 100 changes; uploaded bytes are outside the transaction."""

    def _to_proto(self) -> files_pb2.FailurePolicy.ValueType:
        return files_pb2.FAILURE_POLICY_ABORT if self is FailurePolicy.ABORT else files_pb2.FAILURE_POLICY_CONTINUE


def _require_same_drive(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"change belongs to a different drive ({actual})")


@dataclass(frozen=True)
class PreparedPut:
    """Uploaded bytes ready to commit with `DriveBatch.put`.

    Obtain this from `Drive.prepare_put_file` or `Drive.prepare_put_file_obj`.
    Preparation pins the destination precondition but does not change the drive.
    """

    _drive_rid: str = field(repr=False)
    _put: files_pb2.PutFile = field(repr=False, hash=False)


def _move_directory_change(source: str, destination: str) -> files_pb2.FileChange:
    """Encode a directory move for both single and batch operations."""
    move = files_pb2.MoveDirectory(to=file_store_pb2.LogicalPath(path=destination))
    # Protobuf's field name is a Python keyword and is omitted from its generated stub.
    getattr(move, "from").CopyFrom(file_store_pb2.LogicalPath(path=source))
    return files_pb2.FileChange(move_directory=move)


class DriveBatch:
    """Build an ordered batch for one drive, then submit it once.

    Obtain a batch with `Drive.batch`. Adding operations does not make requests.
    File revision preconditions are read from the supplied snapshots at submission;
    prepared puts retain their preparation-time preconditions. Input snapshots are
    not refreshed by submission. Use the returned files or refresh them explicitly.
    """

    def __init__(self, drive: Drive) -> None:
        """Bind a batch to its drive.

        Example:
            ```python
            batch = drive.batch()
            batch.move(file, "archive/readings.csv")
            results = batch.submit()
            ```
        """
        self._drive = drive
        self._changes: list[Callable[[], files_pb2.FileChange]] = []
        self._submitted = False

    def _append(self, change: Callable[[], files_pb2.FileChange]) -> Self:
        """Queue an encoder so file snapshot preconditions are read at submission."""
        if self._submitted:
            raise RuntimeError("this DriveBatch was already submitted; create a new batch")
        self._changes.append(change)
        return self

    def move(self, source: ManagedDriveFile, destination: FileDestination) -> Self:
        """Queue a file move and return this batch.

        Args:
            source: File in this drive to move.
            destination: Unoccupied path, or a file/revision to replace in this drive.

        Raises:
            ValueError: The source belongs to another drive. Destination ownership
                is checked at submission, before any request.
            RuntimeError: The batch was already submitted.
        """
        _require_same_drive(source._drive_rid, self._drive.rid)
        return self._append(partial(_move_change, source, destination))

    def remove(self, source: ManagedDriveFile) -> Self:
        """Queue removal of a file, retaining its history, and return this batch.

        Args:
            source: File in this drive to remove.

        Raises:
            ValueError: The source belongs to another drive.
            RuntimeError: The batch was already submitted.
        """
        _require_same_drive(source._drive_rid, self._drive.rid)
        return self._append(partial(_remove_change, source))

    def restore(self, revision: DriveFileRevision, destination: FileDestination) -> Self:
        """Queue restoration of historical content and return this batch.

        Args:
            revision: Historical revision in this drive.
            destination: Unoccupied path, or a file/revision to replace in this drive.

        Raises:
            ValueError: The revision belongs to another drive. Destination ownership
                is checked at submission, before any request.
            RuntimeError: The batch was already submitted.
        """
        _require_same_drive(revision._drive_rid, self._drive.rid)
        return self._append(partial(_restore_change, revision, destination))

    def move_directory(self, source: str, destination: str) -> Self:
        """Queue a directory move and return this batch.

        Args:
            source: Drive-relative directory path.
            destination: Unoccupied directory path, neither inside nor containing the source.

        Raises:
            RuntimeError: The batch was already submitted.

        Note:
            A successful directory move returns no per-file details.
        """
        return self._append(partial(_move_directory_change, source, destination))

    def put(self, prepared: PreparedPut) -> Self:
        """Queue an already uploaded file and return this batch.

        Args:
            prepared: Upload from this drive's ``prepare_put_file`` or
                ``prepare_put_file_obj``. Its destination precondition stays pinned.

        Raises:
            ValueError: The upload belongs to another drive.
            RuntimeError: The batch was already submitted.
        """
        _require_same_drive(prepared._drive_rid, self._drive.rid)
        return self._append(partial(files_pb2.FileChange, put=prepared._put))

    def submit(self, *, failure_policy: FailurePolicy = FailurePolicy.CONTINUE) -> list[FileChangeResult]:
        """Submit the queued changes in one request, preserving their order.

        Args:
            failure_policy: ``CONTINUE`` applies changes independently (up to 1,000).
                ``ABORT`` applies all changes or none (up to 100). Batches are not split.

        Returns:
            One typed success or failure per operation, in order. Per-change failures
            are returned rather than raised. An empty batch returns an empty list.

        Raises:
            ValueError: The batch exceeds its limit or a destination belongs to another drive.
            NominalFileStoreError: The drive is read-only or a file has no current revision.
            NominalError: The request failed; a transport error can leave its outcome unknown.
            RuntimeError: The batch was already submitted.

        Note:
            Submission consumes the batch even if it fails. Do not blindly recreate and
            retry it: see :ref:`drive-change-retries` to reconcile uncertain outcomes.
            Earlier changes are visible to later changes, but all revision preconditions
            are encoded before the request. Two changes to the same file can therefore
            conflict. Under ``ABORT``, the failing change has its own error and the other
            results have ``BATCH_FAILURE``. Uploaded bytes are outside the transaction.
        """
        if self._submitted:
            raise RuntimeError("this DriveBatch was already submitted; create a new batch")
        self._submitted = True
        self._drive._require_writable()
        limit = 100 if failure_policy is FailurePolicy.ABORT else 1000
        if len(self._changes) > limit:
            raise ValueError(f"at most {limit} changes are allowed under {failure_policy.value}")
        if not self._changes:
            return []
        request = files_pb2.ApplyFileChangesRequest(
            drive_rid=self._drive.rid,
            changes=[encode() for encode in self._changes],
            failure_policy=failure_policy._to_proto(),
        )
        with translate_grpc_errors():
            response = self._drive._clients.drive_files.ApplyFileChanges(request)
        return [_result_from_proto(self._drive._clients, self._drive.rid, result) for result in response.results]


@dataclass(frozen=True)
class FileChangeSuccess:
    """File and revision produced by a successful put, move, remove, or restore."""

    file: ManagedDriveFile
    revision: DriveFileRevision


@dataclass(frozen=True)
class DirectoryMoveSuccess:
    """A directory move completed; existing file snapshots keep their old paths until refreshed."""


@dataclass(frozen=True)
class FileChangeFailure:
    """A rejected change, returned in its input position rather than raised.

    Under `FailurePolicy.ABORT`, the failing change has its own error and the other
    changes have `FileStoreErrorCode.BATCH_FAILURE`. None of the changes is applied.
    """

    code: FileStoreErrorCode
    message: str


FileChangeResult: TypeAlias = FileChangeSuccess | DirectoryMoveSuccess | FileChangeFailure
"""One outcome for each submitted change, in input order."""


def _result_from_proto(
    clients: ManagedDriveFile._Clients, drive_rid: str, result: files_pb2.FileChangeResult
) -> FileChangeResult:
    match result.WhichOneof("result"):
        case "success":
            return FileChangeSuccess(
                file=ManagedDriveFile._from_proto(clients, drive_rid, result.success.file),
                revision=DriveFileRevision._from_proto(clients, drive_rid, result.success.file_revision),
            )
        case "directory_moved":
            return DirectoryMoveSuccess()
        case "failure":
            return FileChangeFailure(FileStoreErrorCode._from_failure(result.failure), result.failure.message)
        case _:
            raise ValueError("File Store returned a change result with no recognized outcome")
