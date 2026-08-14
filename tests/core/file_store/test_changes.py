from __future__ import annotations

import pytest

from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store.file import ManagedDriveFile, VirtualDriveFile
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from tests.core.file_store.test_drive import _clients, _drive_proto
from tests.core.file_store.test_file import (
    _managed_drive,
    _managed_file_proto,
    _virtual_drive,
    _virtual_file_proto,
)


def _success(path: str = "archive/run-001.csv") -> files_pb2.ApplyFileChangesResponse:
    return files_pb2.ApplyFileChangesResponse(
        results=[
            files_pb2.FileChangeResult(
                success=files_pb2.FileChangeSuccess(
                    file=_managed_file_proto(path=path),
                    file_revision=file_store_pb2.ManagedFileRevision(
                        file_revision_rid="ri.rev.2",
                        file_rid="ri.drive-file.1",
                        path=file_store_pb2.LogicalPath(path=path),
                        state=file_store_pb2.FILE_STATE_ACTIVE,
                    ),
                )
            )
        ]
    )


def _failure(code: file_store_pb2.FileStoreError.ValueType, message: str) -> files_pb2.ApplyFileChangesResponse:
    return files_pb2.ApplyFileChangesResponse(
        results=[files_pb2.FileChangeResult(failure=files_pb2.FileChangeFailure(code=code, message=message))]
    )


def _managed_file(clients: object) -> ManagedDriveFile:
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto())
    file = _managed_drive(clients).get_file("data/run-001.csv")
    assert isinstance(file, ManagedDriveFile)
    return file


def test_move_to_a_path_expects_that_path_to_be_empty() -> None:
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _success()

    returned = file.move_to("archive/run-001.csv")

    assert returned is file
    assert file.path == "archive/run-001.csv"
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.move.source_revision_rid == "ri.drive-file-revision.1"
    assert change.move.destination.path.path.path == "archive/run-001.csv"


def test_move_onto_a_file_replaces_that_file_by_its_current_revision() -> None:
    """A file destination means 'replace this file', expressed as its current head."""
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=_managed_file_proto(path="archive/run-001.csv", rid="ri.drive-file.2", revision_rid="ri.rev.9")
    )
    destination = _managed_drive(clients).get_file("archive/run-001.csv")
    clients.drive_files.ApplyFileChanges.return_value = _success()

    file.move_to(destination)

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.move.destination.file_revision_rid == "ri.rev.9"


def test_move_onto_a_file_with_no_current_revision_is_rejected_locally() -> None:
    clients = _clients()
    file = _managed_file(clients)
    headless = files_pb2.GetFileResponse(file=_managed_file_proto(path="archive/x.csv", rid="ri.drive-file.2"))
    headless.file.ClearField("current_revision")
    clients.drive_files.GetFile.return_value = headless
    destination = _managed_drive(clients).get_file("archive/x.csv")
    clients.drive_files.ApplyFileChanges.reset_mock()

    with pytest.raises(NominalFileStoreError):
        file.move_to(destination)

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_a_destination_in_another_drive_is_rejected_before_any_request() -> None:
    clients = _clients()
    file = _managed_file(clients)
    other_drive_file = _managed_drive(clients)
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=_managed_file_proto(path="x.csv", rid="ri.drive-file.9", revision_rid="ri.rev.9")
    )
    from nominal.core.file_store.drive import Drive

    elsewhere = Drive._from_proto(clients, _drive_proto(rid="ri.drive.OTHER"))
    destination = elsewhere.get_file("x.csv")
    clients.drive_files.ApplyFileChanges.reset_mock()
    assert other_drive_file.rid == "ri.drive.1"

    with pytest.raises(NominalFileStoreError, match="different drive"):
        file.move_to(destination)

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_remove_sends_the_current_head_revision() -> None:
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _success(path="data/run-001.csv")

    file.remove()

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.remove.revision_rid == "ri.drive-file-revision.1"


def test_a_reported_failure_becomes_a_typed_error() -> None:
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _failure(
        file_store_pb2.FILE_STORE_ERROR_PATH_ALREADY_EXISTS, "Path already exists"
    )

    with pytest.raises(NominalFileStoreError) as excinfo:
        file.move_to("archive/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.PATH_ALREADY_EXISTS
    assert excinfo.value.message == "Path already exists"


def test_a_stale_revision_failure_is_not_retried() -> None:
    """A precondition failure means the caller's view is stale; refreshing silently would hide that."""
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _failure(
        file_store_pb2.FILE_STORE_ERROR_REVISION_PRECONDITION_FAILED, "File revision precondition failed"
    )

    with pytest.raises(NominalFileStoreError):
        file.remove()

    assert clients.drive_files.ApplyFileChanges.call_count == 1


def test_restoring_a_revision_places_it_at_a_path() -> None:
    clients = _clients()
    file = _managed_file(clients)
    clients.drive_files.ListFileRevisions.side_effect = [
        files_pb2.ListFileRevisionsResponse(
            file_revisions=[
                file_store_pb2.ManagedFileRevision(
                    file_revision_rid="ri.rev.1",
                    file_rid="ri.drive-file.1",
                    path=file_store_pb2.LogicalPath(path="data/run-001.csv"),
                    state=file_store_pb2.FILE_STATE_ACTIVE,
                )
            ]
        )
    ]
    revision = file.revisions()[0]
    clients.drive_files.ApplyFileChanges.return_value = _success(path="data/restored.csv")

    restored = revision.restore("data/restored.csv")

    assert isinstance(restored, ManagedDriveFile)
    assert restored.path == "data/restored.csv"
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.restore.restore_revision_rid == "ri.rev.1"
    assert change.restore.destination.path.path.path == "data/restored.csv"


def test_virtual_files_refuse_mutations_without_a_request() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_virtual_file_proto())
    file = _virtual_drive(clients).get_file("logs/boot.txt")
    assert isinstance(file, VirtualDriveFile)

    for operation in (lambda: file.move_to("other.txt"), file.remove):
        with pytest.raises(NominalFileStoreError) as excinfo:
            operation()
        assert excinfo.value.code is FileStoreErrorCode.READ_ONLY_DRIVE

    clients.drive_files.ApplyFileChanges.assert_not_called()
