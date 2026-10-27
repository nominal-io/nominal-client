from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.fs.drive import Drive
from nominal.core.fs.errors import FileStoreErrorCode, NominalFileStoreError
from nominal.core.fs.file import ManagedDriveFile
from nominal.protos.file_store.v1 import files_pb2
from tests.core.fs._fixtures import (
    change_success,
    drive_proto,
    managed_drive,
    managed_file_proto,
    mock_clients,
    revision_proto,
)


def _failure(code: files_pb2.FileChangeError.ValueType, message: str) -> files_pb2.ApplyFileChangesResponse:
    return files_pb2.ApplyFileChangesResponse(
        results=[files_pb2.FileChangeResult(failure=files_pb2.FileChangeFailure(change=code, message=message))]
    )


def _managed_file(clients: MagicMock) -> ManagedDriveFile:
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    assert isinstance(file, ManagedDriveFile)
    return file


def test_move_to_a_path_expects_that_path_to_be_empty() -> None:
    """A path destination sends the file's head revision and refreshes the same instance at its new path."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = change_success()

    returned = file.move_to("archive/run-001.csv")

    assert returned is file
    assert file.path == "archive/run-001.csv"
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.move.source_revision_rid == "ri.drive-file-revision.1"
    assert change.move.destination.path.path.path == "archive/run-001.csv"


def test_move_onto_a_file_replaces_that_file_by_its_current_revision() -> None:
    """A file destination means 'replace this file', expressed as its current head."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(path="archive/run-001.csv", rid="ri.drive-file.2", revision_rid="ri.rev.9")
    )
    destination = managed_drive(clients).get_file("archive/run-001.csv")
    clients.drive_files.ApplyFileChanges.return_value = change_success()

    file.move_to(destination)

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.move.destination.file_revision_rid == "ri.rev.9"


def test_move_onto_a_revision_replaces_exactly_that_revision() -> None:
    """A revision destination means 'replace exactly this revision', not its file's current head."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ListFileRevisions.side_effect = [
        files_pb2.ListFileRevisionsResponse(file_revisions=[revision_proto("ri.rev.pinned")])
    ]
    destination = file.revisions()[0]
    clients.drive_files.ApplyFileChanges.return_value = change_success()

    file.move_to(destination)

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.move.destination.file_revision_rid == "ri.rev.pinned"


def test_move_onto_a_file_with_no_current_revision_is_rejected_locally() -> None:
    """A destination file with no head revision cannot be replaced, so the move fails before any request."""
    clients = mock_clients()
    file = _managed_file(clients)
    headless = files_pb2.GetFileResponse(file=managed_file_proto(path="archive/x.csv", rid="ri.drive-file.2"))
    headless.file.ClearField("current_revision")
    clients.drive_files.GetFile.return_value = headless
    destination = managed_drive(clients).get_file("archive/x.csv")
    clients.drive_files.ApplyFileChanges.reset_mock()

    with pytest.raises(NominalFileStoreError):
        file.move_to(destination)

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_a_file_with_no_current_revision_refuses_move_and_remove_locally() -> None:
    """The source-side guard must run before any request, same as the destination-side check."""
    clients = mock_clients()
    headless = files_pb2.GetFileResponse(file=managed_file_proto(path="data/headless.csv"))
    headless.file.ClearField("current_revision")
    clients.drive_files.GetFile.return_value = headless
    file = managed_drive(clients).get_file("data/headless.csv")
    assert isinstance(file, ManagedDriveFile)
    assert file.current_revision_rid is None

    for operation in (file.remove, lambda: file.move_to("somewhere.csv")):
        with pytest.raises(NominalFileStoreError):
            operation()

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_a_destination_in_another_drive_is_rejected_before_any_request() -> None:
    """Passing another drive's file as a destination is a caller mistake, reported before any request."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(path="x.csv", rid="ri.drive-file.9", revision_rid="ri.rev.9")
    )
    elsewhere = Drive._from_proto(clients, drive_proto(rid="ri.drive.OTHER")).get_file("x.csv")
    clients.drive_files.ApplyFileChanges.reset_mock()

    with pytest.raises(ValueError, match="different drive"):
        file.move_to(elsewhere)

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_remove_sends_the_current_head_revision() -> None:
    """Removal names the file by its head revision, so the backend rejects it if the file has moved on."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/run-001.csv")

    file.remove()

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.remove.revision_rid == "ri.drive-file-revision.1"


def test_a_reported_failure_becomes_a_typed_error() -> None:
    """An in-band failure raises with the backend's code and message rather than returning silently."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _failure(
        files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS, "Path already exists"
    )

    with pytest.raises(NominalFileStoreError) as excinfo:
        file.move_to("archive/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.PATH_ALREADY_EXISTS
    assert excinfo.value.message == "Path already exists"


def test_a_stale_revision_failure_is_not_retried() -> None:
    """A precondition failure means the caller's view is stale; refreshing silently would hide that."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ApplyFileChanges.return_value = _failure(
        files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED, "File revision precondition failed"
    )

    with pytest.raises(NominalFileStoreError):
        file.remove()

    assert clients.drive_files.ApplyFileChanges.call_count == 1


def test_restoring_a_revision_places_it_at_a_path() -> None:
    """Restoring sends the revision and destination, and returns the file as it now stands."""
    clients = mock_clients()
    file = _managed_file(clients)
    clients.drive_files.ListFileRevisions.side_effect = [
        files_pb2.ListFileRevisionsResponse(file_revisions=[revision_proto("ri.rev.1")])
    ]
    revision = file.revisions()[0]
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/restored.csv")

    restored = revision.restore("data/restored.csv")

    assert isinstance(restored, ManagedDriveFile)
    assert restored.path == "data/restored.csv"
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.restore.restore_revision_rid == "ri.rev.1"
    assert change.restore.destination.path.path.path == "data/restored.csv"
