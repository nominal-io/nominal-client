from __future__ import annotations

import io
from unittest.mock import patch

import pytest

from nominal.core._utils.multipart import _CompletedUpload
from nominal.experimental import drives
from nominal.experimental.drives import drive as drive_module
from nominal.protos.file_store.v1 import files_pb2
from tests.experimental.drives._fixtures import (
    change_success,
    drive_proto,
    managed_drive,
    managed_file_proto,
    mock_clients,
    revision_proto,
    virtual_drive,
)


def test_mixed_batch_preserves_order_results_and_snapshots() -> None:
    """All change kinds share one ordered request without refreshing input snapshots."""
    clients = mock_clients()
    drive = managed_drive(clients)
    file = drives.ManagedDriveFile._from_proto(clients, drive.rid, managed_file_proto())
    revision = drives.DriveFileRevision._from_proto(clients, drive.rid, revision_proto("ri.rev.old"))
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[
            change_success().results[0],
            files_pb2.FileChangeResult(
                failure=files_pb2.FileChangeFailure(
                    change=files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED, message="stale"
                )
            ),
            change_success().results[0],
            files_pb2.FileChangeResult(directory_moved=files_pb2.MoveDirectorySuccess()),
            change_success().results[0],
        ]
    )
    with patch.object(drive_module, "_put_multipart_upload_to", return_value=_CompletedUpload("uploaded", "s3://x")):
        put = drive.prepare_put_file_obj(io.BytesIO(b"content"), "new.csv")
    clients.drive_files.ApplyFileChanges.assert_not_called()

    results = (
        drive.batch()
        .move(file, "archive/run.csv")
        .remove(file)
        .restore(revision, file)
        .move_directory("raw", "processed")
        .put(put)
        .submit()
    )

    clients.drive_files.ApplyFileChanges.assert_called_once()
    request = clients.drive_files.ApplyFileChanges.call_args.args[0]
    assert [c.WhichOneof("change") for c in request.changes] == ["move", "remove", "restore", "move_directory", "put"]
    assert request.changes[0].move.source_revision_rid == file.current_revision_rid
    assert request.changes[2].restore.destination.file_revision_rid == file.current_revision_rid
    assert getattr(request.changes[3].move_directory, "from").path == "raw"
    assert request.changes[4].put.object.object_key == "uploaded"
    assert request.failure_policy == files_pb2.FAILURE_POLICY_CONTINUE
    assert isinstance(results[0], drives.FileChangeSuccess)
    assert results[0].file.path == "archive/run-001.csv"
    assert results[0].revision.rid == "ri.rev.2"
    assert isinstance(results[1], drives.FileChangeFailure)
    assert results[1].code is drives.FileStoreErrorCode.REVISION_PRECONDITION_FAILED
    assert results[1].message == "stale"
    assert isinstance(results[3], drives.DirectoryMoveSuccess)
    assert file.path == "data/run-001.csv"


def test_atomic_failure_keeps_every_error_in_request_order() -> None:
    """ABORT preserves the cause and BATCH_FAILURE markers without claiming any success."""
    clients = mock_clients()
    drive = managed_drive(clients)
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[
            files_pb2.FileChangeResult(failure=files_pb2.FileChangeFailure(change=code))
            for code in (files_pb2.FILE_CHANGE_ERROR_BATCH_FAILURE, files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS)
        ]
    )
    results = (
        drive.batch()
        .move_directory("a", "b")
        .move_directory("c", "d")
        .submit(failure_policy=drives.FailurePolicy.ABORT)
    )
    assert all(isinstance(result, drives.FileChangeFailure) for result in results)
    assert [result.code for result in results if isinstance(result, drives.FileChangeFailure)] == [
        drives.FileStoreErrorCode.BATCH_FAILURE,
        drives.FileStoreErrorCode.PATH_ALREADY_EXISTS,
    ]
    assert clients.drive_files.ApplyFileChanges.call_args.args[0].failure_policy == files_pb2.FAILURE_POLICY_ABORT


@pytest.mark.parametrize("atomic,limit", [(False, 1000), (True, 100)])
def test_oversized_batch_is_not_split_or_submitted(atomic: bool, limit: int) -> None:
    """Limits are checked before submission so atomic requests are never silently split."""
    clients = mock_clients()
    policy = drives.FailurePolicy.ABORT if atomic else drives.FailurePolicy.CONTINUE
    batch = managed_drive(clients).batch()
    for _ in range(limit + 1):
        batch.move_directory("a", "b")
    with pytest.raises(ValueError, match=str(limit)):
        batch.submit(failure_policy=policy)
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_cross_drive_sources_are_rejected_before_any_submission() -> None:
    """Every source-bearing change is checked before any part of the request is sent."""
    clients = mock_clients()
    drive = managed_drive(clients)
    foreign = drives.ManagedDriveFile._from_proto(clients, "ri.drive.other", managed_file_proto())
    revision = drives.DriveFileRevision._from_proto(clients, "ri.drive.other", revision_proto("ri.rev.old"))
    batch = drive.batch().move_directory("a", "b")
    with pytest.raises(ValueError, match="different drive"):
        batch.move(foreign, "new.csv")
    with pytest.raises(ValueError, match="different drive"):
        batch.remove(foreign)
    with pytest.raises(ValueError, match="different drive"):
        batch.restore(revision, "new.csv")
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_cross_drive_destination_is_rejected_before_submission() -> None:
    """Batch moves preserve the single-operation destination ownership check."""
    clients = mock_clients()
    drive = managed_drive(clients)
    foreign = drives.ManagedDriveFile._from_proto(clients, "ri.drive.other", managed_file_proto())
    local = drives.ManagedDriveFile._from_proto(clients, drive.rid, managed_file_proto())
    with pytest.raises(ValueError, match="different drive"):
        drive.batch().move_directory("a", "b").move(local, foreign).submit()
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_prepared_put_stays_bound_to_its_drive_and_revision() -> None:
    """Refreshing a destination after preparation does not weaken its original precondition."""
    clients = mock_clients()
    drive = managed_drive(clients)
    destination = drives.ManagedDriveFile._from_proto(clients, drive.rid, managed_file_proto())
    with patch.object(drive_module, "_put_multipart_upload_to", return_value=_CompletedUpload("uploaded", "s3://x")):
        put = drive.prepare_put_file_obj(io.BytesIO(b"content"), destination)
    destination._refresh_from_api(managed_file_proto(revision_rid="ri.rev.new"))
    other = drives.Drive._from_proto(clients, drive_proto(rid="ri.drive.other"))
    with pytest.raises(ValueError, match="different drive"):
        other.batch().put(put)
    clients.drive_files.ApplyFileChanges.assert_not_called()
    clients.drive_files.ApplyFileChanges.return_value = change_success()
    drive.batch().put(put).submit()
    request = clients.drive_files.ApplyFileChanges.call_args.args[0]
    assert request.changes[0].put.destination.file_revision_rid == "ri.drive-file-revision.1"


def test_empty_batch_and_read_only_drive_make_no_request() -> None:
    """Empty writes are a no-op; populated writes on virtual drives are rejected locally."""
    clients = mock_clients()
    assert managed_drive(clients).batch().submit() == []
    with pytest.raises(drives.NominalFileStoreError):
        virtual_drive(clients).batch().move_directory("a", "b").submit()
    clients.drive_files.ApplyFileChanges.assert_not_called()


@pytest.mark.parametrize("fails", [False, True])
def test_batch_cannot_be_replayed_or_extended_after_submission(fails: bool) -> None:
    """Both successful and uncertain failed submissions consume the builder."""
    clients = mock_clients()
    batch = managed_drive(clients).batch().move_directory("a", "b")
    if fails:
        clients.drive_files.ApplyFileChanges.side_effect = RuntimeError("response lost")
        with pytest.raises(RuntimeError, match="response lost"):
            batch.submit()
    else:
        clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
            results=[files_pb2.FileChangeResult(directory_moved=files_pb2.MoveDirectorySuccess())]
        )
        batch.submit()
    with pytest.raises(RuntimeError, match="already submitted"):
        batch.submit()
    with pytest.raises(RuntimeError, match="already submitted"):
        batch.move_directory("c", "d")
    clients.drive_files.ApplyFileChanges.assert_called_once()


def test_file_preconditions_are_read_at_submission() -> None:
    """Refreshing a queued source preserves the existing submission-time snapshot contract."""
    clients = mock_clients()
    drive = managed_drive(clients)
    source = drives.ManagedDriveFile._from_proto(clients, drive.rid, managed_file_proto())
    batch = drive.batch().move(source, "new.csv")
    source._refresh_from_api(managed_file_proto(revision_rid="ri.rev.new"))
    clients.drive_files.ApplyFileChanges.return_value = change_success()
    batch.submit()
    request = clients.drive_files.ApplyFileChanges.call_args.args[0]
    assert request.changes[0].move.source_revision_rid == "ri.rev.new"
