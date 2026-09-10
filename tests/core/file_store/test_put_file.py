from __future__ import annotations

import pathlib

import pytest
from nominal_api import ingest_api

from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store.drive import Drive
from nominal.core.file_store.file import ManagedDriveFile
from nominal.protos.file_store.v1 import file_store_pb2
from tests.core.file_store.test_changes import _success
from tests.core.file_store.test_drive import _clients, _drive_proto
from tests.core.file_store.test_file import _managed_drive, _virtual_drive


def _local_file(tmp_path: pathlib.Path, name: str = "run-001.csv", data: bytes = b"a,b\n1,2\n") -> pathlib.Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_put_file_uploads_then_commits_the_returned_key(tmp_path: pathlib.Path, monkeypatch) -> None:
    clients = _clients()
    drive = _managed_drive(clients)
    local = _local_file(tmp_path)
    clients.drive_files.ApplyFileChanges.return_value = _success(path="data/run-001.csv")

    captured: dict[str, object] = {}

    def fake_upload(*args: object, **kwargs: object):
        captured["args"] = args
        captured["kwargs"] = kwargs

        class _Completed:
            key = "0f9a5c2e-0000-4000-8000-000000000000"
            bucket = "FILE_STORE"
            location = "s3://bucket/key"

        return _Completed()

    monkeypatch.setattr("nominal.core.file_store.drive._put_multipart_upload_to", fake_upload)

    file = drive.put_file(local, "data/run-001.csv", chunk_size=1234, max_workers=3)

    assert isinstance(file, ManagedDriveFile)
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.put.object.object_key == "0f9a5c2e-0000-4000-8000-000000000000"
    assert change.put.size_bytes == local.stat().st_size
    assert change.put.destination.path.path.path == "data/run-001.csv"
    assert captured["kwargs"]["destination"] is ingest_api.UploadDestination.FILE_STORE
    assert captured["kwargs"]["chunk_size"] == 1234
    assert captured["kwargs"]["max_workers"] == 3


def test_put_file_names_the_upload_after_the_destination(tmp_path: pathlib.Path, monkeypatch) -> None:
    """The stored object keeps the suffix the caller chose in the drive, not the local one."""
    clients = _clients()
    drive = _managed_drive(clients)
    local = _local_file(tmp_path, name="scratch.tmp")
    clients.drive_files.ApplyFileChanges.return_value = _success(path="data/run-001.csv")
    seen: dict[str, object] = {}

    def fake_upload(auth_header, workspace_rid, f, filename, mimetype, upload_client, **kwargs):
        seen["filename"] = filename

        class _Completed:
            key = "0f9a5c2e-0000-4000-8000-000000000000"
            bucket = "FILE_STORE"
            location = "s3://bucket/key"

        return _Completed()

    monkeypatch.setattr("nominal.core.file_store.drive._put_multipart_upload_to", fake_upload)

    drive.put_file(local, "data/run-001.csv")

    assert seen["filename"] == "run-001.csv"


def test_put_file_rejects_a_virtual_drive_before_uploading(tmp_path: pathlib.Path) -> None:
    """The guard must fire before the transfer — this is the whole point of checking locally."""
    clients = _clients()
    drive = _virtual_drive(clients)

    with pytest.raises(NominalFileStoreError) as excinfo:
        drive.put_file(_local_file(tmp_path), "data/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.READ_ONLY_DRIVE
    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_put_file_rejects_a_read_only_managed_drive_before_uploading(tmp_path: pathlib.Path) -> None:
    """A managed (NOMINAL-sourced) drive can also be read-only, in which case it is a base `Drive`,
    not a `VirtualDrive` — the `content_mutability` check must fire on its own, not rely on the
    `VirtualDrive` override.
    """
    clients = _clients()
    drive = Drive._from_proto(clients, _drive_proto(mutability=file_store_pb2.DRIVE_MUTABILITY_READ_ONLY))
    assert type(drive) is Drive

    with pytest.raises(NominalFileStoreError) as excinfo:
        drive.put_file(_local_file(tmp_path), "data/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.READ_ONLY_DRIVE
    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_put_file_rejects_a_missing_path_a_directory_and_an_empty_file(tmp_path: pathlib.Path) -> None:
    clients = _clients()
    drive = _managed_drive(clients)
    empty = _local_file(tmp_path, name="empty.csv", data=b"")

    with pytest.raises(FileNotFoundError):
        drive.put_file(tmp_path / "nope.csv", "data/x.csv")
    with pytest.raises(IsADirectoryError):
        drive.put_file(tmp_path, "data/x.csv")
    with pytest.raises(ValueError, match="empty"):
        drive.put_file(empty, "data/x.csv")

    clients.upload.initiate_multipart_upload.assert_not_called()


def test_put_file_rejects_a_destination_with_no_filename(tmp_path: pathlib.Path) -> None:
    """A trailing-slash destination must fail locally, not after the whole file has uploaded."""
    clients = _clients()
    drive = _managed_drive(clients)
    local = _local_file(tmp_path)

    with pytest.raises(ValueError, match="data/"):
        drive.put_file(local, "data/")

    clients.upload.initiate_multipart_upload.assert_not_called()
