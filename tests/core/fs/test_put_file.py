from __future__ import annotations

import io
import pathlib
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import ingest_api

from nominal.core.fs import drive as drive_module
from nominal.core.fs.drive import Drive
from nominal.core.fs.errors import FileStoreErrorCode, NominalFileStoreError
from nominal.core.fs.file import ManagedDriveFile
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from tests.core.fs._fixtures import (
    change_success,
    drive_proto,
    managed_drive,
    managed_file_proto,
    mock_clients,
    virtual_drive,
)

UPLOADED_KEY = "0f9a5c2e-0000-4000-8000-000000000000"


def _local_file(tmp_path: pathlib.Path, name: str = "run-001.csv", data: bytes = b"a,b\n1,2\n") -> pathlib.Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def _patch_upload():
    return patch.object(
        drive_module,
        "_put_multipart_upload_to",
        return_value=MagicMock(key=UPLOADED_KEY, bucket="FILE_STORE", location="s3://bucket/key"),
    )


def test_put_file_uploads_then_commits_the_returned_key(tmp_path: pathlib.Path) -> None:
    """The key the upload returns is what the put change commits, with the local file's size."""
    clients = mock_clients()
    drive = managed_drive(clients)
    local = _local_file(tmp_path)
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/run-001.csv")

    with _patch_upload() as upload:
        file = drive.put_file(local, "data/run-001.csv", chunk_size=1234, max_workers=3)

    assert isinstance(file, ManagedDriveFile)
    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.put.object.object_key == UPLOADED_KEY
    assert change.put.size_bytes == local.stat().st_size
    assert change.put.destination.path.path.path == "data/run-001.csv"
    assert upload.call_args.kwargs["destination"] is ingest_api.UploadDestination.FILE_STORE
    assert upload.call_args.kwargs["chunk_size"] == 1234
    assert upload.call_args.kwargs["max_workers"] == 3


def test_put_file_names_and_types_the_upload_after_the_destination(tmp_path: pathlib.Path) -> None:
    """The stored object keeps the name and type the caller chose in the drive, not the local ones."""
    clients = mock_clients()
    drive = managed_drive(clients)
    local = _local_file(tmp_path, name="scratch.tmp")
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/run-001.csv")

    with _patch_upload() as upload:
        drive.put_file(local, "data/run-001.csv")

    _auth_header, _workspace_rid, _f, filename, mimetype, _upload_client = upload.call_args.args
    assert filename == "run-001.csv"
    assert mimetype == "text/csv"


def test_put_file_obj_uploads_from_the_current_position_and_records_the_remaining_size() -> None:
    """Bytes before the stream's position are not part of the file, so they are neither sent nor counted."""
    clients = mock_clients()
    drive = managed_drive(clients)
    stream = io.BytesIO(b"header-a,b\n1,2\n")
    stream.seek(len(b"header-"))
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/run-001.csv")

    with _patch_upload() as upload:
        drive.put_file_obj(stream, "data/run-001.csv")

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.put.size_bytes == len(b"a,b\n1,2\n")
    uploaded_stream = upload.call_args.args[2]
    assert uploaded_stream.read() == b"a,b\n1,2\n"


def test_put_file_obj_rejects_streams_it_cannot_measure_before_uploading() -> None:
    """Text-mode, unseekable, and exhausted streams must fail before any bytes are sent."""
    clients = mock_clients()
    drive = managed_drive(clients)
    unseekable = MagicMock(spec=io.BufferedReader)
    unseekable.seekable.return_value = False
    exhausted = io.BytesIO(b"a,b\n")
    exhausted.read()

    with pytest.raises(TypeError):
        drive.put_file_obj(io.StringIO("a,b\n"), "data/x.csv")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="seekable"):
        drive.put_file_obj(unseekable, "data/x.csv")
    with pytest.raises(ValueError, match="empty"):
        drive.put_file_obj(exhausted, "data/x.csv")

    clients.upload.initiate_multipart_upload.assert_not_called()


def test_put_file_rejects_a_virtual_drive_before_uploading(tmp_path: pathlib.Path) -> None:
    """The guard must fire before the transfer — this is the whole point of checking locally."""
    clients = mock_clients()
    drive = virtual_drive(clients)

    with pytest.raises(NominalFileStoreError) as excinfo:
        drive.put_file(_local_file(tmp_path), "data/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.READ_ONLY_DRIVE
    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_put_file_rejects_a_read_only_managed_drive_before_uploading(tmp_path: pathlib.Path) -> None:
    """A managed (NOMINAL-sourced) drive can be read-only too; writability comes from the field, not the class."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(mutability=file_store_pb2.DRIVE_MUTABILITY_READ_ONLY))
    assert type(drive) is Drive

    with pytest.raises(NominalFileStoreError) as excinfo:
        drive.put_file(_local_file(tmp_path), "data/run-001.csv")

    assert excinfo.value.code is FileStoreErrorCode.READ_ONLY_DRIVE
    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_put_file_rejects_a_missing_path_a_directory_and_an_empty_file(tmp_path: pathlib.Path) -> None:
    """Problems with the local source surface as the usual filesystem errors, before uploading."""
    clients = mock_clients()
    drive = managed_drive(clients)
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
    clients = mock_clients()
    drive = managed_drive(clients)

    with pytest.raises(ValueError, match="no filename"):
        drive.put_file(_local_file(tmp_path), "data/")

    clients.upload.initiate_multipart_upload.assert_not_called()


def test_put_file_accepts_names_object_storage_would_reject() -> None:
    """The service keys drive uploads by its own id, so characters unsafe in an object key are fine in a drive."""
    clients = mock_clients()
    drive = managed_drive(clients)
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="reports/O'Brien 50%.pdf")

    with _patch_upload() as upload:
        drive.put_file_obj(io.BytesIO(b"%PDF"), "reports/O'Brien 50%.pdf")

    assert upload.call_args.args[3] == "O'Brien 50%.pdf"


def test_put_onto_an_existing_file_adds_a_version_to_it() -> None:
    """A file destination puts against its head revision, keeping the file's identity and history."""
    clients = mock_clients()
    drive = managed_drive(clients)
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(path="data/run-001.csv", revision_rid="ri.rev.head")
    )
    existing = drive.get_file("data/run-001.csv")
    assert isinstance(existing, ManagedDriveFile)
    clients.drive_files.ApplyFileChanges.return_value = change_success(path="data/run-001.csv")

    with _patch_upload() as upload:
        drive.put_file_obj(io.BytesIO(b"a,b\n3,4\n"), existing)

    change = clients.drive_files.ApplyFileChanges.call_args.args[0].changes[0]
    assert change.put.destination.file_revision_rid == "ri.rev.head"
    assert upload.call_args.args[3] == "run-001.csv"


def test_put_onto_a_file_in_another_drive_is_rejected_before_uploading() -> None:
    """A destination from another drive can never apply, so it must fail before any bytes move."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    elsewhere = Drive._from_proto(clients, drive_proto(rid="ri.drive.OTHER")).get_file("data/run-001.csv")
    assert isinstance(elsewhere, ManagedDriveFile)

    with pytest.raises(ValueError, match="different drive"):
        managed_drive(clients).put_file_obj(io.BytesIO(b"a"), elsewhere)

    clients.upload.initiate_multipart_upload.assert_not_called()
