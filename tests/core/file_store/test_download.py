from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest

from nominal.core._utils.multipart_downloader import DownloadItem
from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store.file import DriveFileRevision, ManagedDriveFile
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from tests.core.file_store.test_drive import _clients
from tests.core.file_store.test_file import _managed_drive, _managed_file_proto, _virtual_drive, _virtual_file_proto


def _download_file_via_the_provider(item: DownloadItem) -> pathlib.Path:
    """Stand-in for the real downloader's synchronous planning phase, which resolves the
    presigned URL through the item's provider before any part is fetched.
    """
    item.provider.get_url()
    return item.destination


def _mock_downloader(monkeypatch) -> MagicMock:
    downloader = MagicMock()
    downloader.__enter__.return_value.download_file.side_effect = _download_file_via_the_provider
    monkeypatch.setattr(
        "nominal.core.file_store.file.MultipartFileDownloader.create", MagicMock(return_value=downloader)
    )
    return downloader


def test_download_writes_to_the_drive_paths_basename(tmp_path: pathlib.Path, monkeypatch) -> None:
    """The presigned URL names an opaque object, so the filename comes from the drive path."""
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto())
    file = _managed_drive(clients).get_file("data/run-001.csv")
    assert isinstance(file, ManagedDriveFile)
    clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://s3.example.com/signed"
    )
    _mock_downloader(monkeypatch)

    destination = file.download(tmp_path)

    assert destination == tmp_path / "run-001.csv"
    assert clients.drive_files.GetDownloadUrl.call_args.args[0].file_revision_rid == "ri.drive-file-revision.1"


def test_revision_download_uses_its_own_rid_and_path_basename(tmp_path: pathlib.Path, monkeypatch) -> None:
    """A revision downloads by its own rid, named after its own path — not the file's current one."""
    clients = _clients()
    revision = DriveFileRevision._from_proto(
        clients,
        "ri.drive.1",
        file_store_pb2.ManagedFileRevision(
            file_revision_rid="ri.drive-file-revision.old",
            file_rid="ri.drive-file.1",
            path=file_store_pb2.LogicalPath(path="archive/run-001-old.csv"),
            size_bytes=2048,
            state=file_store_pb2.FILE_STATE_ACTIVE,
        ),
    )
    clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://s3.example.com/signed"
    )
    _mock_downloader(monkeypatch)

    destination = revision.download(tmp_path)

    assert destination == tmp_path / "run-001-old.csv"
    assert clients.drive_files.GetDownloadUrl.call_args.args[0].file_revision_rid == "ri.drive-file-revision.old"


def test_download_of_a_file_with_no_current_revision_is_rejected(tmp_path: pathlib.Path) -> None:
    clients = _clients()
    headless = files_pb2.GetFileResponse(file=_managed_file_proto())
    headless.file.ClearField("current_revision")
    clients.drive_files.GetFile.return_value = headless
    file = _managed_drive(clients).get_file("data/run-001.csv")

    with pytest.raises(NominalFileStoreError):
        file.download(tmp_path)

    clients.drive_files.GetDownloadUrl.assert_not_called()


def test_virtual_files_refuse_download_without_a_request(tmp_path: pathlib.Path) -> None:
    """Content for a provider-backed file is not served through this API."""
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_virtual_file_proto())
    file = _virtual_drive(clients).get_file("logs/boot.txt")

    with pytest.raises(NominalFileStoreError) as excinfo:
        file.download(tmp_path)

    assert excinfo.value.code is FileStoreErrorCode.FILE_HISTORY_NOT_AVAILABLE
    clients.drive_files.GetDownloadUrl.assert_not_called()


def test_download_rejects_a_non_directory_destination(tmp_path: pathlib.Path) -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto())
    file = _managed_drive(clients).get_file("data/run-001.csv")
    not_a_dir = tmp_path / "file.txt"
    not_a_dir.write_bytes(b"x")

    with pytest.raises(NotADirectoryError):
        file.download(not_a_dir)

    clients.drive_files.GetDownloadUrl.assert_not_called()
