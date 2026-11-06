from __future__ import annotations

import pathlib
import sys
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.multipart_downloader import DownloadItem, MultipartFileDownloader
from nominal.core.fs.errors import NominalFileStoreError
from nominal.core.fs.file import DriveFileRevision, ManagedDriveFile, VirtualDriveFile
from nominal.protos.file_store.v1 import files_pb2
from tests.core.fs._fixtures import (
    managed_drive,
    managed_file_proto,
    mock_clients,
    revision_proto,
    virtual_drive,
    virtual_file_proto,
)


def _download_file_via_the_provider(item: DownloadItem) -> pathlib.Path:
    """Stand-in for the real downloader's synchronous planning phase, which resolves the
    presigned URL through the item's provider before any part is fetched.
    """
    item.provider.get_url()
    return item.destination


def _downloader() -> MagicMock:
    downloader = MagicMock()
    downloader.__enter__.return_value.download_file.side_effect = _download_file_via_the_provider
    return downloader


def _revision(path: str) -> DriveFileRevision:
    return DriveFileRevision._from_proto(
        mock_clients(),
        "ri.drive.1",
        revision_proto("ri.drive-file-revision.old", path=path),
    )


def test_download_writes_to_the_drive_paths_basename(tmp_path: pathlib.Path) -> None:
    """The presigned URL names an opaque object, so the filename comes from the drive path."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    assert isinstance(file, ManagedDriveFile)
    clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://s3.example.com/signed"
    )
    with patch.object(MultipartFileDownloader, "create", return_value=_downloader()):
        destination = file.download(tmp_path)

    assert destination == tmp_path / "run-001.csv"
    assert clients.drive_files.GetDownloadUrl.call_args.args[0].file_revision_rid == "ri.drive-file-revision.1"


def test_revision_download_uses_its_own_rid_and_path_basename(tmp_path: pathlib.Path) -> None:
    """A revision downloads by its own rid, named after its own path — not the file's current one."""
    revision = _revision("archive/run-001-old.csv")
    revision._clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://s3.example.com/signed"
    )
    with patch.object(MultipartFileDownloader, "create", return_value=_downloader()):
        destination = revision.download(tmp_path)

    assert destination == tmp_path / "run-001-old.csv"
    request = revision._clients.drive_files.GetDownloadUrl.call_args.args[0]
    assert request.file_revision_rid == "ri.drive-file-revision.old"


def test_download_of_a_file_with_no_current_revision_is_rejected(tmp_path: pathlib.Path) -> None:
    """A managed file with nothing at its head has no content to fetch, so no URL is requested."""
    clients = mock_clients()
    headless = files_pb2.GetFileResponse(file=managed_file_proto())
    headless.file.ClearField("current_revision")
    clients.drive_files.GetFile.return_value = headless
    file = managed_drive(clients).get_file("data/run-001.csv")

    with pytest.raises(NominalFileStoreError):
        file.download(tmp_path)

    clients.drive_files.GetDownloadUrl.assert_not_called()


def test_virtual_file_downloads_the_revision_its_observed_content_resolves_to(tmp_path: pathlib.Path) -> None:
    """The backend serves resolved virtual revisions, so downloading pins the observed content first."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=virtual_file_proto())
    file = virtual_drive(clients).get_file("logs/boot.txt")
    assert isinstance(file, VirtualDriveFile)
    clients.drive_files.ResolveFileRevision.return_value = files_pb2.ResolveFileRevisionResponse(
        file_revision_rid="ri.drive-file-revision.pinned"
    )
    clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://nominal.example.com/proxied"
    )
    with patch.object(MultipartFileDownloader, "create", return_value=_downloader()):
        destination = file.download(tmp_path)

    assert destination == tmp_path / "boot.txt"
    assert clients.drive_files.ResolveFileRevision.call_args.args[0].source_ref.virtual.s3.etag == "etag-1"
    assert clients.drive_files.GetDownloadUrl.call_args.args[0].file_revision_rid == "ri.drive-file-revision.pinned"


@pytest.mark.parametrize(
    ("output", "error"),
    [("file.txt", NotADirectoryError), ("missing", FileNotFoundError), ("occupied", FileExistsError)],
)
def test_virtual_file_download_checks_the_destination_before_resolving(
    tmp_path: pathlib.Path, output: str, error: type[Exception]
) -> None:
    """Resolving pins content server-side, so every local destination problem must fail before that request."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=virtual_file_proto())
    file = virtual_drive(clients).get_file("logs/boot.txt")
    (tmp_path / "file.txt").write_bytes(b"x")
    (tmp_path / "occupied").mkdir()
    (tmp_path / "occupied" / "boot.txt").write_bytes(b"x")

    with pytest.raises(error):
        file.download(tmp_path / output)

    clients.drive_files.ResolveFileRevision.assert_not_called()


@pytest.mark.skipif(sys.platform != "win32", reason="only Windows lets a drive-qualified name replace the directory")
def test_download_rejects_a_drive_qualified_basename(tmp_path: pathlib.Path) -> None:
    """A segment such as `D:evil.csv` is a legal drive path but would land outside the directory on Windows."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(path="data/D:evil.csv")
    )
    file = managed_drive(clients).get_file("data/D:evil.csv")

    with pytest.raises(ValueError):
        file.download(tmp_path)

    clients.drive_files.GetDownloadUrl.assert_not_called()


def test_download_rejects_a_basename_containing_a_backslash(tmp_path: pathlib.Path) -> None:
    """A single legal path segment can still carry a backslash — `directory / filename` would
    treat that as a separator on Windows and escape `output_directory`.
    """
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(path=r"data/..\..\evil.csv")
    )
    file = managed_drive(clients).get_file(r"data/..\..\evil.csv")
    assert isinstance(file, ManagedDriveFile)

    with pytest.raises(ValueError):
        file.download(tmp_path)

    clients.drive_files.GetDownloadUrl.assert_not_called()


def test_revision_download_rejects_a_basename_containing_a_backslash(tmp_path: pathlib.Path) -> None:
    """Revisions are named after their own path, so they need the same separator guard as files."""
    revision = _revision(r"archive/..\..\evil.csv")

    with pytest.raises(ValueError):
        revision.download(tmp_path)

    revision._clients.drive_files.GetDownloadUrl.assert_not_called()


def test_download_uses_a_sixty_second_presign_ttl_with_a_twenty_second_refresh_skew(tmp_path: pathlib.Path) -> None:
    """Load-bearing, not a guess: the server presigns for exactly one minute, and a long ranged
    download only survives because the provider refreshes on this schedule before it expires.
    """
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    clients.drive_files.GetDownloadUrl.return_value = files_pb2.GetDownloadUrlResponse(
        url="https://s3.example.com/signed"
    )
    with patch.object(MultipartFileDownloader, "create", return_value=_downloader()) as create:
        file.download(tmp_path)

    item = create.return_value.__enter__.return_value.download_file.call_args.args[0]
    assert item.provider.ttl_secs == 60.0
    assert item.provider.skew_secs == 20.0


def test_download_rejects_a_non_directory_destination(tmp_path: pathlib.Path) -> None:
    """An output path that is a file fails locally rather than after fetching a URL."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    not_a_dir = tmp_path / "file.txt"
    not_a_dir.write_bytes(b"x")

    with pytest.raises(NotADirectoryError):
        file.download(not_a_dir)

    clients.drive_files.GetDownloadUrl.assert_not_called()
