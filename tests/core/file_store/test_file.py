from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store.drive import Drive
from nominal.core.file_store.enums import DriveFileState, DriveSource
from nominal.core.file_store.file import (
    DriveDirectory,
    DriveFile,
    ManagedDriveFile,
    VirtualDriveFile,
)
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from tests.core.file_store.test_drive import _clients, _drive_proto


def _managed_file_proto(
    path: str = "data/run-001.csv",
    *,
    rid: str = "ri.drive-file.1",
    revision_rid: str = "ri.drive-file-revision.1",
    state: file_store_pb2.FileState.ValueType = file_store_pb2.FILE_STATE_ACTIVE,
    size_bytes: int = 2048,
) -> file_store_pb2.LogicalFile:
    msg = file_store_pb2.LogicalFile(
        identity=file_store_pb2.LogicalFileIdentity(managed=file_store_pb2.ManagedFileIdentity(file_rid=rid)),
        path=file_store_pb2.LogicalPath(path=path),
        state=state,
        size_bytes=size_bytes,
        current_revision=file_store_pb2.FileRevisionRef(
            managed=file_store_pb2.ManagedFileRevisionRef(file_revision_rid=revision_rid)
        ),
    )
    msg.created.time.FromNanoseconds(1_700_000_000_000_000_000)
    msg.created.user_rid = "ri.user.1"
    msg.observed.time.FromNanoseconds(1_700_000_001_000_000_000)
    return msg


def _virtual_file_proto(path: str = "logs/boot.txt", *, etag: str = "etag-1") -> file_store_pb2.LogicalFile:
    return file_store_pb2.LogicalFile(
        identity=file_store_pb2.LogicalFileIdentity(
            virtual=file_store_pb2.VirtualFileIdentity(
                s3=file_store_pb2.S3FileIdentity(drive_rid="ri.drive.1", path=path)
            )
        ),
        path=file_store_pb2.LogicalPath(path=path),
        state=file_store_pb2.FILE_STATE_ACTIVE,
        size_bytes=17,
        current_revision=file_store_pb2.FileRevisionRef(
            virtual=file_store_pb2.VirtualFileRevisionRef(
                s3=file_store_pb2.S3FileRevisionRef(drive_rid="ri.drive.1", path=path, etag=etag)
            )
        ),
    )


def _managed_drive(clients: MagicMock) -> Drive:
    return Drive._from_proto(clients, _drive_proto())


def _virtual_drive(clients: MagicMock) -> Drive:
    return Drive._from_proto(clients, _drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))


def test_get_file_returns_a_managed_file_with_its_identity() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=_managed_file_proto(), drive_rid="ri.drive.1"
    )

    file = _managed_drive(clients).get_file("data/run-001.csv")

    assert isinstance(file, ManagedDriveFile)
    assert file.rid == "ri.drive-file.1"
    assert file.path == "data/run-001.csv"
    assert file.size_bytes == 2048
    assert file.state is DriveFileState.ACTIVE
    assert file.current_revision_rid == "ri.drive-file-revision.1"
    assert file.created_by_rid == "ri.user.1"
    request = clients.drive_files.GetFile.call_args.args[0]
    assert request.drive_rid == "ri.drive.1"
    assert request.path.path == "data/run-001.csv"
    assert request.include_removed is False


def test_get_file_on_a_virtual_drive_returns_a_virtual_file() -> None:
    """Listing a provider-backed drive from Python must work, not raise."""
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=_virtual_file_proto(), drive_rid="ri.drive.1"
    )

    file = _virtual_drive(clients).get_file("logs/boot.txt")

    assert isinstance(file, VirtualDriveFile)
    assert file.provider is DriveSource.S3
    assert file.path == "logs/boot.txt"
    assert file.size_bytes == 17


def test_list_files_mixes_directories_and_files_and_follows_pages() -> None:
    clients = _clients()
    clients.drive_files.ListFiles.side_effect = [
        files_pb2.ListFilesResponse(
            entries=[
                file_store_pb2.FileEntry(
                    directory=file_store_pb2.Directory(path=file_store_pb2.LogicalPath(path="data/raw"))
                )
            ],
            next_page_token="page-2",
        ),
        files_pb2.ListFilesResponse(entries=[file_store_pb2.FileEntry(file=_managed_file_proto())]),
    ]

    entries = _managed_drive(clients).list_files("data")

    assert [e.path for e in entries] == ["data/raw", "data/run-001.csv"]
    assert isinstance(entries[0], DriveDirectory)
    assert isinstance(entries[1], ManagedDriveFile)
    assert clients.drive_files.ListFiles.call_args_list[0].args[0].parent_path.path == "data"
    assert clients.drive_files.ListFiles.call_args_list[1].args[0].page_token == "page-2"


def test_list_files_defaults_to_the_drive_root() -> None:
    clients = _clients()
    clients.drive_files.ListFiles.side_effect = [files_pb2.ListFilesResponse(entries=[])]

    _managed_drive(clients).list_files()

    assert clients.drive_files.ListFiles.call_args.args[0].parent_path.path == ""


def test_unknown_file_state_survives_as_unknown() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto(state=999))

    file = _managed_drive(clients).get_file("data/run-001.csv")

    assert file.state is DriveFileState.UNKNOWN


def test_revisions_are_returned_in_backend_order_across_pages() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto())
    file = _managed_drive(clients).get_file("data/run-001.csv")
    clients.drive_files.ListFileRevisions.side_effect = [
        files_pb2.ListFileRevisionsResponse(
            file_revisions=[
                file_store_pb2.ManagedFileRevision(
                    file_revision_rid="ri.rev.1",
                    file_rid="ri.drive-file.1",
                    path=file_store_pb2.LogicalPath(path="data/run-001.csv"),
                    size_bytes=2048,
                    state=file_store_pb2.FILE_STATE_ACTIVE,
                )
            ],
            next_page_token="page-2",
        ),
        files_pb2.ListFileRevisionsResponse(
            file_revisions=[
                file_store_pb2.ManagedFileRevision(
                    file_revision_rid="ri.rev.2",
                    file_rid="ri.drive-file.1",
                    path=file_store_pb2.LogicalPath(path="data/run-001.csv"),
                    state=file_store_pb2.FILE_STATE_REMOVED,
                )
            ]
        ),
    ]

    revisions = file.revisions()

    assert [r.rid for r in revisions] == ["ri.rev.1", "ri.rev.2"]
    assert revisions[1].state is DriveFileState.REMOVED
    request = clients.drive_files.ListFileRevisions.call_args_list[0].args[0]
    assert request.drive_rid == "ri.drive.1"
    assert request.file_rid == "ri.drive-file.1"


def test_virtual_file_refuses_revision_history_without_a_request() -> None:
    """History is unavailable for virtual drives; fail locally rather than round-trip."""
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_virtual_file_proto())
    file = _virtual_drive(clients).get_file("logs/boot.txt")

    with pytest.raises(NominalFileStoreError) as excinfo:
        file.revisions()

    assert excinfo.value.code is FileStoreErrorCode.FILE_HISTORY_NOT_AVAILABLE
    clients.drive_files.ListFileRevisions.assert_not_called()


def test_virtual_file_resolves_to_a_pinned_revision_rid() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_virtual_file_proto())
    file = _virtual_drive(clients).get_file("logs/boot.txt")
    clients.drive_files.ResolveFileRevision.return_value = files_pb2.ResolveFileRevisionResponse(
        file_revision_rid="ri.drive-file-revision.pinned"
    )

    assert isinstance(file, VirtualDriveFile)
    assert file.resolve() == "ri.drive-file-revision.pinned"
    source_ref = clients.drive_files.ResolveFileRevision.call_args.args[0].source_ref
    assert source_ref.virtual.s3.etag == "etag-1"


def test_managed_file_refreshes_via_its_own_identity() -> None:
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_managed_file_proto())
    file = _managed_drive(clients).get_file("data/run-001.csv")
    clients.drive_files.GetFileByIdentity.return_value = files_pb2.GetFileByIdentityResponse(
        file=_managed_file_proto(path="archive/run-001.csv"), drive_rid="ri.drive.1"
    )

    returned = file.refresh()

    assert returned is file
    assert file.path == "archive/run-001.csv"
    identity = clients.drive_files.GetFileByIdentity.call_args.args[0].identity
    assert identity.managed.file_rid == "ri.drive-file.1"


def test_drive_file_base_exposes_shared_fields_without_narrowing() -> None:
    """A listing's shared read surface must be reachable on the base type."""
    clients = _clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_virtual_file_proto())
    file: DriveFile = _virtual_drive(clients).get_file("logs/boot.txt")

    assert file.path == "logs/boot.txt"
    assert file.size_bytes == 17
    assert file.state is DriveFileState.ACTIVE
