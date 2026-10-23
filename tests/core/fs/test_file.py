from __future__ import annotations

from nominal.core.fs.enums import DriveFileState, DriveSource
from nominal.core.fs.file import (
    DriveDirectory,
    DriveFileRevision,
    ManagedDriveFile,
    VirtualDriveFile,
)
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from tests.core.fs._fixtures import (
    managed_drive,
    managed_file_proto,
    mock_clients,
    revision_proto,
    virtual_drive,
    virtual_file_proto,
)


def test_get_file_returns_a_managed_file_with_its_identity() -> None:
    """A managed file carries its RID, current revision, and attribution from the response."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=managed_file_proto(), drive_rid="ri.drive.1"
    )

    file = managed_drive(clients).get_file("data/run-001.csv")

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
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(
        file=virtual_file_proto(), drive_rid="ri.drive.1"
    )

    file = virtual_drive(clients).get_file("logs/boot.txt")

    assert isinstance(file, VirtualDriveFile)
    assert file.provider is DriveSource.S3
    assert file.path == "logs/boot.txt"
    assert file.size_bytes == 17


def test_list_files_mixes_directories_and_files_and_follows_pages() -> None:
    """A listing collects every page and keeps directories and files as distinct entry types."""
    clients = mock_clients()
    clients.drive_files.ListFiles.side_effect = [
        files_pb2.ListFilesResponse(
            entries=[
                file_store_pb2.FileEntry(
                    directory=file_store_pb2.Directory(path=file_store_pb2.LogicalPath(path="data/raw"))
                )
            ],
            next_page_token="page-2",
        ),
        files_pb2.ListFilesResponse(entries=[file_store_pb2.FileEntry(file=managed_file_proto())]),
    ]

    entries = managed_drive(clients).list_files("data")

    assert [e.path for e in entries] == ["data/raw", "data/run-001.csv"]
    assert isinstance(entries[0], DriveDirectory)
    assert isinstance(entries[1], ManagedDriveFile)
    assert clients.drive_files.ListFiles.call_args_list[0].args[0].parent_path.path == "data"
    assert clients.drive_files.ListFiles.call_args_list[1].args[0].page_token == "page-2"


def test_list_files_defaults_to_the_drive_root() -> None:
    """Listing with no parent path lists the drive root, which the backend spells as the empty path."""
    clients = mock_clients()
    clients.drive_files.ListFiles.side_effect = [files_pb2.ListFilesResponse(entries=[])]

    managed_drive(clients).list_files()

    assert clients.drive_files.ListFiles.call_args.args[0].parent_path.path == ""


def test_revisions_are_returned_in_backend_order_across_pages() -> None:
    """Revision history is collected across pages in backend order, removal markers included."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    clients.drive_files.ListFileRevisions.side_effect = [
        files_pb2.ListFileRevisionsResponse(
            file_revisions=[revision_proto("ri.rev.1")],
            next_page_token="page-2",
        ),
        files_pb2.ListFileRevisionsResponse(
            file_revisions=[revision_proto("ri.rev.2", size_bytes=0, state=file_store_pb2.FILE_STATE_REMOVED)]
        ),
    ]

    revisions = file.revisions()

    assert [r.rid for r in revisions] == ["ri.rev.1", "ri.rev.2"]
    assert revisions[1].state is DriveFileState.REMOVED
    request = clients.drive_files.ListFileRevisions.call_args_list[0].args[0]
    assert request.drive_rid == "ri.drive.1"
    assert request.file_rid == "ri.drive-file.1"


def test_virtual_file_resolves_to_a_pinned_revision_rid() -> None:
    """Resolving sends the observed revision reference and returns the pinned RID."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=virtual_file_proto())
    file = virtual_drive(clients).get_file("logs/boot.txt")
    clients.drive_files.ResolveFileRevision.return_value = files_pb2.ResolveFileRevisionResponse(
        file_revision_rid="ri.drive-file-revision.pinned"
    )

    assert isinstance(file, VirtualDriveFile)
    assert file.resolve() == "ri.drive-file-revision.pinned"
    source_ref = clients.drive_files.ResolveFileRevision.call_args.args[0].source_ref
    assert source_ref.virtual.s3.etag == "etag-1"


def test_managed_file_refreshes_via_its_own_identity() -> None:
    """Refreshing looks the file up by identity, so it follows the file after a move, and updates in place."""
    clients = mock_clients()
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=managed_file_proto())
    file = managed_drive(clients).get_file("data/run-001.csv")
    clients.drive_files.GetFileByIdentity.return_value = files_pb2.GetFileByIdentityResponse(
        file=managed_file_proto(path="archive/run-001.csv"), drive_rid="ri.drive.1"
    )

    returned = file.refresh()

    assert returned is file
    assert file.path == "archive/run-001.csv"
    identity = clients.drive_files.GetFileByIdentity.call_args.args[0].identity
    assert identity.managed.file_rid == "ri.drive-file.1"


def test_virtual_files_can_be_collected_into_a_set() -> None:
    """Listings are hashable on every drive, though a virtual file carries a generated revision reference."""
    clients = mock_clients()
    clients.drive_files.ListFiles.side_effect = [
        files_pb2.ListFilesResponse(entries=[file_store_pb2.FileEntry(file=virtual_file_proto())])
    ]

    entries = set(virtual_drive(clients).list_files())

    assert {entry.path for entry in entries} == {"logs/boot.txt"}


def test_a_revision_from_a_server_without_refs_keeps_its_rids() -> None:
    """Older servers identify a revision only by its deprecated rid fields, which must still convert."""
    legacy = file_store_pb2.ManagedFileRevision(file_revision_rid="ri.rev.old", file_rid="ri.drive-file.1")

    revision = DriveFileRevision._from_proto(mock_clients(), "ri.drive.1", legacy)

    assert (revision.rid, revision.file_rid) == ("ri.rev.old", "ri.drive-file.1")
