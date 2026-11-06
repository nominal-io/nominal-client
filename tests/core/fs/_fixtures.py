from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.fs.drive import Drive
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2


def mock_clients() -> MagicMock:
    """Build a clients bunch whose stubs are mocks and whose workspace resolves to a default."""
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    clients.resolve_workspace.return_value.rid = "ri.workspace.default"
    return clients


def drive_proto(
    rid: str = "ri.drive.1",
    *,
    id: str = "telemetry",
    source: file_store_pb2.DriveSource.ValueType = file_store_pb2.DRIVE_SOURCE_NOMINAL,
    state: file_store_pb2.DriveState.ValueType = file_store_pb2.DRIVE_STATE_ACTIVE,
    mutability: file_store_pb2.DriveMutability.ValueType = file_store_pb2.DRIVE_MUTABILITY_WRITABLE,
    description: str | None = None,
) -> file_store_pb2.Drive:
    """Build a drive response, managed and writable unless told otherwise."""
    drive = file_store_pb2.Drive(
        rid=rid,
        workspace_rid="ri.workspace.default",
        id=id,
        state=state,
        source=source,
        content_mutability=mutability,
        description=description,
    )
    drive.created.time.FromNanoseconds(1_700_000_000_000_000_000)
    drive.created.user_rid = "ri.user.1"
    return drive


def managed_drive(clients: MagicMock) -> Drive:
    """Build a writable Nominal-backed drive."""
    return Drive._from_proto(clients, drive_proto())


def virtual_drive(clients: MagicMock) -> Drive:
    """Build a read-only S3-backed drive, as the backend reports provider-backed drives."""
    return Drive._from_proto(
        clients,
        drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3, mutability=file_store_pb2.DRIVE_MUTABILITY_READ_ONLY),
    )


def managed_file_proto(
    path: str = "data/run-001.csv",
    *,
    rid: str = "ri.drive-file.1",
    revision_rid: str = "ri.drive-file-revision.1",
    state: file_store_pb2.FileState.ValueType = file_store_pb2.FILE_STATE_ACTIVE,
    size_bytes: int = 2048,
) -> file_store_pb2.LogicalFile:
    """Build a managed file response with attribution and a current revision."""
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


def virtual_file_proto(path: str = "logs/boot.txt", *, etag: str = "etag-1") -> file_store_pb2.LogicalFile:
    """Build an S3-mirrored file response observed at ``etag``."""
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


def revision_proto(
    rid: str,
    *,
    path: str = "data/run-001.csv",
    size_bytes: int = 2048,
    state: file_store_pb2.FileState.ValueType = file_store_pb2.FILE_STATE_ACTIVE,
) -> file_store_pb2.ManagedFileRevision:
    """Build a revision of file ``ri.drive-file.1``, identified the way current servers do."""
    return file_store_pb2.ManagedFileRevision(
        ref=file_store_pb2.FileRevisionRef(managed=file_store_pb2.ManagedFileRevisionRef(file_revision_rid=rid)),
        identity=file_store_pb2.LogicalFileIdentity(
            managed=file_store_pb2.ManagedFileIdentity(file_rid="ri.drive-file.1")
        ),
        path=file_store_pb2.LogicalPath(path=path),
        size_bytes=size_bytes,
        state=state,
    )


def change_success(path: str = "archive/run-001.csv") -> files_pb2.ApplyFileChangesResponse:
    """Build a response in which a single change succeeded, leaving the file at ``path``."""
    return files_pb2.ApplyFileChangesResponse(
        results=[
            files_pb2.FileChangeResult(
                success=files_pb2.FileChangeSuccess(
                    file=managed_file_proto(path=path),
                    file_revision=revision_proto("ri.rev.2", path=path),
                )
            )
        ]
    )
