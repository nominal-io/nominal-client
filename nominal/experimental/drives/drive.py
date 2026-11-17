"""Drives: workspace-scoped namespaces for files (nominal.file_store.v1).

A managed drive stores files in Nominal's own storage and can be written through this SDK.
A virtual drive mirrors an external provider (S3, Google Drive, GCS) and is read-only —
it is returned as `VirtualDrive`, whose write methods refuse before spending a request.
"""

from __future__ import annotations

import io
import pathlib
from dataclasses import dataclass, field
from typing import BinaryIO, Sequence, cast

from nominal_api import ingest_api
from typing_extensions import Self

from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE, DEFAULT_NUM_WORKERS, _put_multipart_upload_to
from nominal.core.client import NominalClient
from nominal.core.filetype import FileType
from nominal.experimental.drives._clients import _Clients
from nominal.experimental.drives._pagination import list_drives_paginated, list_files_paginated
from nominal.experimental.drives.changes import DriveBatch, PreparedPut, _move_directory_change
from nominal.experimental.drives.enums import DriveMutability, DriveSource, DriveState, VirtualDriveState
from nominal.experimental.drives.errors import FileStoreErrorCode, NominalFileStoreError
from nominal.experimental.drives.file import (
    DriveEntry,
    DriveFile,
    FileDestination,
    ManagedDriveFile,
    _apply_one,
    _attribution,
    _basename,
    _destination_to_proto,
    _entry_from_proto,
    _file_from_proto,
)
from nominal.protos.file_store.v1 import drives_pb2, file_store_pb2, files_pb2
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class VirtualDriveStatus:
    """Connectivity of a virtual drive's backing provider, probed when requested."""

    state: VirtualDriveState
    message: str
    last_successful_check_at: IntegralNanosecondsUTC | None
    """When the provider was last reached successfully; None if it never has been."""

    @classmethod
    def _from_proto(cls, msg: file_store_pb2.VirtualDriveStatus) -> Self:
        return cls(
            state=VirtualDriveState._from_proto(msg.state),
            message=msg.message,
            last_successful_check_at=(
                msg.last_successful_check_time.ToNanoseconds() if msg.HasField("last_successful_check_time") else None
            ),
        )


@dataclass(frozen=True)
class Drive(HasRid, RefreshableMixin[file_store_pb2.Drive]):
    """A workspace-scoped namespace for files."""

    rid: str
    id: str | None
    """Human-readable identifier, unique within the workspace, or ``None`` for a subordinate drive."""
    description: str | None
    """Free-text description, or ``None`` when the drive has none."""
    workspace_rid: str
    state: DriveState
    source: DriveSource
    """Where the drive's files come from."""
    content_mutability: DriveMutability
    """Whether the drive's contents can be modified through Nominal."""
    created_at: IntegralNanosecondsUTC | None
    created_by_rid: str | None
    subordinate_to_rid: str | None
    """Owning resource RID for a subordinate drive, otherwise ``None``."""
    _clients: _Clients = field(repr=False)

    def _get_latest_api(self) -> file_store_pb2.Drive:
        with translate_grpc_errors():
            return self._clients.drives.GetDrive(drives_pb2.GetDriveRequest(drive_rid=self.rid)).drive

    def _refresh_to_self(self, api_obj: file_store_pb2.Drive) -> Self:
        # `_from_proto` picks the concrete class from the drive's source, which the type system
        # cannot tie back to `Self`. A drive's source never changes, so this is sound. This is why
        # `Drive` subclasses `RefreshableMixin` directly rather than `RefreshableGrpcMixin`, whose
        # `_from_proto` must return `Self` — the same reason `ContainerImage` does.
        return cast("Self", Drive._from_proto(self._clients, api_obj))

    def update(self, *, id: str | None = None, description: str | None = None) -> Self:
        """Update this drive's details, refreshing it in place.

        Args:
            id: New identifier for the drive, unique within the workspace. Up to 100 letters,
                digits, ``.``, ``-``, and ``_``, starting with a letter or digit; ids starting
                with ``nominal.`` in any casing are reserved.
            description: New description for the drive. An empty string clears it.

        Returns:
            This instance, updated.

        Raises:
            NominalAlreadyExistsError: Another drive in the workspace already has ``id``.

        Note:
            Fields left as ``None`` are unchanged. Updating a drive requires
            organization-admin permissions.
        """
        request = drives_pb2.UpdateDriveDetailsRequest(drive_rid=self.rid, id=id, description=description)
        with translate_grpc_errors():
            response = self._clients.drives.UpdateDriveDetails(request)
        return self._refresh_from_api(response.drive)

    def archive(self) -> Self:
        """Archive this drive, hiding it from listings that exclude archived drives.

        Returns:
            This instance, updated.

        Note:
            Archiving a drive requires organization-admin permissions.
        """
        with translate_grpc_errors():
            response = self._clients.drives.ArchiveDrive(drives_pb2.ArchiveDriveRequest(drive_rid=self.rid))
        return self._refresh_from_api(response.drive)

    def unarchive(self) -> Self:
        """Unarchive this drive, restoring it to listings.

        Returns:
            This instance, updated.

        Note:
            Unarchiving a drive requires organization-admin permissions.
        """
        with translate_grpc_errors():
            response = self._clients.drives.UnarchiveDrive(drives_pb2.UnarchiveDriveRequest(drive_rid=self.rid))
        return self._refresh_from_api(response.drive)

    def get_file(self, path: str, *, include_removed: bool = False) -> DriveFile:
        """Retrieve the file at a path in this drive.

        Args:
            path: Drive-relative path, with no leading or trailing slash and no ``.`` or
                ``..`` segments.
            include_removed: Also match a file that has been removed from this path.

        Returns:
            The file. This is a `ManagedDriveFile` in a managed drive and a
            `VirtualDriveFile` in a provider-backed one.

        Raises:
            NominalNotFoundError: No file exists at that path.
        """
        request = files_pb2.GetFileRequest(
            drive_rid=self.rid,
            path=file_store_pb2.LogicalPath(path=path),
            include_removed=include_removed,
        )
        with translate_grpc_errors():
            response = self._clients.drive_files.GetFile(request)
        return _file_from_proto(self._clients, self.rid, response.file)

    def list_files(
        self, parent_path: str = "", *, include_removed: bool = False, recursive: bool = False
    ) -> Sequence[DriveEntry]:
        """List files and directories beneath a path in this drive.

        Args:
            parent_path: Directory to list. The default lists the drive root.
            include_removed: Include files that have been removed. Cannot be combined with ``recursive``.
            recursive: List active files at every depth, without directory entries. Managed drives only.

        Returns:
            Every page in backend order. By default, entries are the immediate child
            directories and files. Recursive results contain active files at every depth.

        Raises:
            ValueError: Recursive listing is requested for a virtual drive or with removed files.
        """
        if recursive and include_removed:
            raise ValueError("recursive listing cannot be combined with include_removed")
        if recursive and self.source is not DriveSource.NOMINAL:
            raise ValueError("recursive listing is only supported for managed drives")
        return [
            _entry_from_proto(self._clients, self.rid, msg)
            for msg in list_files_paginated(
                self._clients.drive_files, self.rid, parent_path, include_removed=include_removed, recursive=recursive
            )
        ]

    def move_directory(self, source: str, destination: str) -> None:
        """Move a directory and its active files within this drive.

        Args:
            source: Existing drive-relative directory path.
            destination: Unoccupied directory path. Must not contain or sit inside ``source``.

        Raises:
            NominalFileStoreError: The drive is read-only, the destination is occupied,
                the paths are invalid, or the move exceeds the service's limits.

        Note:
            Existing file snapshots retain their old paths until refreshed. The service
            returns no per-file details. A successful call means the move has been applied.

            File changes are not automatically replayed after transport errors, which can leave
            the outcome unknown. See :ref:`drive-change-retries` before retrying.
        """
        self._require_writable()
        _apply_one(self._clients, self.rid, _move_directory_change(source, destination))

    def batch(self) -> DriveBatch:
        """Create a single-use batch for this drive.

        Queue operations with `DriveBatch.move`, `DriveBatch.remove`, `DriveBatch.restore`,
        `DriveBatch.move_directory`, or `DriveBatch.put`, then call `DriveBatch.submit`.
        Upload preparation remains separate from batch submission.
        """
        return DriveBatch(self)

    def _require_writable(self) -> None:
        # Writability is read from the drive's own field rather than its class: a managed drive
        # can be read-only too, and every value this SDK doesn't recognize counts as not writable.
        if self.content_mutability is DriveMutability.WRITABLE:
            return
        backing = "" if self.source is DriveSource.NOMINAL else f" (backed by {self.source.value})"
        raise NominalFileStoreError(
            FileStoreErrorCode.READ_ONLY_DRIVE, f"drive {self.id!r}{backing} is read-only through Nominal"
        )

    def put_file(
        self,
        local_path: PathLike,
        destination: FileDestination,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> ManagedDriveFile:
        """Upload a local file into this drive, as a new file or a new version of an existing one.

        Args:
            local_path: File on disk to upload.
            destination: Where the upload goes. A path creates a file there and must be
                unoccupied; a file or revision adds a new version to that file, which must
                still be at that revision.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of threads uploading parts concurrently.

        Returns:
            The file as it now exists in the drive.

        Raises:
            FileNotFoundError: ``local_path`` does not exist.
            IsADirectoryError: ``local_path`` is a directory.
            ValueError: ``local_path`` is empty, ``destination`` is a path with no filename
                (for example it ends in ``/``), or ``destination`` is from a different drive.
            NominalFileStoreError: This drive is read-only, or the service rejected the new
                content — for example the path is occupied, or the file has moved on.

        Note:
            The service checks ``destination`` only once the bytes have been uploaded, so a
            rejection arrives after the transfer. A file or revision passed as ``destination``
            is not refreshed; use the returned file.

            File changes are not automatically replayed after transport errors, which can leave
            the outcome unknown. See :ref:`drive-change-retries` before retrying.
        """
        path = pathlib.Path(local_path)
        if path.is_dir():
            raise IsADirectoryError(f"expected a file, got a directory: {path}")
        with path.open("rb") as f:
            return self.put_file_obj(f, destination, chunk_size=chunk_size, max_workers=max_workers)

    def put_file_obj(
        self,
        file_obj: BinaryIO,
        destination: FileDestination,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> ManagedDriveFile:
        """Upload the contents of a binary file-like object into this drive.

        The object is read from its current position to its end. It must be seekable, because
        empty input is rejected before anything is uploaded.

        Args:
            file_obj: Seekable binary file-like object, e.g. ``open(path, "rb")`` or `io.BytesIO`.
            destination: Where the upload goes. A path creates a file there and must be
                unoccupied; a file or revision adds a new version to that file, which must
                still be at that revision.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of threads uploading parts concurrently.

        Returns:
            The file as it now exists in the drive.

        Raises:
            TypeError: ``file_obj`` is open in text mode.
            ValueError: ``file_obj`` is not seekable or has nothing left to read,
                ``destination`` is a path with no filename (for example it ends in ``/``), or
                ``destination`` is from a different drive.
            NominalFileStoreError: This drive is read-only, or the service rejected the new
                content — for example the path is occupied, or the file has moved on.

        Note:
            The service checks ``destination`` only once the bytes have been uploaded, so a
            rejection arrives after the transfer. A file or revision passed as ``destination``
            is not refreshed; use the returned file.

            File changes are not automatically replayed after transport errors, which can leave
            the outcome unknown. See :ref:`drive-change-retries` before retrying.
        """
        prepared = self.prepare_put_file_obj(
            file_obj,
            destination,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )
        result = _apply_one(self._clients, self.rid, files_pb2.FileChange(put=prepared._put))
        return ManagedDriveFile._from_proto(self._clients, self.rid, result.success.file)

    def prepare_put_file(
        self,
        local_path: PathLike,
        destination: FileDestination,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> PreparedPut:
        """Upload a local file for a later `DriveBatch.put` and `DriveBatch.submit`, without changing this drive.

        Args:
            local_path: Nonempty file on disk to upload.
            destination: Unoccupied path or a file/revision whose current head must match.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of concurrent upload threads.

        Returns:
            A prepared put with a pinned destination precondition. Submit it to this drive.

        Raises:
            FileNotFoundError: ``local_path`` does not exist.
            IsADirectoryError: ``local_path`` is a directory.
            ValueError: ``local_path`` is empty, or the destination has no filename or
                belongs to another drive.
            NominalFileStoreError: The drive is read-only.

        Note:
            The upload remains separate from the batch transaction. If preparation or the
            batch fails, already uploaded bytes are not rolled back. The destination is
            checked by the service only when the batch is applied. Input snapshots are not refreshed.
        """
        path = pathlib.Path(local_path)
        if path.is_dir():
            raise IsADirectoryError(f"expected a file, got a directory: {path}")
        with path.open("rb") as file_obj:
            return self.prepare_put_file_obj(file_obj, destination, chunk_size=chunk_size, max_workers=max_workers)

    def prepare_put_file_obj(
        self,
        file_obj: BinaryIO,
        destination: FileDestination,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> PreparedPut:
        """Upload a binary stream for a later `DriveBatch.put` and `DriveBatch.submit`, without changing this drive.

        Args:
            file_obj: Seekable binary stream, read from its current position to its end.
            destination: Unoccupied path or a file/revision whose current head must match.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of concurrent upload threads.

        Returns:
            A prepared put with a pinned destination precondition. The caller may close
            the stream after this method returns.

        Raises:
            TypeError: The stream is open in text mode.
            ValueError: The stream is not seekable or has no bytes left, or the destination
                has no filename or belongs to another drive.
            NominalFileStoreError: The drive is read-only.

        Note:
            Uploaded bytes are outside the batch transaction and are not rolled back by
            an aborted batch. Destination conflicts are reported only when applying the batch.
        """
        self._require_writable()
        if isinstance(file_obj, io.TextIOBase):
            raise TypeError(f"{file_obj!r} must be open in binary mode, rather than text mode")
        if not file_obj.seekable():
            raise ValueError(f"{file_obj!r} must be seekable so its size can be measured before uploading")
        start = file_obj.tell()
        size_bytes = file_obj.seek(0, io.SEEK_END) - start
        file_obj.seek(start)
        destination_path = destination if isinstance(destination, str) else destination.path
        if size_bytes <= 0:
            raise ValueError(f"cannot upload an empty file to {destination_path!r}")

        # The drive path decides the file's identity and its stored suffix, so the upload is
        # named, and typed, after the destination rather than wherever the bytes came from. The
        # service keys the stored object by its own id, so any legal drive filename is safe here.
        filename = _basename(destination_path)
        if not filename:
            raise ValueError(f"destination path {destination_path!r} has no filename")
        target = _destination_to_proto(destination, self.rid)
        mimetype = FileType.from_path(filename).mimetype
        uploaded = _put_multipart_upload_to(
            self._clients.auth_header,
            self.workspace_rid,
            file_obj,
            filename,
            mimetype,
            self._clients.upload,
            chunk_size=chunk_size,
            max_workers=max_workers,
            header_provider=self._clients.header_provider,
            destination=ingest_api.UploadDestination.FILE_STORE,
        )

        return PreparedPut(
            self.rid,
            files_pb2.PutFile(
                object=files_pb2.UploadedObjectRef(object_key=uploaded.key),
                destination=target,
            ),
        )

    @classmethod
    def _from_proto(cls, clients: _Clients, msg: file_store_pb2.Drive) -> Drive:
        source = DriveSource._from_proto(msg.source)
        created_at, created_by_rid = _attribution(msg.created)
        # Construct the concrete class explicitly rather than via `cls`, so that refreshing an
        # instance can never resurrect it as the wrong one.
        drive_cls = Drive if source is DriveSource.NOMINAL else VirtualDrive
        return drive_cls(
            rid=msg.rid,
            id=msg.id if msg.HasField("id") else None,
            subordinate_to_rid=msg.subordinate_to_rid if msg.HasField("subordinate_to_rid") else None,
            description=msg.description if msg.HasField("description") else None,
            workspace_rid=msg.workspace_rid,
            state=DriveState._from_proto(msg.state),
            source=source,
            content_mutability=DriveMutability._from_proto(msg.content_mutability),
            created_at=created_at,
            created_by_rid=created_by_rid,
            _clients=clients,
        )


@dataclass(frozen=True)
class VirtualDrive(Drive):
    """A read-only drive whose files are mirrored from an external provider.

    Reads work exactly as they do on a managed drive. Writes raise `NominalFileStoreError`
    before any request is sent.
    """

    def status(self) -> VirtualDriveStatus:
        """Probe the backing provider and report its current connectivity.

        Returns:
            The provider's state, a human-readable message, and when it was last reached.
        """
        with translate_grpc_errors():
            response = self._clients.drives.GetVirtualDriveStatus(
                drives_pb2.GetVirtualDriveStatusRequest(drive_rid=self.rid)
            )
        return VirtualDriveStatus._from_proto(response.status)


def create_drive(
    client: NominalClient, id: str, *, description: str | None = None, workspace_rid: str | None = None
) -> Drive:
    """Create a managed drive.

    Args:
        client: Client providing authentication and workspace configuration.
        id: Identifier for the drive, unique within the workspace. Up to 100 letters,
            digits, ``.``, ``-``, and ``_``, starting with a letter or digit; ids starting
            with ``nominal.`` in any casing are reserved.
        description: Free-text description of the drive.
        workspace_rid: Workspace to create the drive in. Defaults to the client's workspace.

    Returns:
        The created drive.

    Raises:
        NominalAlreadyExistsError: A drive with this id already exists in the workspace.
    """
    clients = client._clients
    workspace = clients.resolve_workspace(workspace_rid).rid
    request = drives_pb2.CreateDriveRequest(workspace_rid=workspace, id=id, description=description)
    with translate_grpc_errors():
        response = clients.drives.CreateDrive(request)
    return Drive._from_proto(clients, response.drive)


def get_drive(client: NominalClient, rid: str) -> Drive:
    """Retrieve a drive by RID.

    Args:
        client: Client providing authentication and workspace configuration.
        rid: RID of the drive to retrieve.

    Returns:
        The drive. This is a `VirtualDrive` when the drive mirrors an external provider.
    """
    clients = client._clients
    with translate_grpc_errors():
        response = clients.drives.GetDrive(drives_pb2.GetDriveRequest(drive_rid=rid))
    return Drive._from_proto(clients, response.drive)


def get_drive_by_id(client: NominalClient, id: str, *, workspace_rid: str | None = None) -> Drive:
    """Retrieve a drive by its workspace-unique id.

    Args:
        client: Client providing authentication and workspace configuration.
        id: Identifier of the drive to retrieve.
        workspace_rid: Workspace to look in. Defaults to the client's workspace.

    Returns:
        The drive. This is a `VirtualDrive` when the drive mirrors an external provider.
    """
    clients = client._clients
    workspace = clients.resolve_workspace(workspace_rid).rid
    request = drives_pb2.GetDriveByIdRequest(workspace_rid=workspace, id=id)
    with translate_grpc_errors():
        response = clients.drives.GetDriveById(request)
    return Drive._from_proto(clients, response.drive)


def list_drives(
    client: NominalClient, *, include_archived: bool = False, workspace_rid: str | None = None
) -> Sequence[Drive]:
    """List the drives in a workspace.

    Args:
        client: Client providing authentication and workspace configuration.
        include_archived: Include archived drives in the results.
        workspace_rid: Workspace to list. Defaults to the client's workspace.

    Returns:
        All drives in the workspace, with every page collected.
    """
    clients = client._clients
    workspace = clients.resolve_workspace(workspace_rid).rid
    return [
        Drive._from_proto(clients, msg)
        for msg in list_drives_paginated(clients.drives, workspace, include_archived=include_archived)
    ]
