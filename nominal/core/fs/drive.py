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
from nominal.core._utils.filenames import validate_upload_filename
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE, DEFAULT_NUM_WORKERS, _put_multipart_upload_to
from nominal.core._utils.pagination_tools import list_drives_paginated, list_files_paginated
from nominal.core.filetype import FileType, FileTypes
from nominal.core.fs._clients import _attribution, _basename, _Clients
from nominal.core.fs.enums import DriveMutability, DriveSource, DriveState, VirtualDriveState
from nominal.core.fs.errors import FileStoreErrorCode, NominalFileStoreError
from nominal.core.fs.file import (
    DriveEntry,
    DriveFile,
    ManagedDriveFile,
    _apply_one,
    _destination_to_proto,
    _entry_from_proto,
    _file_from_proto,
    _managed_file_from_proto,
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
    id: str
    """Human-readable identifier, unique within the workspace."""
    workspace_rid: str
    state: DriveState
    source: DriveSource
    """Where the drive's files come from."""
    content_mutability: DriveMutability
    """Whether the drive's contents can be modified through Nominal."""
    created_at: IntegralNanosecondsUTC | None
    created_by_rid: str | None
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

    def rename(self, new_id: str) -> Self:
        """Change this drive's id, refreshing it in place.

        Args:
            new_id: New identifier for the drive. Must be unique within the workspace, and
                may contain only lowercase letters, digits, `-`, and `_`.

        Returns:
            This instance, updated.

        Note:
            Renaming a drive requires organization-admin permissions.
        """
        request = drives_pb2.UpdateDriveDetailsRequest(drive_rid=self.rid, id=new_id)
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
            path: Drive-relative path, with no leading or trailing slash and no `.` or
                `..` segments.
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

    def list_files(self, parent_path: str = "", *, include_removed: bool = False) -> Sequence[DriveEntry]:
        """List the immediate children of a path in this drive.

        Args:
            parent_path: Directory to list. The default lists the drive root. Listing is
                not recursive.
            include_removed: Include files that have been removed.

        Returns:
            The directories and files directly beneath `parent_path`, in the order the
            backend returns them, with every page collected.
        """
        return [
            _entry_from_proto(self._clients, self.rid, msg)
            for msg in list_files_paginated(
                self._clients.drive_files, self.rid, parent_path, include_removed=include_removed
            )
        ]

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
        destination_path: str,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> ManagedDriveFile:
        """Upload a local file into this drive.

        Args:
            local_path: File on disk to upload.
            destination_path: Drive-relative path to create. Nothing may exist there
                already.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of threads uploading parts concurrently.

        Returns:
            The file as it now exists in the drive.

        Raises:
            FileNotFoundError: `local_path` does not exist.
            IsADirectoryError: `local_path` is a directory.
            ValueError: `local_path` is empty, or `destination_path` has no filename (for
                example it ends in `/`) or a filename with characters storage rejects.
            NominalFileStoreError: This drive is read-only, or a file already exists at
                `destination_path`.
        """
        path = pathlib.Path(local_path)
        if path.is_dir():
            raise IsADirectoryError(f"expected a file, got a directory: {path}")
        with path.open("rb") as f:
            return self.put_file_obj(f, destination_path, chunk_size=chunk_size, max_workers=max_workers)

    def put_file_obj(
        self,
        file_obj: BinaryIO,
        destination_path: str,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_NUM_WORKERS,
    ) -> ManagedDriveFile:
        """Upload the contents of a binary file-like object into this drive.

        The object is read from its current position to its end. It must be seekable, because
        the drive records the file's size, which is measured before anything is uploaded.

        Args:
            file_obj: Seekable binary file-like object, e.g. `open(path, "rb")` or `io.BytesIO`.
            destination_path: Drive-relative path to create. Nothing may exist there
                already.
            chunk_size: Size in bytes of each upload part.
            max_workers: Number of threads uploading parts concurrently.

        Returns:
            The file as it now exists in the drive.

        Raises:
            TypeError: `file_obj` is open in text mode.
            ValueError: `file_obj` is not seekable or has nothing left to read, or
                `destination_path` has no filename (for example it ends in `/`) or a filename
                with characters storage rejects.
            NominalFileStoreError: This drive is read-only, or a file already exists at
                `destination_path`.
        """
        self._require_writable()
        if isinstance(file_obj, io.TextIOBase):
            raise TypeError(f"{file_obj!r} must be open in binary mode, rather than text mode")
        if not file_obj.seekable():
            raise ValueError(f"{file_obj!r} must be seekable so its size can be measured before uploading")
        start = file_obj.tell()
        size_bytes = file_obj.seek(0, io.SEEK_END) - start
        file_obj.seek(start)
        if size_bytes <= 0:
            raise ValueError(f"cannot upload an empty file to {destination_path!r}")

        # The drive path decides the file's identity and its stored suffix, so the upload is
        # named, and typed, after the destination rather than wherever the bytes came from.
        filename = _basename(destination_path)
        if not filename:
            raise ValueError(f"destination path {destination_path!r} has no filename")
        validate_upload_filename(filename)
        mimetype = FileType.from_path(filename, default_mimetype=FileTypes.BINARY.mimetype).mimetype
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

        change = files_pb2.FileChange(
            put=files_pb2.PutFile(
                object=files_pb2.UploadedObjectRef(object_key=uploaded.key),
                size_bytes=size_bytes,
                destination=_destination_to_proto(destination_path, self.rid),
            )
        )
        return _managed_file_from_proto(self._clients, self.rid, _apply_one(self._clients, self.rid, change).file)

    @classmethod
    def _from_proto(cls, clients: _Clients, msg: file_store_pb2.Drive) -> Drive:
        source = DriveSource._from_proto(msg.source)
        created_at, created_by_rid = _attribution(msg.created)
        # Construct the concrete class explicitly rather than via `cls`, so that refreshing an
        # instance can never resurrect it as the wrong one.
        drive_cls = Drive if source is DriveSource.NOMINAL else VirtualDrive
        return drive_cls(
            rid=msg.rid,
            id=msg.id,
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


def _create_drive(clients: _Clients, id: str, *, workspace_rid: str | None = None) -> Drive:
    workspace = clients.resolve_workspace(workspace_rid).rid
    request = drives_pb2.CreateDriveRequest(workspace_rid=workspace, id=id)
    with translate_grpc_errors():
        response = clients.drives.CreateDrive(request)
    return Drive._from_proto(clients, response.drive)


def _get_drive(clients: _Clients, rid: str) -> Drive:
    with translate_grpc_errors():
        response = clients.drives.GetDrive(drives_pb2.GetDriveRequest(drive_rid=rid))
    return Drive._from_proto(clients, response.drive)


def _get_drive_by_id(clients: _Clients, id: str, *, workspace_rid: str | None = None) -> Drive:
    workspace = clients.resolve_workspace(workspace_rid).rid
    request = drives_pb2.GetDriveByIdRequest(workspace_rid=workspace, id=id)
    with translate_grpc_errors():
        response = clients.drives.GetDriveById(request)
    return Drive._from_proto(clients, response.drive)


def _list_drives(
    clients: _Clients, *, include_archived: bool = False, workspace_rid: str | None = None
) -> Sequence[Drive]:
    workspace = clients.resolve_workspace(workspace_rid).rid
    return [
        Drive._from_proto(clients, msg)
        for msg in list_drives_paginated(clients.drives, workspace, include_archived=include_archived)
    ]
