"""Files and revisions within a File Store drive (nominal.file_store.v1).

Listing a drive yields `DriveEntry` values — a `DriveDirectory`, or a `DriveFile`. Files
come in two concrete kinds, because the backend models them differently: a
`ManagedDriveFile` in a Nominal-backed drive has a stable RID and a linear revision
history, while a `VirtualDriveFile` mirrored from an external provider is identified by a
provider-specific tuple and pinned by content rather than by RID.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Sequence, cast

from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import list_file_revisions_paginated
from nominal.core.exceptions import FileStoreErrorCode, NominalFileStoreError
from nominal.core.file_store._clients import _Clients
from nominal.core.file_store.enums import DriveFileState, DriveSource
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from nominal.ts import IntegralNanosecondsUTC


def _attribution(msg: file_store_pb2.Attribution) -> tuple[IntegralNanosecondsUTC | None, str | None]:
    return (
        msg.time.ToNanoseconds() if msg.HasField("time") else None,
        msg.user_rid or None,
    )


@dataclass(frozen=True)
class DriveEntry:
    """Anything a drive listing can yield: a file or a directory."""

    path: str
    """Drive-relative path, with no leading or trailing slash."""


@dataclass(frozen=True)
class DriveDirectory(DriveEntry):
    """A directory in a drive listing.

    Directories are implied by the paths of the files beneath them; they carry no metadata
    of their own.
    """


@dataclass(frozen=True)
class DriveFileRevision(HasRid):
    """One entry in a managed file's linear history.

    A revision records either a content change (`state` is `ACTIVE`) or a removal
    (`state` is `REMOVED`), which is how a removal is distinguished from a new version.
    """

    rid: str
    file_rid: str
    path: str
    """Path at which this revision became current."""
    size_bytes: int
    state: DriveFileState
    created_at: IntegralNanosecondsUTC | None
    created_by_rid: str | None
    _drive_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    @classmethod
    def _from_proto(cls, clients: _Clients, drive_rid: str, msg: file_store_pb2.ManagedFileRevision) -> Self:
        created_at, created_by_rid = _attribution(msg.created)
        return cls(
            rid=msg.file_revision_rid,
            file_rid=msg.file_rid,
            path=msg.path.path,
            size_bytes=msg.size_bytes,
            state=DriveFileState._from_proto(msg.state),
            created_at=created_at,
            created_by_rid=created_by_rid,
            _drive_rid=drive_rid,
            _clients=clients,
        )


@dataclass(frozen=True)
class DriveFile(DriveEntry, abc.ABC):
    """A file in a drive, managed or mirrored from an external provider.

    Operations a drive cannot support raise `NominalFileStoreError` before any request is
    sent — a virtual file has no history to list and cannot be modified.
    """

    size_bytes: int
    state: DriveFileState
    observed_at: IntegralNanosecondsUTC | None
    """When the backend last observed this file."""
    _drive_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    @abc.abstractmethod
    def revisions(self) -> Sequence[DriveFileRevision]:
        """List this file's revision history, in the order the backend returns it.

        Returns:
            Every revision of this file.

        Raises:
            NominalFileStoreError: This file is in a virtual drive, which has no history.
        """


@dataclass(frozen=True)
class ManagedDriveFile(DriveFile, HasRid, RefreshableMixin[file_store_pb2.LogicalFile]):
    """A file stored in a Nominal-backed drive, with a stable RID and a revision history."""

    rid: str
    created_at: IntegralNanosecondsUTC | None
    created_by_rid: str | None
    current_revision_rid: str | None
    """RID of the revision currently at this path, if the file has one."""

    def _get_latest_api(self) -> file_store_pb2.LogicalFile:
        request = files_pb2.GetFileByIdentityRequest(
            identity=file_store_pb2.LogicalFileIdentity(managed=file_store_pb2.ManagedFileIdentity(file_rid=self.rid))
        )
        with translate_grpc_errors():
            return self._clients.drive_files.GetFileByIdentity(request).file

    def _refresh_to_self(self, api_obj: file_store_pb2.LogicalFile) -> Self:
        # LogicalFile does not echo its drive, so the drive is re-supplied here; a managed file
        # cannot move between drives, so that is stable. The cast is needed for the same reason as
        # in `Drive`: the factory picks a concrete class the type system cannot tie back to `Self`.
        refreshed = _file_from_proto(self._clients, self._drive_rid, api_obj)
        if not isinstance(refreshed, ManagedDriveFile):
            raise NominalFileStoreError(
                FileStoreErrorCode.UNKNOWN,
                f"File {self.rid!r} came back from the server as a different kind of file",
            )
        return cast("Self", refreshed)

    def revisions(self) -> Sequence[DriveFileRevision]:
        """List this file's revision history, in the order the backend returns it.

        Returns:
            Every revision of this file, including removal markers.
        """
        return [
            DriveFileRevision._from_proto(self._clients, self._drive_rid, msg)
            for msg in list_file_revisions_paginated(self._clients.drive_files, self._drive_rid, self.rid)
        ]


@dataclass(frozen=True)
class VirtualDriveFile(DriveFile):
    """A file mirrored from an external provider, pinned by content rather than by RID."""

    provider: DriveSource
    """Which external provider backs this file."""
    _identity: file_store_pb2.LogicalFileIdentity = field(repr=False)
    _revision_ref: file_store_pb2.FileRevisionRef = field(repr=False)

    def resolve(self) -> str:
        """Pin this file's currently-observed content and return its revision RID.

        Resolving is durable: the same observed content always resolves to the same RID,
        even after the file later changes upstream.

        Returns:
            RID of the pinned revision.
        """
        request = files_pb2.ResolveFileRevisionRequest(source_ref=self._revision_ref)
        with translate_grpc_errors():
            return self._clients.drive_files.ResolveFileRevision(request).file_revision_rid

    def revisions(self) -> Sequence[DriveFileRevision]:
        """Not supported: files mirrored from an external provider have no history.

        Raises:
            NominalFileStoreError: Always, with code `FILE_HISTORY_NOT_AVAILABLE`.
        """
        raise NominalFileStoreError(
            FileStoreErrorCode.FILE_HISTORY_NOT_AVAILABLE,
            f"{self.path!r} is in a drive backed by {self.provider.value}, which does not keep file history",
        )


_VIRTUAL_PROVIDERS = {
    "s3": DriveSource.S3,
    "google_drive": DriveSource.GOOGLE_DRIVE,
    "gcs": DriveSource.GCS,
}


def _file_from_proto(clients: _Clients, drive_rid: str, msg: file_store_pb2.LogicalFile) -> DriveFile:
    observed_at, _ = _attribution(msg.observed)
    path = msg.path.path
    size_bytes = msg.size_bytes
    state = DriveFileState._from_proto(msg.state)
    if msg.identity.WhichOneof("identity") == "managed":
        created_at, created_by_rid = _attribution(msg.created)
        current_revision = msg.current_revision
        return ManagedDriveFile(
            path=path,
            size_bytes=size_bytes,
            state=state,
            observed_at=observed_at,
            _drive_rid=drive_rid,
            _clients=clients,
            rid=msg.identity.managed.file_rid,
            created_at=created_at,
            created_by_rid=created_by_rid,
            current_revision_rid=(
                current_revision.managed.file_revision_rid
                if current_revision.WhichOneof("reference") == "managed"
                else None
            ),
        )
    provider_kind = msg.identity.virtual.WhichOneof("kind") or ""
    return VirtualDriveFile(
        path=path,
        size_bytes=size_bytes,
        state=state,
        observed_at=observed_at,
        _drive_rid=drive_rid,
        _clients=clients,
        provider=_VIRTUAL_PROVIDERS.get(provider_kind, DriveSource.UNKNOWN),
        _identity=msg.identity,
        _revision_ref=msg.current_revision,
    )


def _entry_from_proto(clients: _Clients, drive_rid: str, msg: file_store_pb2.FileEntry) -> DriveEntry:
    if msg.WhichOneof("entry") == "directory":
        return DriveDirectory(path=msg.directory.path.path)
    return _file_from_proto(clients, drive_rid, msg.file)
