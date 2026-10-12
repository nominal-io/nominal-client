"""Files and revisions within a drive (nominal.file_store.v1).

Listing a drive yields `DriveEntry` values — a `DriveDirectory`, or a `DriveFile`. Files
come in two concrete kinds, because the backend models them differently: a
`ManagedDriveFile` in a Nominal-backed drive has a stable RID and a linear revision
history, while a `VirtualDriveFile` mirrored from an external provider is identified by a
provider-specific tuple and pinned by content rather than by RID.
"""

from __future__ import annotations

import abc
import pathlib
from dataclasses import dataclass, field
from typing import Sequence, TypeAlias

from typing_extensions import Self

from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.multipart_downloader import DownloadItem, MultipartFileDownloader, PresignedURLProvider
from nominal.core._utils.pagination_tools import list_file_revisions_paginated
from nominal.core.fs._clients import _Clients
from nominal.core.fs.enums import DriveFileState, DriveSource
from nominal.core.fs.errors import FileStoreErrorCode, NominalFileStoreError
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from nominal.ts import IntegralNanosecondsUTC


def _attribution(msg: file_store_pb2.Attribution) -> tuple[IntegralNanosecondsUTC | None, str | None]:
    """Split an attribution into (time, user rid), tolerating either being unset."""
    return (
        msg.time.ToNanoseconds() if msg.HasField("time") else None,
        msg.user_rid or None,
    )


def _basename(path: str) -> str:
    """Last segment of a drive-relative path, e.g. the filename portion of `data/run.csv`."""
    return path.rsplit("/", 1)[-1]


def _download_destination(output_directory: PathLike, drive_path: str) -> pathlib.Path:
    """Where downloading `drive_path` into `output_directory` writes, checked before any request."""
    directory = pathlib.Path(output_directory)
    filename = _basename(drive_path)
    destination = directory / filename
    # The filename is the last segment of a server-supplied path, and joined onto the directory it
    # must stay a single file inside it. The backend rejects `.`/`..` segments, but a legal segment
    # can still carry a backslash (`..\..\evil.csv`), which Windows treats as a separator, or a
    # drive (`D:evil.csv`), which replaces the directory outright. Backslashes are refused on every
    # platform so that a drive path downloads to the same name everywhere.
    if "\\" in filename or destination.parent != directory or destination.name != filename:
        raise ValueError(f"refusing to download to {filename!r}: it would not be a file in {directory}")
    if not directory.is_dir():
        if directory.exists():
            raise NotADirectoryError(f"Output directory is not a directory: {directory}")
        raise FileNotFoundError(f"Output directory does not exist: {directory}")
    if destination.exists():
        raise FileExistsError(f"Destination already exists: {destination}")
    return destination


def _download_revision(
    clients: _Clients, revision_rid: str, destination: pathlib.Path, *, part_size: int, num_retries: int
) -> pathlib.Path:
    """Download a revision's bytes to `destination`.

    The presigned URL the backend issues is short-lived, so it is fetched through a provider
    that refreshes it rather than being resolved once up front.
    """

    def fetch() -> str:
        with translate_grpc_errors():
            return clients.drive_files.GetDownloadUrl(
                files_pb2.GetDownloadUrlRequest(file_revision_rid=revision_rid)
            ).url

    item = DownloadItem(
        provider=PresignedURLProvider(fetch_fn=fetch, ttl_secs=60.0, skew_secs=20.0),
        destination=destination,
        part_size=part_size,
    )
    with MultipartFileDownloader.create(
        max_part_retries=num_retries, header_provider=clients.header_provider
    ) as downloader:
        return downloader.download_file(item)


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

    A revision records either a content change (``state`` is `DriveFileState.ACTIVE`) or a
    removal (``state`` is `DriveFileState.REMOVED`), which is how a removal is distinguished
    from a new version.
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
        # `ref` and `identity` replace the plain rid fields, which servers predating them send instead.
        return cls(
            rid=msg.ref.managed.file_revision_rid if msg.HasField("ref") else msg.file_revision_rid,
            file_rid=msg.identity.managed.file_rid if msg.HasField("identity") else msg.file_rid,
            path=msg.path.path,
            size_bytes=msg.size_bytes,
            state=DriveFileState._from_proto(msg.state),
            created_at=created_at,
            created_by_rid=created_by_rid,
            _drive_rid=drive_rid,
            _clients=clients,
        )

    def restore(self, destination: FileDestination) -> ManagedDriveFile:
        """Reinstate this revision's content at a destination.

        Args:
            destination: Where to place the restored content. If the owning file is still
                active, this must replace that file; if it was removed, a free path works.

        Returns:
            The file as it now stands.

        Raises:
            ValueError: ``destination`` is a file or revision from a different drive.
            NominalFileStoreError: The restore was rejected — for example the destination
                path is occupied, or the file it targets has moved on.
        """
        change = files_pb2.FileChange(
            restore=files_pb2.RestoreFile(
                restore_revision_rid=self.rid,
                destination=_destination_to_proto(destination, self._drive_rid),
            )
        )
        success = _apply_one(self._clients, self._drive_rid, change)
        return ManagedDriveFile._from_proto(self._clients, self._drive_rid, success.file)

    def download(
        self, output_directory: PathLike, *, part_size: int = DEFAULT_CHUNK_SIZE, num_retries: int = 3
    ) -> pathlib.Path:
        """Download this revision's content into a directory.

        Args:
            output_directory: Directory to write into. The file is named after this
                revision's path.
            part_size: Size in bytes of each ranged download request.
            num_retries: Retries per part.

        Returns:
            Path the file was written to.

        Raises:
            ValueError: The revision's filename would not stay inside ``output_directory``,
                for example because it contains a backslash.
            NotADirectoryError: ``output_directory`` exists but is not a directory.
            FileNotFoundError: ``output_directory`` does not exist.
            FileExistsError: A file already exists at the destination.
            NominalNotFoundError: This revision's bytes are no longer retained.
        """
        destination = _download_destination(output_directory, self.path)
        return _download_revision(self._clients, self.rid, destination, part_size=part_size, num_retries=num_retries)


@dataclass(frozen=True)
class DriveFile(DriveEntry, abc.ABC):
    """A file in a drive, managed or mirrored from an external provider.

    Both kinds share these fields and can be downloaded. Only a `ManagedDriveFile` has a
    revision history and can be moved or removed; narrow with ``isinstance`` to reach those.
    """

    size_bytes: int
    state: DriveFileState
    observed_at: IntegralNanosecondsUTC | None
    """When the backend last observed this file."""
    _clients: _Clients = field(repr=False)

    @abc.abstractmethod
    def _content_revision_rid(self) -> str:
        """RID of the revision holding the content this file currently shows."""

    def download(
        self, output_directory: PathLike, *, part_size: int = DEFAULT_CHUNK_SIZE, num_retries: int = 3
    ) -> pathlib.Path:
        """Download this file's current content into a directory.

        A virtual file is resolved first (see `VirtualDriveFile.resolve`), and the revision
        that pins is what downloads; Nominal may fetch it from the provider as it streams.

        Args:
            output_directory: Directory to write into. The file is named after its path in
                the drive.
            part_size: Size in bytes of each ranged download request.
            num_retries: Retries per part.

        Returns:
            Path the file was written to.

        Raises:
            ValueError: The file's name would not stay inside ``output_directory``, for
                example because it contains a backslash.
            NotADirectoryError: ``output_directory`` exists but is not a directory.
            FileNotFoundError: ``output_directory`` does not exist.
            FileExistsError: A file already exists at the destination.
            NominalFileStoreError: This managed file has no current revision.
            NominalNotFoundError: This file's bytes are no longer retained.
            requests.HTTPError: A virtual file's provider can no longer serve the observed
                content, or can't be reached.
        """
        destination = _download_destination(output_directory, self.path)
        return _download_revision(
            self._clients, self._content_revision_rid(), destination, part_size=part_size, num_retries=num_retries
        )


@dataclass(frozen=True)
class ManagedDriveFile(DriveFile, HasRid, RefreshableMixin[file_store_pb2.LogicalFile]):
    """A file stored in a Nominal-backed drive, with a stable RID and a revision history."""

    rid: str
    created_at: IntegralNanosecondsUTC | None
    created_by_rid: str | None
    current_revision_rid: str | None
    """RID of the revision currently at this path, if the file has one."""
    _drive_rid: str = field(repr=False)

    @classmethod
    def _from_proto(cls, clients: _Clients, drive_rid: str, msg: file_store_pb2.LogicalFile) -> Self:
        observed_at, _ = _attribution(msg.observed)
        created_at, created_by_rid = _attribution(msg.created)
        current_revision = msg.current_revision
        return cls(
            path=msg.path.path,
            size_bytes=msg.size_bytes,
            state=DriveFileState._from_proto(msg.state),
            observed_at=observed_at,
            _clients=clients,
            rid=msg.identity.managed.file_rid,
            created_at=created_at,
            created_by_rid=created_by_rid,
            current_revision_rid=(
                current_revision.managed.file_revision_rid
                if current_revision.WhichOneof("reference") == "managed"
                else None
            ),
            _drive_rid=drive_rid,
        )

    def _get_latest_api(self) -> file_store_pb2.LogicalFile:
        request = files_pb2.GetFileByIdentityRequest(
            identity=file_store_pb2.LogicalFileIdentity(managed=file_store_pb2.ManagedFileIdentity(file_rid=self.rid))
        )
        with translate_grpc_errors():
            return self._clients.drive_files.GetFileByIdentity(request).file

    def _refresh_to_self(self, api_obj: file_store_pb2.LogicalFile) -> Self:
        # LogicalFile does not echo its drive, so the drive is re-supplied here; a managed file
        # cannot move between drives, so that is stable.
        return self._from_proto(self._clients, self._drive_rid, api_obj)

    def _require_current_revision(self) -> str:
        if self.current_revision_rid is None:
            raise NominalFileStoreError(
                FileStoreErrorCode.FILE_REVISION_NOT_FOUND,
                f"{self.path!r} has no current revision to act on",
            )
        return self.current_revision_rid

    def _content_revision_rid(self) -> str:
        return self._require_current_revision()

    def revisions(self) -> Sequence[DriveFileRevision]:
        """List this file's revision history, in the order the backend returns it.

        Returns:
            Every revision of this file, including removal markers.
        """
        return [
            DriveFileRevision._from_proto(self._clients, self._drive_rid, msg)
            for msg in list_file_revisions_paginated(self._clients.drive_files, self._drive_rid, self.rid)
        ]

    def move_to(self, destination: FileDestination) -> Self:
        """Move this file, refreshing it in place.

        Args:
            destination: Where to move the file. A path must be unoccupied; pass a file or
                revision to replace what is already there.

        Returns:
            This instance, updated.

        Raises:
            ValueError: ``destination`` is a file or revision from a different drive.
            NominalFileStoreError: The move was rejected — for example the path is occupied,
                or this file's revision is no longer current.
        """
        change = files_pb2.FileChange(
            move=files_pb2.MoveFile(
                source_revision_rid=self._require_current_revision(),
                destination=_destination_to_proto(destination, self._drive_rid),
            )
        )
        return self._refresh_from_api(_apply_one(self._clients, self._drive_rid, change).file)

    def remove(self) -> Self:
        """Remove this file, refreshing it in place.

        Removal is soft: the file's revisions remain, and any of them can be restored.

        Returns:
            This instance, updated.

        Raises:
            NominalFileStoreError: The removal was rejected — for example this file's revision
                is no longer current.
        """
        change = files_pb2.FileChange(remove=files_pb2.RemoveFile(revision_rid=self._require_current_revision()))
        return self._refresh_from_api(_apply_one(self._clients, self._drive_rid, change).file)


@dataclass(frozen=True)
class VirtualDriveFile(DriveFile):
    """A file mirrored from an external provider, pinned by content rather than by RID.

    It has no revision history and cannot be moved or removed through Nominal.
    """

    provider: DriveSource
    """Which external provider backs this file."""
    # Excluded from comparison because generated messages are unhashable; the public fields
    # already describe the observed content.
    _revision_ref: file_store_pb2.FileRevisionRef = field(repr=False, compare=False)

    @classmethod
    def _from_proto(cls, clients: _Clients, msg: file_store_pb2.LogicalFile) -> Self:
        observed_at, _ = _attribution(msg.observed)
        return cls(
            path=msg.path.path,
            size_bytes=msg.size_bytes,
            state=DriveFileState._from_proto(msg.state),
            observed_at=observed_at,
            _clients=clients,
            provider=_VIRTUAL_PROVIDERS.get(msg.identity.virtual.WhichOneof("kind") or "", DriveSource.UNKNOWN),
            _revision_ref=msg.current_revision,
        )

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

    def _content_revision_rid(self) -> str:
        return self.resolve()


_VIRTUAL_PROVIDERS = {
    "s3": DriveSource.S3,
    "google_drive": DriveSource.GOOGLE_DRIVE,
    "gcs": DriveSource.GCS,
}


def _file_from_proto(clients: _Clients, drive_rid: str, msg: file_store_pb2.LogicalFile) -> DriveFile:
    if msg.identity.WhichOneof("identity") == "managed":
        return ManagedDriveFile._from_proto(clients, drive_rid, msg)
    return VirtualDriveFile._from_proto(clients, msg)


def _entry_from_proto(clients: _Clients, drive_rid: str, msg: file_store_pb2.FileEntry) -> DriveEntry:
    if msg.WhichOneof("entry") == "directory":
        return DriveDirectory(path=msg.directory.path.path)
    return _file_from_proto(clients, drive_rid, msg.file)


FileDestination: TypeAlias = str | ManagedDriveFile | DriveFileRevision
"""Where a put, move, or restore places content.

- a ``str`` is a drive-relative path, and nothing may be there already;
- a `ManagedDriveFile` is that file, which must still be at its current revision;
- a `DriveFileRevision` is the file at exactly that revision.

A put adds a revision to a file destination; a move or restore onto one replaces it.
"""


def _destination_to_proto(destination: FileDestination, drive_rid: str) -> files_pb2.Destination:
    if isinstance(destination, str):
        return files_pb2.Destination(path=files_pb2.PathTarget(path=file_store_pb2.LogicalPath(path=destination)))
    if destination._drive_rid != drive_rid:
        raise ValueError(f"destination {destination.path!r} belongs to a different drive ({destination._drive_rid})")
    if isinstance(destination, DriveFileRevision):
        return files_pb2.Destination(file_revision_rid=destination.rid)
    return files_pb2.Destination(file_revision_rid=destination._require_current_revision())


def _apply_one(clients: _Clients, drive_rid: str, change: files_pb2.FileChange) -> files_pb2.FileChangeSuccess:
    """Apply a single change, raising the backend's in-band failure as an exception.

    A failure is reported in the response rather than as a gRPC error, so it is decoded
    here. A stale-revision failure is surfaced rather than retried: it means the caller's
    view of the file is out of date, which they need to see.
    """
    request = files_pb2.ApplyFileChangesRequest(drive_rid=drive_rid, changes=[change])
    with translate_grpc_errors():
        (result,) = clients.drive_files.ApplyFileChanges(request).results
    if result.WhichOneof("result") == "failure":
        raise NominalFileStoreError(FileStoreErrorCode._from_failure(result.failure), result.failure.message)
    return result.success
