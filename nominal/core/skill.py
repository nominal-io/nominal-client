from __future__ import annotations

import re
import zipfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Protocol, Self

import grpc
import yaml
from google.protobuf import field_mask_pb2
from nominal_api import upload_api

from nominal._utils.iterator_tools import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import upload_file_store_object
from nominal.core._utils.networking import create_multipart_request_session
from nominal.core._utils.pagination_tools import paginate_grpc, search_skills_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalAlreadyExistsError, NominalConflictError, NominalError, NominalNotFoundError
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2, files_pb2_grpc
from nominal.protos.skills.v1 import skills_pb2, skills_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

_ENTRY_FILE = "SKILL.md"
# The app and Analyst save each skill at skills/<name>/SKILL.md in the workspace's skills drive.
_SKILLS_ROOT = "skills"
_MAX_NAME_LENGTH = 64
# Lowercase letters and numbers in groups joined by single hyphens.
_SKILL_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
# A YAML frontmatter block at the very start of SKILL.md, with LF or CRLF line endings.
_FRONTMATTER = re.compile(r"---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|$)", re.DOTALL)
_TEXT_MIMETYPE = "text/plain; charset=utf-8"
# ApplyFileChanges takes at most this many changes per request.
_MAX_FILE_CHANGES = 1000
# Seconds a file download may wait to connect or between reads.
_DOWNLOAD_TIMEOUT = 30.0


class SkillLoadStatus(Enum):
    """Whether Analyst and connected agents can load a skill."""

    UNSPECIFIED = "UNSPECIFIED"
    """Not reported, or a status from a newer server."""
    AVAILABLE = "AVAILABLE"
    """SKILL.md is readable and valid."""
    INVALID = "INVALID"
    """SKILL.md is readable but is not a valid skill, for example because its frontmatter has no description."""
    MISSING = "MISSING"
    """SKILL.md no longer exists, or is no longer where the skill expects it."""
    UNAVAILABLE = "UNAVAILABLE"
    """SKILL.md could not be read for another reason, such as an unavailable source."""

    @classmethod
    def _from_proto(cls, value: skills_pb2.SkillBackingStatus.ValueType) -> SkillLoadStatus:
        match value:
            case skills_pb2.SKILL_BACKING_STATUS_AVAILABLE:
                result = cls.AVAILABLE
            case skills_pb2.SKILL_BACKING_STATUS_INVALID:
                result = cls.INVALID
            case skills_pb2.SKILL_BACKING_STATUS_MISSING:
                result = cls.MISSING
            case skills_pb2.SKILL_BACKING_STATUS_UNAVAILABLE:
                result = cls.UNAVAILABLE
            case _:
                # Unset, or a value a newer server sent that this SDK doesn't know.
                result = cls.UNSPECIFIED
        return result


@dataclass(frozen=True)
class SkillFileInfo:
    """A file in a skill's folder: its path relative to the folder, and its size."""

    path: str
    size_bytes: int


@dataclass(frozen=True)
class Skill(HasRid, RefreshableGrpcMixin[skills_pb2.Skill]):
    """A skill in a workspace's skills library: a SKILL.md, and any reference files, that Analyst and connected
    agents follow. Its files are in the folder ``skills/<name>`` of the workspace's skills drive.

    `name` and `description` are what the library last recorded from SKILL.md's frontmatter, and are None when it had
    no valid frontmatter. This client records them each time it saves SKILL.md.
    """

    rid: str
    workspace_rid: str
    name: str | None
    description: str | None
    is_enabled: bool
    is_archived: bool
    version: int
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def files(self) -> files_pb2_grpc.FilesServiceStub: ...
        @property
        def skills(self) -> skills_pb2_grpc.SkillsServiceStub: ...
        @property
        def upload(self) -> upload_api.UploadService: ...

    def _get_latest_api(self) -> skills_pb2.Skill:
        request = skills_pb2.GetSkillRequest(skill_rid=self.rid)
        with translate_grpc_errors():
            return self._clients.skills.GetSkill(request).skill

    def get_load_status(self) -> SkillLoadStatus:
        """Read SKILL.md now and report whether Analyst and connected agents can load the skill."""
        request = skills_pb2.GetSkillBackingStatusRequest(skill_rid=self.rid)
        with translate_grpc_errors():
            response = self._clients.skills.GetSkillBackingStatus(request)
        return SkillLoadStatus._from_proto(response.status)

    def list_files(self) -> Sequence[SkillFileInfo]:
        """List every file in the skill's folder, SKILL.md included, by path.

        Raises:
            NominalError: If the skill's SKILL.md is not at ``skills/<name>/SKILL.md``, so it has no folder.
        """
        folder = _SkillFolder.of(self._get_latest_api())
        return [
            SkillFileInfo(path=path, size_bytes=file.size_bytes)
            for path, file in _list_folder(self._clients, folder).items()
        ]

    def read_file(self, path: str) -> str:
        """Read one file in the skill's folder, such as ``SKILL.md`` or ``methods/limits.md``.

        Raises:
            ValueError: If `path` leads outside the skill's folder.
            NominalNotFoundError: If the skill has no file at this path.
            NominalError: If the file is not UTF-8 text. The client reads and writes text files only.
            requests.RequestException: If downloading the file fails.
        """
        folder = _SkillFolder.of(self._get_latest_api())
        request = files_pb2.GetFileRequest(
            drive_rid=folder.drive_rid, path=file_store_pb2.LogicalPath(path=folder.path_of(_file_path(path)))
        )
        with translate_grpc_errors():
            file = self._clients.files.GetFile(request).file
        return _read_text(self._clients, file)

    def update(self, *, content: str | None = None, enabled: bool | None = None) -> Self:
        """Update the skill in place. Fields left as None are unchanged.

        Each change is saved only if the skill hasn't changed since this object was fetched or last refreshed.

        Args:
            content: The skill's new SKILL.md, frontmatter included, replacing the current one. A new name in its
                frontmatter renames the skill: its folder moves to ``skills/<new name>``, and it keeps its RID.
            enabled: Whether the skill is turned on. Turning it on needs a valid SKILL.md, and an archived skill must
                be restored with :meth:`unarchive` first.

        SKILL.md is saved first, then the folder is moved for a rename, then the skill records its new name and
        description. These are separate saves. If moving the folder fails, SKILL.md gets its previous content back.
        If a later step fails, the error's notes say what was saved.

        Raises:
            ValueError: If `content` has no frontmatter with a valid name. Nothing is saved.
            NominalConflictError: If the skill changed since this object was fetched or last refreshed, or SKILL.md
                changed while it was being saved.
            NominalAlreadyExistsError: If a skill already has the new name.
        """
        if content is None and enabled is None:
            return self.refresh()
        skill = skills_pb2.Skill(skill_rid=self.rid)
        paths = []
        if content is not None:
            skill.backing.file_store.entry_file_identity.CopyFrom(self._save_entry(content))
            paths.append("backing")
        if enabled is not None:
            skill.state = skills_pb2.SKILL_STATE_ENABLED if enabled else skills_pb2.SKILL_STATE_DISABLED
            paths.append("state")
        request = skills_pb2.UpdateSkillRequest(
            skill=skill, update_mask=field_mask_pb2.FieldMask(paths=paths), expected_version=self.version
        )
        try:
            return self._update(request)
        except NominalError as e:
            if content is not None:
                e.add_note(f"The new SKILL.md is saved, but skill {self.rid} hasn't recorded it.")
            raise

    def write_files(self, files: Mapping[str, str]) -> Self:
        """Add or replace reference files in the skill's folder, replacing each file as it is now.

        Each file is saved on its own, so when one fails the others may still be saved; the error names both.

        Args:
            files: UTF-8 text keyed by path relative to the skill's folder, such as ``methods/limits.md``. SKILL.md
                is not a reference file; change it with :meth:`update`.

        Raises:
            ValueError: If a path is SKILL.md or leads outside the skill's folder. Nothing is saved.
            NominalConflictError: If another save changed or created one of the files while it was being saved.
        """
        texts = {_reference_path(path): text for path, text in files.items()}
        folder = _SkillFolder.of(self._get_latest_api())
        current = _list_folder(self._clients, folder)
        changes = [
            _put(
                self._clients,
                self.workspace_rid,
                path,
                text,
                _replacing(_head_revision(self._clients, current[path]))
                if path in current
                else _at_path(folder.path_of(path)),
            )
            for path, text in texts.items()
        ]
        if (error := _change_error(list(texts), _apply(self._clients, folder.drive_rid, changes), "save")) is not None:
            raise error
        return self

    def remove_files(self, paths: Sequence[str]) -> Self:
        """Remove reference files from the skill's folder.

        Each file is removed on its own, so when one fails the others may still be removed; the error names both.

        Raises:
            ValueError: If a path is SKILL.md or leads outside the skill's folder. Nothing is removed.
            NominalNotFoundError: If the skill has no file at one of the paths. Nothing is removed.
            NominalConflictError: If another save changed one of the files while it was being removed.
        """
        targets = [_reference_path(path) for path in dict.fromkeys(paths)]
        folder = _SkillFolder.of(self._get_latest_api())
        current = _list_folder(self._clients, folder)
        missing = [path for path in targets if path not in current]
        if missing:
            raise NominalNotFoundError(f"Skill {self.rid} has no file at {', '.join(missing)}")
        changes = [
            files_pb2.FileChange(remove=files_pb2.RemoveFile(revision_rid=_head_revision(self._clients, current[path])))
            for path in targets
        ]
        if (error := _change_error(targets, _apply(self._clients, folder.drive_rid, changes), "remove")) is not None:
            raise error
        return self

    def archive(self) -> None:
        """Archive the skill, hiding it from the skills library, Analyst, and connected agents.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
        """
        self._update(_state_update(self.rid, skills_pb2.SKILL_STATE_ARCHIVED, self.version))

    def unarchive(self) -> None:
        """Restore an archived skill. It comes back turned off; turn it on with ``update(enabled=True)``.

        Refreshes this object first, and does nothing when the skill is not archived.
        """
        if self.refresh().is_archived:
            self._update(_state_update(self.rid, skills_pb2.SKILL_STATE_DISABLED, self.version))

    def export(self, path: PathLike) -> Path:
        """Write the skill's current folder, named after the skill, under `path`, and return the folder.

        Every file is read before any is written.

        Raises:
            FileExistsError: If `path` already has an entry with the skill's name.
            NominalError: If one of the skill's files is not UTF-8 text.
            requests.RequestException: If downloading a file fails.
        """
        folder = _SkillFolder.of(self._get_latest_api())
        target = Path(path) / folder.name
        if target.exists():
            raise FileExistsError(f"{target} already exists")
        # File Store paths have no "." or ".." parts, so every file lands inside the target folder.
        texts = {
            file_path: _read_text(self._clients, file)
            for file_path, file in _list_folder(self._clients, folder).items()
        }
        for file_path, text in texts.items():
            destination = target / file_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(text.encode("utf-8"))
        return target

    def _save_entry(self, content: str) -> file_store_pb2.LogicalFileIdentity:
        """Save SKILL.md, moving the skill's folder when its name changes, and return the identity to point it at."""
        name = _skill_name(content)
        latest = self._get_latest_api()
        if latest.version != self.version:
            raise NominalConflictError(f"Skill {self.rid} changed since this object was fetched; refresh it")
        folder = _SkillFolder.of(latest)
        target = _SkillFolder(drive_rid=folder.drive_rid, name=name)
        if name != folder.name:
            if latest.state == skills_pb2.SKILL_STATE_ARCHIVED:
                raise NominalError(f"Restore skill {self.rid} with unarchive() before renaming it")
            if not _is_empty(self._clients, target):
                raise NominalAlreadyExistsError(f"A skill named {name} already exists")
        entry_request = files_pb2.GetFileByIdentityRequest(identity=latest.backing.file_store.entry_file_identity)
        with translate_grpc_errors():
            entry = self._clients.files.GetFileByIdentity(entry_request).file
        previous = _head_revision(self._clients, entry)
        put = _put(self._clients, self.workspace_rid, _ENTRY_FILE, content, _replacing(previous))
        (saved,) = _apply(self._clients, folder.drive_rid, [put])
        if (error := _change_error([_ENTRY_FILE], [saved], "save")) is not None:
            raise error
        if name == folder.name:
            return entry.identity

        move = files_pb2.MoveDirectory(to=file_store_pb2.LogicalPath(path=target.root))
        # The source field is named `from`, a Python keyword, so the constructor can't take it.
        getattr(move, "from").path = folder.root
        try:
            (moved,) = _apply(self._clients, folder.drive_rid, [files_pb2.FileChange(move_directory=move)])
        except NominalError as e:
            e.add_note(f"SKILL.md is saved with the name {name}; check whether its folder moved to {target.root}.")
            raise
        if (error := _change_error([folder.root], [moved], f"move to {target.root}")) is not None:
            restore = files_pb2.RestoreFile(
                restore_revision_rid=previous,
                destination=_replacing(saved.success.file_revision.ref.managed.file_revision_rid),
            )
            (restored,) = _apply(self._clients, folder.drive_rid, [files_pb2.FileChange(restore=restore)])
            error.add_note(
                "SKILL.md has its previous content back."
                if restored.HasField("success")
                else f"SKILL.md names the skill {name}, but its folder is still {folder.root}."
            )
            raise error
        request = files_pb2.GetFileRequest(
            drive_rid=target.drive_rid, path=file_store_pb2.LogicalPath(path=target.path_of(_ENTRY_FILE))
        )
        with translate_grpc_errors():
            return self._clients.files.GetFile(request).file.identity

    def _update(self, request: skills_pb2.UpdateSkillRequest) -> Self:
        with _translate_skill_errors():
            response = self._clients.skills.UpdateSkill(request)
        # The service omits the skill when returning it would disclose a source the caller can no longer read.
        return self._refresh_from_api(response.skill) if response.HasField("skill") else self

    @classmethod
    def _from_proto(cls, clients: _Clients, raw_skill: skills_pb2.Skill) -> Self:
        last_known = raw_skill.last_known
        return cls(
            rid=raw_skill.skill_rid,
            workspace_rid=raw_skill.workspace_rid,
            name=last_known.name if last_known.HasField("name") else None,
            description=last_known.description if last_known.HasField("description") else None,
            is_enabled=raw_skill.state == skills_pb2.SKILL_STATE_ENABLED,
            is_archived=raw_skill.state == skills_pb2.SKILL_STATE_ARCHIVED,
            version=raw_skill.version,
            created_at=raw_skill.created.time.ToNanoseconds(),
            updated_at=raw_skill.updated.time.ToNanoseconds(),
            _clients=clients,
        )


@dataclass(frozen=True)
class _SkillFolder:
    """Where a skill's files are: its drive, and the folder ``skills/<name>`` in it."""

    drive_rid: str
    name: str

    @property
    def root(self) -> str:
        return f"{_SKILLS_ROOT}/{self.name}"

    def path_of(self, path: str) -> str:
        return f"{self.root}/{path}"

    @classmethod
    def of(cls, skill: skills_pb2.Skill) -> _SkillFolder:
        backing = skill.backing.file_store
        parts = backing.entry_path.path.split("/")
        if len(parts) != 3 or parts[0] != _SKILLS_ROOT or not parts[1] or parts[2] != _ENTRY_FILE:
            raise NominalError(
                f"Skill {skill.skill_rid} has no folder of files: its SKILL.md is at {backing.entry_path.path}, "
                f"not {_SKILLS_ROOT}/<name>/{_ENTRY_FILE}"
            )
        return cls(drive_rid=backing.drive_rid, name=parts[1])


def _create_skill(
    clients: Skill._Clients,
    *,
    content: str,
    files: Mapping[str, str] | None,
    enabled: bool,
    workspace_rid: str,
) -> Skill:
    name = _skill_name(content)
    texts = {_ENTRY_FILE: content, **{_reference_path(path): text for path, text in (files or {}).items()}}
    drive_request = skills_pb2.UpdateWorkspaceSkillsDriveRequest(
        skills_drive=skills_pb2.WorkspaceSkillsDrive(workspace_rid=workspace_rid)
    )
    with translate_grpc_errors():
        drive = clients.skills.UpdateWorkspaceSkillsDrive(drive_request).skills_drive.drive
    folder = _SkillFolder(drive_rid=drive.rid, name=name)
    # A folder with other files but no SKILL.md is still taken by that name.
    if not _is_empty(clients, folder):
        raise NominalAlreadyExistsError(f"A skill named {name} already exists in workspace {workspace_rid}")
    changes = [_put(clients, workspace_rid, path, text, _at_path(folder.path_of(path))) for path, text in texts.items()]
    results = _apply(clients, folder.drive_rid, changes)
    try:
        if (error := _change_error(list(texts), results, "save")) is not None:
            raise error
        backing = skills_pb2.FileStoreSkillBacking(entry_file_identity=results[0].success.file.identity)
        request = skills_pb2.CreateSkillRequest(
            skill=skills_pb2.Skill(workspace_rid=workspace_rid, backing=skills_pb2.SkillBacking(file_store=backing))
        )
        with translate_grpc_errors():
            skill = Skill._from_proto(clients, clients.skills.CreateSkill(request).skill)
    except NominalError:
        # Remove each saved file by the revision this call saved, so a name left free can be used again.
        removals = [
            files_pb2.FileChange(
                remove=files_pb2.RemoveFile(revision_rid=result.success.file_revision.ref.managed.file_revision_rid)
            )
            for result in results
            if result.HasField("success")
        ]
        _apply(clients, folder.drive_rid, removals)
        raise
    if enabled:
        try:
            skill.update(enabled=True)
        except NominalError as e:
            e.add_note(f"Skill {skill.rid} was created turned off, as a draft.")
            raise
    return skill


def _import_skill(clients: Skill._Clients, path: Path, *, enabled: bool, workspace_rid: str) -> Skill:
    files = _read_skill_folder(path)
    content = files.pop(_ENTRY_FILE)
    return _create_skill(clients, content=content, files=files, enabled=enabled, workspace_rid=workspace_rid)


def _get_skill(clients: Skill._Clients, rid: str) -> Skill:
    request = skills_pb2.GetSkillRequest(skill_rid=rid)
    with translate_grpc_errors():
        response = clients.skills.GetSkill(request)
    return Skill._from_proto(clients, response.skill)


def _iter_search_skills(
    clients: Skill._Clients,
    *,
    search_text: str | None,
    enabled: bool | None,
    archive_status: ArchiveStatusFilter,
    workspace_rid: str,
) -> Iterable[Skill]:
    states = _searched_states(enabled, archive_status)
    # The service reads an empty state list as every unarchived state, so a filter that excludes every state
    # searches nothing.
    if not states:
        return
    search_filter = skills_pb2.SkillSearchFilter(text=search_text, states=states)
    for skill in search_skills_paginated(clients.skills, workspace_rid, search_filter):
        yield Skill._from_proto(clients, skill)


def _searched_states(
    enabled: bool | None, archive_status: ArchiveStatusFilter
) -> list[skills_pb2.SkillState.ValueType]:
    unarchived = [skills_pb2.SKILL_STATE_DRAFT, skills_pb2.SKILL_STATE_ENABLED, skills_pb2.SKILL_STATE_DISABLED]
    match archive_status:
        case ArchiveStatusFilter.NOT_ARCHIVED:
            states = unarchived
        case ArchiveStatusFilter.ARCHIVED:
            states = [skills_pb2.SKILL_STATE_ARCHIVED]
        case ArchiveStatusFilter.ANY:
            states = [*unarchived, skills_pb2.SKILL_STATE_ARCHIVED]
    if enabled is None:
        return states
    return [state for state in states if (state == skills_pb2.SKILL_STATE_ENABLED) == enabled]


def _skill_name(content: str) -> str:
    """The name in SKILL.md's frontmatter, which names the skill's folder.

    Raises:
        ValueError: If SKILL.md does not start with frontmatter holding a valid name.
    """
    match = _FRONTMATTER.match(content)
    try:
        frontmatter = yaml.safe_load(match.group(1)) if match else None
    except yaml.YAMLError as ex:
        raise ValueError("SKILL.md's frontmatter is not valid YAML") from ex
    name = frontmatter.get("name") if isinstance(frontmatter, dict) else None
    if not isinstance(name, str) or len(name) > _MAX_NAME_LENGTH or not _SKILL_NAME.fullmatch(name):
        raise ValueError(
            "SKILL.md must start with frontmatter whose name is lowercase letters, numbers, and single hyphens, "
            f"at most {_MAX_NAME_LENGTH} characters"
        )
    return name


def _file_path(path: str) -> str:
    """Check that `path` is relative and stays inside a skill's folder, and return it.

    Raises:
        ValueError: If the path is absolute or has an empty, ".", or ".." part.
    """
    if "\\" in path or any(part in ("", ".", "..") for part in path.split("/")):
        raise ValueError(f"Not a path inside the skill's folder: {path!r}")
    return path


def _reference_path(path: str) -> str:
    if _file_path(path) == _ENTRY_FILE:
        raise ValueError("SKILL.md is not a reference file; change it with Skill.update(content=...)")
    return path


def _is_empty(clients: Skill._Clients, folder: _SkillFolder) -> bool:
    request = files_pb2.ListFilesRequest(
        drive_rid=folder.drive_rid, parent_path=file_store_pb2.LogicalPath(path=folder.root), page_size=1
    )
    with translate_grpc_errors():
        return not clients.files.ListFiles(request).entries


def _list_folder(clients: Skill._Clients, folder: _SkillFolder) -> dict[str, file_store_pb2.LogicalFile]:
    """Every file in a skill's folder, at any depth, keyed by its path relative to the folder, in path order."""

    def factory(page_token: str | None) -> files_pb2.ListFilesRequest:
        return files_pb2.ListFilesRequest(
            drive_rid=folder.drive_rid,
            parent_path=file_store_pb2.LogicalPath(path=folder.root),
            recursive=True,
            page_token=page_token,
        )

    return {
        entry.file.path.path.removeprefix(f"{folder.root}/"): entry.file
        for response in paginate_grpc(clients.files.ListFiles, request_factory=factory)
        for entry in response.entries
        if entry.HasField("file")
    }


def _head_revision(clients: Skill._Clients, file: file_store_pb2.LogicalFile) -> str:
    """The RID of a file's current revision."""
    if file.current_revision.HasField("managed"):
        return file.current_revision.managed.file_revision_rid
    request = files_pb2.ResolveFileRevisionRequest(source_ref=file.current_revision)
    with translate_grpc_errors():
        return clients.files.ResolveFileRevision(request).file_revision_rid


def _read_text(clients: Skill._Clients, file: file_store_pb2.LogicalFile) -> str:
    request = files_pb2.GetDownloadUrlRequest(file_revision_rid=_head_revision(clients, file))
    with translate_grpc_errors():
        url = clients.files.GetDownloadUrl(request).url
    session = create_multipart_request_session(pool_size=1, header_provider=clients.header_provider)
    response = session.get(url, timeout=_DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    try:
        return response.content.decode("utf-8")
    except UnicodeDecodeError as ex:
        raise NominalError(f"Not UTF-8 text, which the client can't read: {file.path.path}") from ex


def _put(
    clients: Skill._Clients, workspace_rid: str, path: str, text: str, destination: files_pb2.Destination
) -> files_pb2.FileChange:
    """Upload `text` and return the change that saves it at `destination`."""
    data = text.encode("utf-8")
    key = upload_file_store_object(
        clients.auth_header,
        workspace_rid,
        data,
        PurePosixPath(path).name,
        _TEXT_MIMETYPE,
        clients.upload,
        header_provider=clients.header_provider,
    )
    put = files_pb2.PutFile(
        object=files_pb2.UploadedObjectRef(object_key=key), size_bytes=len(data), destination=destination
    )
    return files_pb2.FileChange(put=put)


def _at_path(path: str) -> files_pb2.Destination:
    """A destination for a new file, which File Store refuses if a file is already at `path`."""
    return files_pb2.Destination(path=files_pb2.PathTarget(path=file_store_pb2.LogicalPath(path=path)))


def _replacing(revision_rid: str) -> files_pb2.Destination:
    """A destination replacing the file whose current revision is `revision_rid`, which File Store refuses if another
    revision has replaced it since.
    """
    return files_pb2.Destination(file_revision_rid=revision_rid)


def _apply(
    clients: Skill._Clients, drive_rid: str, changes: Sequence[files_pb2.FileChange]
) -> list[files_pb2.FileChangeResult]:
    """Apply changes in order, with one result for each. File Store applies each change on its own."""
    results: list[files_pb2.FileChangeResult] = []
    for batch in batched(changes, _MAX_FILE_CHANGES):
        request = files_pb2.ApplyFileChangesRequest(drive_rid=drive_rid, changes=batch)
        with translate_grpc_errors():
            results.extend(clients.files.ApplyFileChanges(request).results)
    return results


def _change_error(
    paths: Sequence[str], results: Sequence[files_pb2.FileChangeResult], action: str
) -> NominalError | None:
    """The error for the changes File Store refused, naming what it saved, or None when it refused none."""
    failures = {path: result.failure for path, result in zip(paths, results) if result.HasField("failure")}
    if not failures:
        return None
    message = f"Could not {action} " + "; ".join(f"{path}: {failure.message}" for path, failure in failures.items())
    saved = [path for path in paths if path not in failures]
    if saved:
        message += f". Saved: {', '.join(saved)}"
    stale = (files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED, files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS)
    conflict = any(failure.HasField("change") and failure.change in stale for failure in failures.values())
    return NominalConflictError(message) if conflict else NominalError(message)


def _read_skill_folder(path: Path) -> dict[str, str]:
    """A skill folder's files as text, keyed by path inside the folder, as the app's Import Skills reads them.

    The folder is `path` itself, or in a .zip the folder holding its shallowest SKILL.md. Paths with a part that
    starts with "." are skipped, such as `.git` and macOS's `._*` copies.
    """
    if path.is_dir():
        contents = {
            file.relative_to(path).as_posix(): file.read_bytes()
            for file in sorted(path.rglob("*"))
            if file.is_file() and _is_importable(file.relative_to(path).as_posix())
        }
    else:
        with zipfile.ZipFile(path) as archive:
            names = [info.filename for info in archive.infolist() if not info.is_dir()]
            entries = [name for name in names if _is_importable(name) and name.split("/")[-1] == _ENTRY_FILE]
            if not entries:
                raise ValueError(f"{path} has no SKILL.md")
            # The skill's SKILL.md is the shallowest one, as a folder's is the one at its top level.
            depth = min(entry.count("/") for entry in entries)
            shallowest = [entry for entry in entries if entry.count("/") == depth]
            if len(shallowest) > 1:
                raise ValueError(f"{path} holds {len(shallowest)} skill folders; import one at a time")
            prefix = shallowest[0].removesuffix(_ENTRY_FILE)
            contents = {
                name.removeprefix(prefix): archive.read(name)
                for name in names
                if name.startswith(prefix) and _is_importable(name)
            }
    if _ENTRY_FILE not in contents:
        raise ValueError(f"{path} has no SKILL.md at its top level")
    texts: dict[str, str] = {}
    binary: list[str] = []
    for name, data in contents.items():
        try:
            texts[name] = data.decode("utf-8")
        except UnicodeDecodeError:
            binary.append(name)
    if binary:
        raise ValueError(f"Skills hold UTF-8 text files only; not text: {', '.join(binary)}")
    return texts


def _is_importable(path: str) -> bool:
    return all(part and not part.startswith(".") for part in path.split("/"))


def _state_update(
    skill_rid: str, state: skills_pb2.SkillState.ValueType, expected_version: int
) -> skills_pb2.UpdateSkillRequest:
    return skills_pb2.UpdateSkillRequest(
        skill=skills_pb2.Skill(skill_rid=skill_rid, state=state),
        update_mask=field_mask_pb2.FieldMask(paths=["state"]),
        expected_version=expected_version,
    )


@contextmanager
def _translate_skill_errors() -> Iterator[None]:
    """Translate gRPC errors as usual, except that the skills service's stale-version error is a conflict."""
    with translate_grpc_errors():
        try:
            yield
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ABORTED:
                raise NominalConflictError(f"{e.code()}: {e.details()}") from e
            raise
