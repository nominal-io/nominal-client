from __future__ import annotations

import base64
import io
import json
import os
import re
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Protocol, Self

import grpc
import yaml
from google.protobuf import field_mask_pb2
from nominal_api import ingest_api, upload_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.filenames import sanitize_upload_filename
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import DEFAULT_NUM_WORKERS, _put_multipart_upload_to
from nominal.core._utils.networking import create_multipart_request_session
from nominal.exceptions import (
    NominalAlreadyExistsError,
    NominalConflictError,
    NominalError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)
from nominal.experimental.drives._pagination import list_files_paginated
from nominal.experimental.drives.changes import _move_directory_change
from nominal.experimental.drives.errors import FileStoreErrorCode
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2, files_pb2_grpc
from nominal.protos.skills.v1 import skills_pb2, skills_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

_ENTRY_FILE = "SKILL.md"
# The app and Analyst save each skill at skills/<name>/SKILL.md in the workspace's skills drive.
_SKILLS_ROOT = "skills"
_MAX_NAME_LENGTH = 64
# Mirrors Scout's SkillBackingResolver, which uses Java String.length() on the trimmed description.
_MAX_DESCRIPTION_LENGTH = 1024
_DESCRIPTION_NEEDED = (
    f"A skill that is turned on needs a description in SKILL.md's frontmatter, of 1 to {_MAX_DESCRIPTION_LENGTH} "
    "UTF-16 code units"
)
# Lowercase letters and numbers in groups joined by single hyphens.
_SKILL_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
# A YAML frontmatter block at the very start of SKILL.md, with LF or CRLF line endings.
_FRONTMATTER = re.compile(r"---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|$)", re.DOTALL)
_TEXT_MIMETYPE = "text/plain; charset=utf-8"
# ApplyFileChanges applies at most this many changes all or nothing.
_MAX_BATCH_CHANGES = 100
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

    Folder operations raise `NominalNotFoundError` if the registered SKILL.md is missing or another file has
    taken its path.
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
        def drive_files(self) -> files_pb2_grpc.FilesServiceStub: ...
        @property
        def skills(self) -> skills_pb2_grpc.SkillsServiceStub: ...
        @property
        def upload(self) -> upload_api.UploadService: ...

    def as_prompt(self, prompt: str = "") -> str:
        """Build a prompt that selects this skill by identity for Analyst.

        This formats text locally without making a request or running the skill. The
        receiving Analyst service resolves the skill's current content and checks access
        when it consumes the prompt. The skill must be enabled and readable then.

        Args:
            prompt: Additional instructions appended unchanged after the skill selection.
                Omit to select the skill without additional instructions.

        Returns:
            Prompt text containing the skill reference and any additional instructions.
        """
        payload = json.dumps({"skillRid": self.rid, "source": "record", "v": 2}, separators=(",", ":"), sort_keys=True)
        reference = "skill:v2:" + base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
        selection = json.dumps({"skillRef": reference}, separators=(",", ":"))
        return f"<SKILL>{selection}</SKILL>\n\n{prompt}"

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

    def get_content(self) -> str:
        """Read the skill's SKILL.md, frontmatter included: the content that `update(content=...)` replaces.

        Raises:
            NominalNotFoundError: If the registered SKILL.md was removed or moved from its recorded folder.
            NominalError: If SKILL.md is not UTF-8 text.
            requests.RequestException: If downloading the file fails.
        """
        latest = self._get_latest_api()
        entry = self._get_entry(latest.backing.file_store.entry_file_identity)
        _SkillFolder.of(latest).require_active_entry(entry)
        return _read_text(self._clients, entry)

    def list_files(self) -> Sequence[SkillFileInfo]:
        """List every file in the skill's folder, SKILL.md included, by path.

        Raises:
            NominalError: If the skill's SKILL.md is not at ``skills/<name>/SKILL.md``, so it has no folder.
        """
        latest = self._get_latest_api()
        folder = _SkillFolder.of(latest)
        files = folder.list_registered_files(self._clients, latest.backing.file_store.entry_file_identity)
        return [SkillFileInfo(path=path, size_bytes=files[path].size_bytes) for path in sorted(files)]

    def read_file(self, path: str) -> str:
        """Read one file in the skill's folder, such as ``SKILL.md`` or ``methods/limits.md``.

        Raises:
            ValueError: If `path` leads outside the skill's folder.
            NominalNotFoundError: If the skill has no file at this path.
            NominalError: If the file is not UTF-8 text. The client reads and writes text files only.
            requests.RequestException: If downloading the file fails.
        """
        latest = self._get_latest_api()
        folder = _SkillFolder.of(latest)
        request = files_pb2.GetFileRequest(
            drive_rid=folder.drive_rid, path=file_store_pb2.LogicalPath(path=folder.path_of(_file_path(path)))
        )
        with translate_grpc_errors():
            file = self._clients.drive_files.GetFile(request).file
        entry = self._get_entry(latest.backing.file_store.entry_file_identity)
        folder.require_active_entry(entry)
        return _read_text(self._clients, file)

    def update(self, *, content: str | None = None, enabled: bool | None = None) -> Self:
        """Update the skill in place. Fields left as None are unchanged.

        Each change is saved only if the skill's record hasn't changed since this object was fetched or last
        refreshed. Edits made to the skill's files in Drives don't change its record, so `content` replaces SKILL.md as
        it is now; read it with :meth:`get_content` first to keep such edits.

        Args:
            content: The skill's new SKILL.md, frontmatter included, replacing the current one. A new name in its
                frontmatter renames the skill: its folder moves to ``skills/<new name>``, and it keeps its RID.
            enabled: Whether the skill is turned on. Turning it on needs a valid SKILL.md, and an archived skill must
                be restored with :meth:`unarchive` first. Turning off an archived skill restores it turned off, as
                :meth:`unarchive` does.

        SKILL.md and, for a rename, the move of its folder are saved together or not at all. Then the skill records
        its new name and description.

        Raises:
            ValueError: If the name is invalid, or enabled content exceeds 64 KiB or has invalid metadata.
                Nothing is saved.
            NominalError: If `enabled` is True and the skill is archived. Nothing is saved.
            NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which changing a
                skill needs. Nothing is saved.
            NominalConflictError: If the skill changed since this object was fetched or last refreshed, or SKILL.md
                changed while it was being saved. Nothing is saved.
            NominalAlreadyExistsError: If a skill already has the new name. Nothing is saved.
            NominalNotFoundError: If `content` is given and the SKILL.md the skill points at is no longer in its
                folder, for example because it was deleted, or deleted and re-created, in Drives. Nothing is saved.
            NominalError: If the file is saved and the record update returns an error. Read the skill again before
                retrying, since a lost response can follow a successful update.
        """
        if content is None and enabled is None:
            return self.refresh()
        if enabled and self.is_archived:
            raise NominalError(f"Restore skill {self.rid} with unarchive() before turning it on")
        state: skills_pb2.SkillState.ValueType | None = None
        if enabled is not None:
            state = skills_pb2.SKILL_STATE_ENABLED if enabled else skills_pb2.SKILL_STATE_DISABLED
        if content is not None:
            name = _skill_name(content, enabled=self.is_enabled if enabled is None else enabled)
        _check_can_edit(self._clients, self.workspace_rid)
        if content is None:
            return self._update(state=state)
        identity = self._save_entry(name, content)
        # SKILL.md keeps its identity through the save and the folder's move, so the skill records the backing it
        # has, which makes the service read SKILL.md again.
        try:
            return self._update(state=state, backing=skills_pb2.FileStoreSkillBacking(entry_file_identity=identity))
        except NominalError as e:
            # A conflict here comes after SKILL.md saved, so it isn't the conflict that saves nothing.
            raise NominalError(
                f"The new SKILL.md is saved. Check skill {self.rid} with get_skill() before changing it again: {e}"
            ) from e

    def write_files(self, files: Mapping[str, str]) -> Self:
        """Add or replace reference files in the skill's folder, replacing each file as it is now.

        The files are saved together or not at all.

        Args:
            files: UTF-8 text keyed by path relative to the skill's folder, such as ``methods/limits.md``. SKILL.md
                is not a reference file; change it with :meth:`update`.

        Raises:
            ValueError: If a path is SKILL.md or leads outside the skill's folder, or there are more than 100 files.
                Nothing is saved.
            NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which changing a
                skill needs. Nothing is saved.
            NominalConflictError: If another save changed or created one of the files while it was being saved.
                Nothing is saved.
        """
        texts = {_reference_path(path): text for path, text in files.items()}
        _require_one_batch(len(texts))
        _check_can_edit(self._clients, self.workspace_rid)
        latest = self._get_latest_api()
        folder = _SkillFolder.of(latest)
        current = folder.list_registered_files(self._clients, latest.backing.file_store.entry_file_identity)
        puts = {
            path: (text, _replacing(current[path]) if path in current else _at_path(folder.path_of(path)))
            for path, text in texts.items()
        }
        _apply(self._clients, folder.drive_rid, _put_changes(self._clients, self.workspace_rid, puts))
        return self

    def remove_files(self, paths: Sequence[str]) -> Self:
        """Remove reference files from the skill's folder.

        The files are removed together or not at all.

        Raises:
            ValueError: If a path is SKILL.md or leads outside the skill's folder, or there are more than 100 paths.
                Nothing is removed.
            NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which changing a
                skill needs. Nothing is removed.
            NominalNotFoundError: If the skill has no file at one of the paths. Nothing is removed.
            NominalConflictError: If another save changed one of the files while it was being removed. Nothing is
                removed.
        """
        targets = [_reference_path(path) for path in dict.fromkeys(paths)]
        _require_one_batch(len(targets))
        _check_can_edit(self._clients, self.workspace_rid)
        latest = self._get_latest_api()
        folder = _SkillFolder.of(latest)
        current = folder.list_registered_files(self._clients, latest.backing.file_store.entry_file_identity)
        missing = [path for path in targets if path not in current]
        if missing:
            raise NominalNotFoundError(f"Skill {self.rid} has no file at {', '.join(missing)}")
        removals = [
            (
                f"remove {path}",
                files_pb2.FileChange(
                    remove=files_pb2.RemoveFile(revision_rid=current[path].current_revision.managed.file_revision_rid)
                ),
            )
            for path in targets
        ]
        _apply(self._clients, folder.drive_rid, removals)
        return self

    def archive(self) -> None:
        """Archive the skill, hiding it from the skills library, Analyst, and connected agents.

        Raises:
            NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which changing a
                skill needs.
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
        """
        _check_can_edit(self._clients, self.workspace_rid)
        self._update(state=skills_pb2.SKILL_STATE_ARCHIVED)

    def unarchive(self) -> None:
        """Restore an archived skill. It comes back turned off; turn it on with ``update(enabled=True)``.

        Refreshes this object first, and does nothing when the skill is not archived.

        Raises:
            NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which changing a
                skill needs.
        """
        if self.refresh().is_archived:
            _check_can_edit(self._clients, self.workspace_rid)
            self._update(state=skills_pb2.SKILL_STATE_DISABLED)

    def export(self, path: PathLike) -> Path:
        """Write the skill's current folder, named after the skill, under `path`, and return the folder.

        Every file is read before any is written.

        Raises:
            ValueError: If the skill's name or a file's path could lead outside `path` on this system, such as a path
                with a backslash or, on Windows, a drive or a colon. Nothing is written.
            FileExistsError: If `path` already has an entry with the skill's name, or two of the skill's files land on
                the same file on this system, such as names differing only in case where case is ignored. The files
                written before the second one stay.
            NominalError: If one of the skill's files is not UTF-8 text.
            requests.RequestException: If downloading a file fails.
        """
        latest = self._get_latest_api()
        folder = _SkillFolder.of(latest)
        target = _export_destination(Path(path), folder.name)
        if target.exists():
            raise FileExistsError(f"{target} already exists")
        files = folder.list_registered_files(self._clients, latest.backing.file_store.entry_file_identity)
        # File Store accepts paths this system may read as leaving the folder, so check every path before reading or
        # writing any file.
        destinations = {file_path: _export_destination(target, file_path) for file_path in files}
        texts = {file_path: _read_text(self._clients, file) for file_path, file in files.items()}
        target.mkdir(parents=True, exist_ok=False)
        for file_path, text in texts.items():
            destination = destinations[file_path]
            destination.parent.mkdir(parents=True, exist_ok=True)
            # A file that is already there is another of the skill's files with the same path on this system.
            with destination.open("xb") as written:
                written.write(text.encode("utf-8"))
        return target

    def _get_entry(self, identity: file_store_pb2.LogicalFileIdentity) -> file_store_pb2.LogicalFile:
        with translate_grpc_errors():
            return self._clients.drive_files.GetFileByIdentity(
                files_pb2.GetFileByIdentityRequest(identity=identity)
            ).file

    def _save_entry(self, name: str, content: str) -> file_store_pb2.LogicalFileIdentity:
        """Save SKILL.md, named `name` in its frontmatter, with the move of the skill's folder in the same batch when
        the name changes, and return the identity of the SKILL.md the skill records.
        """
        identity = self._get_latest_api().backing.file_store.entry_file_identity
        # SKILL.md's revision is read before the skill's version is checked: another update's SKILL.md saved after this
        # read makes the save below refuse the stale revision, and one it recorded before fails the version check.
        entry = self._get_entry(identity)
        latest = self._get_latest_api()
        if latest.version != self.version:
            raise NominalConflictError(f"Skill {self.rid} changed since this object was fetched; refresh it")
        folder = _SkillFolder.of(latest)
        folder.require_active_entry(entry)
        move_to = None if name == folder.name else _SkillFolder(drive_rid=folder.drive_rid, name=name)
        if move_to is not None:
            if latest.state == skills_pb2.SKILL_STATE_ARCHIVED:
                raise NominalError(f"Restore skill {self.rid} with unarchive() before renaming it")
            if not _is_empty(self._clients, move_to):
                raise NominalAlreadyExistsError(f"A skill named {name} already exists")
        changes = _put_changes(self._clients, self.workspace_rid, {_ENTRY_FILE: (content, _replacing(entry))})
        if move_to is not None:
            changes.append((f"save the move to {move_to.root}", _move_directory_change(folder.root, move_to.root)))
        _apply(self._clients, folder.drive_rid, changes)
        return identity

    def _update(
        self,
        *,
        state: skills_pb2.SkillState.ValueType | None = None,
        backing: skills_pb2.FileStoreSkillBacking | None = None,
    ) -> Self:
        """Record the skill's new state, backing, or both, if its record hasn't changed since this object's version."""
        skill = skills_pb2.Skill(skill_rid=self.rid)
        paths = []
        if backing is not None:
            skill.backing.file_store.CopyFrom(backing)
            paths.append("backing")
        if state is not None:
            skill.state = state
            paths.append("state")
        request = skills_pb2.UpdateSkillRequest(
            skill=skill, update_mask=field_mask_pb2.FieldMask(paths=paths), expected_version=self.version
        )
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

    def require_active_entry(self, entry: file_store_pb2.LogicalFile) -> None:
        if entry.state != file_store_pb2.FILE_STATE_ACTIVE or entry.path.path != self.path_of(_ENTRY_FILE):
            raise NominalNotFoundError(f"The registered SKILL.md is no longer in {self.root}")

    def list_registered_files(
        self, clients: Skill._Clients, identity: file_store_pb2.LogicalFileIdentity
    ) -> dict[str, file_store_pb2.LogicalFile]:
        files = {
            entry.file.path.path.removeprefix(f"{self.root}/"): entry.file
            for entry in list_files_paginated(clients.drive_files, self.drive_rid, self.root, recursive=True)
            if entry.HasField("file")
        }
        entry = files.get(_ENTRY_FILE)
        if entry is None or entry.identity != identity:
            raise NominalNotFoundError(f"The registered SKILL.md is no longer in {self.root}")
        return files

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


def _put_changes(
    clients: Skill._Clients, workspace_rid: str, puts: Mapping[str, tuple[str, files_pb2.Destination]]
) -> list[tuple[str, files_pb2.FileChange]]:
    """Upload each text in `puts`, keyed by its path in the skill's folder, and return the change that saves it at its
    destination, labeled with what it does, in the same order.
    """
    # Nothing is saved until the changes apply, so the uploads run at once.
    pool = ThreadPoolExecutor(max_workers=DEFAULT_NUM_WORKERS)
    try:
        keys = list(pool.map(lambda path: _upload(clients, workspace_rid, path, puts[path][0]), puts))
    finally:
        pool.shutdown(cancel_futures=True)
    return [
        (
            f"save {path}",
            files_pb2.FileChange(
                put=files_pb2.PutFile(object=files_pb2.UploadedObjectRef(object_key=key), destination=destination)
            ),
        )
        for (path, (_, destination)), key in zip(puts.items(), keys)
    ]


def _apply(
    clients: Skill._Clients, drive_rid: str, changes: Sequence[tuple[str, files_pb2.FileChange]]
) -> Sequence[files_pb2.FileChangeResult]:
    """Apply `changes` together or not at all, and return a result for each. Each change comes with a label saying
    what it does, which names it in the error when File Store refuses it.

    Raises:
        NominalError: If File Store refuses a change, as the subclass `_refusal` picks for it. Nothing is changed.
    """
    if not changes:
        return []
    request = files_pb2.ApplyFileChangesRequest(
        drive_rid=drive_rid,
        changes=[change for _, change in changes],
        failure_policy=files_pb2.FAILURE_POLICY_ABORT,
    )
    with translate_grpc_errors():
        results = clients.drive_files.ApplyFileChanges(request).results
    refused = [
        (label, change, result.failure)
        for (label, change), result in zip(changes, results)
        if result.HasField("failure")
    ]
    if refused:
        # File Store refuses the batch for one change, which says why, and reports the others as batch failures.
        cause = next(
            (
                refusal
                for refusal in refused
                if FileStoreErrorCode._from_failure(refusal[2]) != FileStoreErrorCode.BATCH_FAILURE
            ),
            refused[0],
        )
        raise _refusal(*cause)
    return results


def _refusal(label: str, change: files_pb2.FileChange, failure: files_pb2.FileChangeFailure) -> NominalError:
    """The error for a batch File Store refused because of `change`, which `label` describes. The batch changed
    nothing.
    """
    code = FileStoreErrorCode._from_failure(failure)
    if code == FileStoreErrorCode.PATH_ALREADY_EXISTS and change.HasField("move_directory"):
        # Another save took the new name after it was checked.
        return NominalAlreadyExistsError(
            f"A skill named {PurePosixPath(change.move_directory.to.path).name} already exists"
        )
    message = f"Could not {label}: {failure.message}. Nothing was changed"
    if code in (FileStoreErrorCode.REVISION_PRECONDITION_FAILED, FileStoreErrorCode.PATH_ALREADY_EXISTS):
        return NominalConflictError(message)
    return NominalError(message)


# Refusals the skills service makes before it registers a skill, after which a failed create removes its files.
_CREATE_REFUSALS = frozenset(
    {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.NOT_FOUND,
    }
)


def _create_skill(
    clients: Skill._Clients,
    *,
    content: str,
    files: Mapping[str, str] | None,
    enabled: bool,
    workspace_rid: str,
) -> Skill:
    name = _skill_name(content, enabled=enabled)
    texts = {_ENTRY_FILE: content, **{_reference_path(path): text for path, text in (files or {}).items()}}
    _require_one_batch(len(texts))
    # Finding the skills drive also refuses a user who can't edit the workspace's configuration.
    folder = _SkillFolder(drive_rid=_skills_drive_rid(clients, workspace_rid), name=name)
    # A folder with files but no SKILL.md is still taken by that name.
    if not _is_empty(clients, folder):
        raise NominalAlreadyExistsError(f"A skill named {name} already exists in workspace {workspace_rid}")
    puts = {path: (text, _at_path(folder.path_of(path))) for path, text in texts.items()}
    try:
        results = _apply(clients, folder.drive_rid, _put_changes(clients, workspace_rid, puts))
    except NominalConflictError as e:
        # Every put is a new file, so File Store refuses one only when another save put a file in the folder first.
        raise NominalAlreadyExistsError(f"A skill named {name} already exists in workspace {workspace_rid}") from e
    # The changes are the puts, in the order of `texts`.
    saved = {path: result.success.file for path, result in zip(texts, results)}
    backing = skills_pb2.FileStoreSkillBacking(entry_file_identity=saved[_ENTRY_FILE].identity)
    request = skills_pb2.CreateSkillRequest(
        skill=skills_pb2.Skill(workspace_rid=workspace_rid, backing=skills_pb2.SkillBacking(file_store=backing))
    )
    try:
        with translate_grpc_errors():
            skill = Skill._from_proto(clients, clients.skills.CreateSkill(request).skill)
    except NominalError as e:
        cause = e.__cause__
        if not (isinstance(cause, grpc.RpcError) and cause.code() in _CREATE_REFUSALS):
            # The skill may be registered, as after a timeout, so its files stay.
            e.add_note(
                f"It's unknown whether the skill was created, so the files this call saved are still in {folder.root} "
                f"in the skills drive of workspace {workspace_rid}. Look for skill {name} with search_skills() before "
                "creating it again."
            )
            raise
        # Remove the revisions this call saved, so the name is free to use again. File Store refuses to remove a file
        # that another save has changed since, and then removes none.
        removals = [
            (
                f"remove {path}",
                files_pb2.FileChange(
                    remove=files_pb2.RemoveFile(revision_rid=file.current_revision.managed.file_revision_rid)
                ),
            )
            for path, file in saved.items()
        ]
        try:
            _apply(clients, folder.drive_rid, removals)
        except NominalError as removal_error:
            e.add_note(
                f"The files this call saved couldn't be removed, so they are still in {folder.root} in the skills "
                f"drive of workspace {workspace_rid}: {removal_error}"
            )
        raise
    if enabled:
        try:
            skill.update(enabled=True)
        except NominalError as e:
            e.add_note(f"Skill {skill.rid} was created. Check its current state with get_skill() before retrying.")
            raise
    return skill


def _skill_name(content: str, *, enabled: bool) -> str:
    """Validate the folder name and, when enabling, Scout's entry size and metadata constraints."""
    if enabled and len(content.encode("utf-8")) > 64 * 1024:
        raise ValueError("An enabled SKILL.md must fit in 64 KiB of UTF-8 text")
    match = _FRONTMATTER.match(content)
    try:
        source = match.group(1) if match else ""
        # Scout rejects alias events. Once safe_load resolves them, they are indistinguishable from literal values.
        if enabled and any(isinstance(event, yaml.AliasEvent) for event in yaml.parse(source, Loader=yaml.SafeLoader)):
            raise ValueError("Enabled skill frontmatter cannot contain YAML aliases")
        frontmatter = yaml.safe_load(source)
    except yaml.YAMLError as ex:
        raise ValueError("SKILL.md's frontmatter is not valid YAML") from ex
    fields = frontmatter if isinstance(frontmatter, dict) else {}
    name = fields.get("name")
    if not isinstance(name, str) or len(name) > _MAX_NAME_LENGTH or not _SKILL_NAME.fullmatch(name):
        raise ValueError(
            "SKILL.md must start with frontmatter whose name is lowercase letters, numbers, and single hyphens, "
            f"at most {_MAX_NAME_LENGTH} characters"
        )
    if enabled:
        _validate_enabled_entry(fields)
    return name


def _utf16_length(value: str) -> int:
    """Count UTF-16 code units, as Scout does for frontmatter text limits."""
    return len(value.encode("utf-16-le")) // 2


def _validate_enabled_entry(fields: Mapping[str, object]) -> None:
    description = fields.get("description")
    if not isinstance(description, str) or not 0 < _utf16_length(description.strip()) <= _MAX_DESCRIPTION_LENGTH:
        raise ValueError(f"{_DESCRIPTION_NEEDED}; pass enabled=False to save a draft")
    for field_name in ("license", "compatibility", "allowed-tools"):
        if field_name in fields:
            value = fields[field_name]
            if not isinstance(value, str) or not value:
                raise ValueError(f"{field_name} must be nonempty text")
            if field_name == "compatibility" and _utf16_length(value) > 500:
                raise ValueError("compatibility exceeds Scout's 500 UTF-16 code unit limit")
    if "metadata" in fields:
        metadata = fields["metadata"]
        if not isinstance(metadata, dict) or not all(isinstance(v, str) for v in metadata.values()):
            raise ValueError("metadata must be a mapping with text values")


def _file_path(path: str) -> str:
    """Check that `path` is relative and stays inside a skill's folder, and return it.

    Raises:
        ValueError: If the path is absolute or has an empty, ".", or ".." part.
    """
    if "\\" in path or any(part in ("", ".", "..") for part in path.split("/")):
        raise ValueError(f"Not a path inside the skill's folder: {path!r}")
    return path


_ON_WINDOWS = os.name == "nt"


def _export_destination(target: Path, path: str) -> Path:
    """Where `path`, a skill's folder name or a file's path in it, is written under `target` on this system.

    Raises:
        ValueError: If the path could lead outside `target` here: a path with a backslash, or on Windows a drive or a
            colon, which names a drive or a file's hidden stream.
    """
    relative = Path(_file_path(path))
    destination = target / relative
    if relative.anchor or (_ON_WINDOWS and ":" in path) or not destination.resolve().is_relative_to(target.resolve()):
        raise ValueError(f"Not a path inside the skill's folder on this system: {path!r}")
    return destination


def _reference_path(path: str) -> str:
    if _file_path(path) == _ENTRY_FILE:
        raise ValueError("SKILL.md is not a reference file; change it with Skill.update(content=...)")
    return path


def _is_empty(clients: Skill._Clients, folder: _SkillFolder) -> bool:
    request = files_pb2.ListFilesRequest(
        drive_rid=folder.drive_rid, parent_path=file_store_pb2.LogicalPath(path=folder.root), page_size=1
    )
    with translate_grpc_errors():
        return not clients.drive_files.ListFiles(request).entries


def _read_text(clients: Skill._Clients, file: file_store_pb2.LogicalFile) -> str:
    request = files_pb2.GetDownloadUrlRequest(source_ref=file.current_revision)
    with translate_grpc_errors():
        url = clients.drive_files.GetDownloadUrl(request).url
    session = create_multipart_request_session(pool_size=1, header_provider=clients.header_provider)
    try:
        response = session.get(url, timeout=_DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        try:
            return response.content.decode("utf-8")
        except UnicodeDecodeError as ex:
            raise NominalError(f"Not UTF-8 text, which the client can't read: {file.path.path}") from ex
    finally:
        session.close()


def _upload(clients: Skill._Clients, workspace_rid: str, path: str, text: str) -> str:
    """Upload `text` for File Store to save at `path`, and return the upload's key."""
    return _put_multipart_upload_to(
        clients.auth_header,
        workspace_rid,
        io.BytesIO(text.encode("utf-8")),
        sanitize_upload_filename(PurePosixPath(path).name),
        _TEXT_MIMETYPE,
        clients.upload,
        header_provider=clients.header_provider,
        destination=ingest_api.UploadDestination.FILE_STORE,
        max_workers=1,
    ).key


def _at_path(path: str) -> files_pb2.Destination:
    """A destination for a new file, which File Store refuses if a file is already at `path`."""
    return files_pb2.Destination(path=files_pb2.PathTarget(path=file_store_pb2.LogicalPath(path=path)))


def _replacing(file: file_store_pb2.LogicalFile) -> files_pb2.Destination:
    """A destination replacing `file` as it is now, which File Store refuses if a newer revision has replaced it."""
    return files_pb2.Destination(file_revision_rid=file.current_revision.managed.file_revision_rid)


def _require_one_batch(count: int) -> None:
    """Refuse a write that won't fit one all-or-nothing batch, before anything is uploaded."""
    if count > _MAX_BATCH_CHANGES:
        raise ValueError(f"One save changes at most {_MAX_BATCH_CHANGES} files; split these {count} into several")


def _check_can_edit(clients: Skill._Clients, workspace_rid: str) -> None:
    """Refuse a user who can't edit the workspace's configuration, which changing a skill needs. Each write calls this
    before its first change.

    Raises:
        NominalPermissionDeniedError: If the workspace doesn't exist, or the user can't edit its configuration.
    """
    _skills_drive_rid(clients, workspace_rid)


def _skills_drive_rid(clients: Skill._Clients, workspace_rid: str) -> str:
    """The RID of the workspace's skills drive, found or created.

    The skills service returns it only to people who can edit the workspace's configuration, which every skill write
    needs, and reports anyone else's request as a missing workspace. Saving files and changing a skill's state call
    this first, so a user without the permission is refused before anything is saved; File Store checks only the
    drive's own permissions.

    Raises:
        NominalPermissionDeniedError: If the workspace doesn't exist, or the user can't edit its configuration.
    """
    request = skills_pb2.UpdateWorkspaceSkillsDriveRequest(
        skills_drive=skills_pb2.WorkspaceSkillsDrive(workspace_rid=workspace_rid)
    )
    try:
        with translate_grpc_errors():
            return clients.skills.UpdateWorkspaceSkillsDrive(request).skills_drive.drive.rid
    except NominalNotFoundError as e:
        raise NominalPermissionDeniedError(
            f"Workspace {workspace_rid} doesn't exist, or this user can't edit its configuration, which creating and "
            "changing skills needs"
        ) from e


def _read_skill_folder(path: Path) -> dict[str, str]:
    """A skill folder's files as text, keyed by path inside the folder, as the app's Import Skills reads them.

    The folder is `path` itself, or in a .zip the folder holding its shallowest SKILL.md. Paths with a part that
    starts with "." are skipped, such as `.git` and macOS's `._*` copies. In a folder, symbolic links and the files
    reached through them are skipped too, since a link can lead outside the folder.
    """
    if path.is_dir():
        root = path.resolve()
        contents = {
            file.relative_to(path).as_posix(): file.read_bytes()
            for file in sorted(path.rglob("*"))
            if file.is_file()
            and not file.is_symlink()
            and file.resolve().is_relative_to(root)
            and _is_importable(file.relative_to(path).as_posix())
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
