from __future__ import annotations

import zipfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Protocol, Self

import grpc
from google.protobuf import field_mask_pb2

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_skills_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalConflictError, NominalError, NominalNotFoundError
from nominal.protos.skills.v1 import skills_pb2, skills_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

_ENTRY_FILE = "SKILL.md"
# BatchGetSkillFiles reads at most this many paths per call.
_MAX_PATHS_PER_READ = 50


class SkillLoadStatus(Enum):
    """Whether Analyst and connected agents can load a skill."""

    UNSPECIFIED = "UNSPECIFIED"
    """Not reported, as after a change, or a status from a newer server."""
    AVAILABLE = "AVAILABLE"
    """SKILL.md is readable and valid."""
    INVALID = "INVALID"
    """SKILL.md is readable but is not a valid skill, for example because its frontmatter has no description."""
    MISSING = "MISSING"
    """The skill's folder or SKILL.md no longer exists."""
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
    """A file in a skill's folder: its path relative to the folder, its size, and the SHA-256 of its bytes."""

    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class Skill(HasRid, RefreshableGrpcMixin[skills_pb2.Skill]):
    """A skill in a workspace's skills library: a SKILL.md, and any reference files, that Analyst and connected
    agents follow.

    `name` and `description` come from SKILL.md's frontmatter, and are None while it has no valid frontmatter.
    """

    rid: str
    workspace_rid: str
    name: str | None
    description: str | None
    is_enabled: bool
    is_archived: bool
    version: int
    load_status: SkillLoadStatus
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def skills(self) -> skills_pb2_grpc.SkillsServiceStub: ...

    def _get_latest_api(self) -> skills_pb2.Skill:
        request = skills_pb2.GetSkillRequest(skill_rid=self.rid, include_content=True)
        with translate_grpc_errors():
            return self._clients.skills.GetSkill(request).skill

    def list_files(self) -> Sequence[SkillFileInfo]:
        """List every file in the skill's folder, SKILL.md included."""
        return [
            SkillFileInfo(path=file.path, size_bytes=file.size_bytes, sha256=file.sha256)
            for file in self._get_latest_api().files
        ]

    def read_file(self, path: str) -> str:
        """Read one file in the skill's folder, such as ``SKILL.md`` or ``methods/limits.md``.

        Raises:
            NominalNotFoundError: If the skill has no file at this path.
            NominalError: If the file is not UTF-8 text. The client reads and writes text files only.
        """
        return _read_files(self._clients, self.rid, [path])[path]

    def update(self, *, content: str | None = None, enabled: bool | None = None) -> Self:
        """Update the skill in place. Fields left as None are unchanged.

        Args:
            content: The skill's new SKILL.md, frontmatter included. A new name in its frontmatter renames the skill
                and keeps its RID. Saved only if the skill hasn't changed since this object was fetched or last
                refreshed.
            enabled: Whether the skill is turned on. Applies to the skill's current version. An archived skill must
                be restored with :meth:`unarchive` before it can be turned on.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalAlreadyExistsError: If another skill in the workspace has the new name.
            NominalInvalidArgumentError: If SKILL.md breaks the Agent Skills format or a limit.
        """
        skill = skills_pb2.Skill(skill_rid=self.rid, content=content)
        paths = []
        if content is not None:
            paths.append("content")
        if enabled is not None:
            skill.state = skills_pb2.SKILL_STATE_ENABLED if enabled else skills_pb2.SKILL_STATE_DISABLED
            paths.append("state")
        request = skills_pb2.UpdateSkillRequest(
            skill=skill,
            update_mask=field_mask_pb2.FieldMask(paths=paths),
            # Zero leaves the version unchecked, which the service allows only for a state change.
            expected_version=self.version if content is not None else 0,
        )
        return self._update(request)

    def write_files(self, files: Mapping[str, str]) -> Self:
        """Add or replace reference files in one save, all or nothing.

        Args:
            files: UTF-8 text keyed by path relative to the skill's folder, such as ``methods/limits.md``. SKILL.md
                is not a reference file; change it with :meth:`update`.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalInvalidArgumentError: If a path or file breaks a limit, or a path is SKILL.md.
        """
        request = skills_pb2.BatchUpdateSkillFilesRequest(
            skill_rid=self.rid,
            expected_version=self.version,
            files=[skills_pb2.SkillFile(path=path, content=text) for path, text in files.items()],
        )
        with _translate_skill_errors():
            response = self._clients.skills.BatchUpdateSkillFiles(request)
        return self._refresh_from_api(response.skill)

    def remove_files(self, paths: Sequence[str]) -> Self:
        """Remove reference files in one save, all or nothing.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalNotFoundError: If the skill has no file at one of the paths.
            NominalInvalidArgumentError: If a path is SKILL.md.
        """
        request = skills_pb2.BatchDeleteSkillFilesRequest(
            skill_rid=self.rid, expected_version=self.version, paths=list(paths)
        )
        with _translate_skill_errors():
            response = self._clients.skills.BatchDeleteSkillFiles(request)
        return self._refresh_from_api(response.skill)

    def archive(self) -> None:
        """Archive the skill, hiding it from the skills library, Analyst, and connected agents."""
        self._update(_state_update(self.rid, skills_pb2.SKILL_STATE_ARCHIVED))

    def unarchive(self) -> None:
        """Restore an archived skill. It comes back turned off; turn it on with ``update(enabled=True)``.

        Does nothing when this object shows the skill as not archived.
        """
        if self.is_archived:
            self._update(_state_update(self.rid, skills_pb2.SKILL_STATE_DISABLED))

    def export(self, path: PathLike) -> Path:
        """Write the skill's current folder, named after the skill, under `path`, and return the folder.

        Every file is read before any is written.

        Raises:
            FileExistsError: If `path` already has an entry with the skill's name.
            NominalError: If the skill has no valid name, or one of its files is not UTF-8 text.
        """
        latest = self._get_latest_api()
        if not latest.HasField("name"):
            raise NominalError(f"Skill {self.rid} has no valid name in its SKILL.md to name its folder")
        folder = Path(path) / latest.name
        if folder.exists():
            raise FileExistsError(f"{folder} already exists")
        texts = _read_files(self._clients, self.rid, [file.path for file in latest.files])
        for file_path, text in texts.items():
            target = folder / file_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(text.encode("utf-8"))
        return folder

    def _update(self, request: skills_pb2.UpdateSkillRequest) -> Self:
        with _translate_skill_errors():
            response = self._clients.skills.UpdateSkill(request)
        # The service omits the skill when returning it would disclose a source the caller can no longer read.
        return self._refresh_from_api(response.skill) if response.HasField("skill") else self

    @classmethod
    def _from_proto(cls, clients: _Clients, raw_skill: skills_pb2.Skill) -> Self:
        return cls(
            rid=raw_skill.skill_rid,
            workspace_rid=raw_skill.workspace_rid,
            name=raw_skill.name if raw_skill.HasField("name") else None,
            description=raw_skill.description if raw_skill.HasField("description") else None,
            is_enabled=raw_skill.state == skills_pb2.SKILL_STATE_ENABLED,
            is_archived=raw_skill.state == skills_pb2.SKILL_STATE_ARCHIVED,
            version=raw_skill.version,
            load_status=SkillLoadStatus._from_proto(raw_skill.backing_status),
            created_at=raw_skill.created.time.ToNanoseconds(),
            updated_at=raw_skill.updated.time.ToNanoseconds(),
            _clients=clients,
        )


def _create_skill(
    clients: Skill._Clients,
    *,
    content: str,
    files: Mapping[str, str] | None,
    enabled: bool,
    workspace_rid: str,
) -> Skill:
    request = skills_pb2.CreateSkillRequest(
        skill=skills_pb2.Skill(
            workspace_rid=workspace_rid,
            content=content,
            state=skills_pb2.SKILL_STATE_ENABLED if enabled else skills_pb2.SKILL_STATE_DRAFT,
        ),
        files=[] if files is None else [skills_pb2.SkillFile(path=path, content=text) for path, text in files.items()],
    )
    with translate_grpc_errors():
        response = clients.skills.CreateSkill(request)
    return Skill._from_proto(clients, response.skill)


def _import_skill(clients: Skill._Clients, path: Path, *, enabled: bool, workspace_rid: str) -> Skill:
    files = _read_skill_folder(path)
    content = files.pop(_ENTRY_FILE)
    return _create_skill(clients, content=content, files=files, enabled=enabled, workspace_rid=workspace_rid)


def _get_skill(clients: Skill._Clients, rid: str) -> Skill:
    request = skills_pb2.GetSkillRequest(skill_rid=rid, include_content=True)
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


def _search_skills(
    clients: Skill._Clients,
    *,
    search_text: str | None,
    enabled: bool | None,
    archive_status: ArchiveStatusFilter,
    workspace_rid: str,
) -> Sequence[Skill]:
    return list(
        _iter_search_skills(
            clients,
            search_text=search_text,
            enabled=enabled,
            archive_status=archive_status,
            workspace_rid=workspace_rid,
        )
    )


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


def _read_files(clients: Skill._Clients, skill_rid: str, paths: Sequence[str]) -> dict[str, str]:
    texts: dict[str, str] = {}
    for start in range(0, len(paths), _MAX_PATHS_PER_READ):
        request = skills_pb2.BatchGetSkillFilesRequest(
            skill_rid=skill_rid, paths=paths[start : start + _MAX_PATHS_PER_READ]
        )
        with translate_grpc_errors():
            response = clients.skills.BatchGetSkillFiles(request)
        if response.not_found_paths:
            raise NominalNotFoundError(f"Skill {skill_rid} has no file at {', '.join(response.not_found_paths)}")
        binary = [file.path for file in response.files if not file.HasField("content")]
        if binary:
            raise NominalError(f"Not UTF-8 text, which the client can't read: {', '.join(binary)}")
        texts.update((file.path, file.content) for file in response.files)
    return texts


def _read_skill_folder(path: Path) -> dict[str, str]:
    """A skill folder's files as text, keyed by path inside the folder, as the app's Import Skills reads them.

    The folder is `path` itself, or in a .zip the folder holding its one SKILL.md. Paths with a part that starts with
    "." are skipped, such as `.git` and macOS's `._*` copies.
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
            if len(entries) != 1:
                raise ValueError(f"{path} must hold exactly one SKILL.md; it holds {len(entries)}")
            prefix = entries[0].removesuffix(_ENTRY_FILE)
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


def _state_update(skill_rid: str, state: skills_pb2.SkillState.ValueType) -> skills_pb2.UpdateSkillRequest:
    return skills_pb2.UpdateSkillRequest(
        skill=skills_pb2.Skill(skill_rid=skill_rid, state=state),
        update_mask=field_mask_pb2.FieldMask(paths=["state"]),
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
