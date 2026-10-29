from __future__ import annotations

import re
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, Self

import grpc
import yaml
from google.protobuf import field_mask_pb2

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.frontend_urls import skill_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalConflictError, NominalError, NominalNotFoundError
from nominal.protos.skills.v1 import skills_pb2, skills_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

_ENTRY_FILE = "SKILL.md"
# BatchGetSkillFiles reads at most this many paths per call.
_MAX_PATHS_PER_READ = 50
# Matches the skills service's frontmatter delimiter line.
_FRONTMATTER_DELIMITER = re.compile(r"---[ \t]*\r?")


class SkillLoadStatus(Enum):
    """Whether Analyst and connected agents can load a skill.

    AVAILABLE means its SKILL.md is readable and valid. INVALID means SKILL.md is readable but is not a valid
    skill, for example because its frontmatter has no description. MISSING means the skill's folder or SKILL.md
    is gone. UNAVAILABLE means SKILL.md could not be read for another reason, such as an unavailable source.
    """

    AVAILABLE = "AVAILABLE"
    INVALID = "INVALID"
    MISSING = "MISSING"
    UNAVAILABLE = "UNAVAILABLE"

    @classmethod
    def _from_proto(cls, value: skills_pb2.SkillBackingStatus.ValueType) -> SkillLoadStatus | None:
        match value:
            case skills_pb2.SKILL_BACKING_STATUS_AVAILABLE:
                return cls.AVAILABLE
            case skills_pb2.SKILL_BACKING_STATUS_INVALID:
                return cls.INVALID
            case skills_pb2.SKILL_BACKING_STATUS_MISSING:
                return cls.MISSING
            case skills_pb2.SKILL_BACKING_STATUS_UNAVAILABLE:
                return cls.UNAVAILABLE
            case _:
                return None


@dataclass(frozen=True)
class SkillFileInfo:
    """A file in a skill's folder: its path relative to the folder, its size, and the SHA-256 of its bytes."""

    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class _SkillContent:
    instructions: str
    files: Sequence[SkillFileInfo]


@dataclass(frozen=True)
class Skill(HasRid, RefreshableGrpcMixin[skills_pb2.Skill]):
    """A skill in a workspace's skills library: instructions, and any reference files, that Analyst and connected
    agents follow.

    ``name`` and ``description`` are None while the skill's SKILL.md has no valid frontmatter. ``load_status`` is
    None when the response that built this object did not report it, as after a change; refresh to read it.
    """

    rid: str
    workspace_rid: str
    name: str | None
    description: str | None
    is_enabled: bool
    is_archived: bool
    version: int
    load_status: SkillLoadStatus | None
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    _content: _SkillContent | None = field(repr=False)
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def skills(self) -> skills_pb2_grpc.SkillsServiceStub: ...

    def _get_latest_api(self) -> skills_pb2.Skill:
        request = skills_pb2.GetSkillRequest(skill_rid=self.rid, include_content=True)
        with translate_grpc_errors():
            return self._clients.skills.GetSkill(request).skill

    @property
    def nominal_url(self) -> str:
        """Returns a link to the skill in the Nominal app."""
        return skill_url(self._clients, self.workspace_rid, self.rid)

    def get_instructions(self) -> str:
        """Return the skill's instructions: the body of its SKILL.md after the frontmatter.

        Search results and changed skills don't carry instructions, so this may fetch the skill, refreshing this
        object.

        Raises:
            NominalError: If the skill can't load; ``load_status`` says why.
        """
        return self._loaded_content().instructions

    def list_files(self) -> Sequence[SkillFileInfo]:
        """Return every file in the skill's folder, SKILL.md included.

        Like :meth:`get_instructions`, this may fetch the skill, refreshing this object.

        Raises:
            NominalError: If the skill can't load; ``load_status`` says why.
        """
        return self._loaded_content().files

    def read_file(self, path: str) -> str:
        """Return the text of one file in the skill's folder, such as ``methods/problem-definition.md``.

        Raises:
            NominalNotFoundError: If the skill has no file at this path.
            NominalError: If the file is not UTF-8 text. The client reads and writes text files only.
        """
        return self._read_files([path])[path]

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        instructions: str | None = None,
        enabled: bool | None = None,
    ) -> Self:
        """Update the skill in place. Fields left as None are unchanged.

        A new name, description, or instructions is saved only if the skill hasn't changed since this object was
        fetched or last refreshed. Turning the skill on or off applies to its current version. Renaming keeps the
        skill's RID. An archived skill must be restored with :meth:`unarchive` before it can be turned on.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalAlreadyExistsError: If another skill in the workspace has the new name.
            NominalInvalidArgumentError: If a new value breaks the Agent Skills format or a limit.
        """
        skill = skills_pb2.Skill(skill_rid=self.rid, name=name, description=description, instructions=instructions)
        content_paths = [
            path
            for path, value in (("name", name), ("description", description), ("instructions", instructions))
            if value is not None
        ]
        state_paths = []
        if enabled is not None:
            skill.state = skills_pb2.SKILL_STATE_ENABLED if enabled else skills_pb2.SKILL_STATE_DISABLED
            state_paths.append("state")
        if not content_paths and not state_paths:
            return self
        request = skills_pb2.UpdateSkillRequest(
            skill=skill,
            update_mask=field_mask_pb2.FieldMask(paths=content_paths + state_paths),
            # Zero leaves the version unchecked, which the service allows only for a state change.
            expected_version=self.version if content_paths else 0,
        )
        return self._update(request)

    def write_files(self, files: Mapping[str, str]) -> Self:
        """Add or replace reference files in one save, keyed by path relative to the skill's folder.

        Files are UTF-8 text. SKILL.md is not a reference file; change it with :meth:`update`. One call saves up to
        100 files and 3 MiB of text, all or nothing, and only if the skill hasn't changed since this object was
        fetched or last refreshed.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalInvalidArgumentError: If a path or file breaks a limit, or a path is SKILL.md.
        """
        request = skills_pb2.BatchUpdateSkillFilesRequest(
            skill_rid=self.rid,
            expected_version=self.version,
            files=[skills_pb2.SkillFile(path=path, content=content) for path, content in files.items()],
        )
        with _translate_skill_errors():
            response = self._clients.skills.BatchUpdateSkillFiles(request)
        return self._refresh_from_api(response.skill)

    def remove_files(self, paths: Sequence[str]) -> Self:
        """Remove reference files in one save, all or nothing.

        Raises:
            NominalConflictError: If the skill changed since this object was fetched or last refreshed.
            NominalNotFoundError: If the skill has no file at one of the paths. Nothing is removed.
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

    def export(self, path: Path | str) -> Path:
        """Write the skill's folder, named after the skill, under ``path``, and return the folder.

        Refreshes this object first, so the folder matches the skill's current version. Every file is read before
        any is written.

        Raises:
            FileExistsError: If ``path`` already has an entry with the skill's name.
            NominalError: If the skill can't load, or one of its files is not UTF-8 text.
        """
        self.refresh()
        content = self._loaded_content()
        if self.name is None:
            raise NominalError(f"Skill {self.rid} has no valid name for its folder ({self.load_status})")
        folder = Path(path) / self.name
        if folder.exists():
            raise FileExistsError(f"{folder} already exists")
        texts = self._read_files([file.path for file in content.files])
        for file_path, text in texts.items():
            target = folder / file_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(text.encode("utf-8"))
        return folder

    def _loaded_content(self) -> _SkillContent:
        if self._content is None:
            self.refresh()
        if self._content is None:
            raise NominalError(f"Skill {self.rid} can't load ({self.load_status}), so it has no instructions or files")
        return self._content

    def _read_files(self, paths: Sequence[str]) -> dict[str, str]:
        texts: dict[str, str] = {}
        for start in range(0, len(paths), _MAX_PATHS_PER_READ):
            request = skills_pb2.BatchGetSkillFilesRequest(
                skill_rid=self.rid, paths=paths[start : start + _MAX_PATHS_PER_READ]
            )
            with translate_grpc_errors():
                response = self._clients.skills.BatchGetSkillFiles(request)
            if response.not_found_paths:
                raise NominalNotFoundError(f"Skill {self.rid} has no file at {', '.join(response.not_found_paths)}")
            binary = [file.path for file in response.files if not file.HasField("content")]
            if binary:
                raise NominalError(f"Not UTF-8 text, which the client can't read: {', '.join(binary)}")
            texts.update((file.path, file.content) for file in response.files)
        return texts

    def _update(self, request: skills_pb2.UpdateSkillRequest) -> Self:
        with _translate_skill_errors():
            response = self._clients.skills.UpdateSkill(request)
        # The service omits the skill when returning it would disclose a source the caller can no longer read.
        return self._refresh_from_api(response.skill) if response.HasField("skill") else self

    @classmethod
    def _from_proto(cls, clients: _Clients, raw_skill: skills_pb2.Skill) -> Self:
        content = None
        if raw_skill.HasField("instructions"):
            content = _SkillContent(
                instructions=raw_skill.instructions,
                files=tuple(
                    SkillFileInfo(path=file.path, size_bytes=file.size_bytes, sha256=file.sha256)
                    for file in raw_skill.files
                ),
            )
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
            _content=content,
            _clients=clients,
        )


@dataclass(frozen=True)
class _SkillPackage:
    """A skill folder read from disk, split the way the skills service splits SKILL.md."""

    name: str
    description: str
    instructions: str
    frontmatter: dict[str, Any]
    files: dict[str, str]

    @classmethod
    def read(cls, path: Path) -> _SkillPackage:
        """Read a skill folder or .zip with SKILL.md at its top level or in its single top-level folder.

        Hidden files and folders, such as ``.git``, are skipped, as are macOS ``__MACOSX`` folders.
        """
        texts = _package_texts(path)
        entry = texts.pop(_ENTRY_FILE, None)
        if entry is None:
            raise ValueError(f"{path} has no SKILL.md at its top level or in a single top-level folder")
        split = _split_frontmatter(entry.removeprefix("﻿"))
        if split is None:
            raise ValueError("SKILL.md must start with YAML frontmatter between --- lines")
        try:
            frontmatter = yaml.load(split[0], Loader=_FrontmatterLoader)
        except yaml.YAMLError as e:
            raise ValueError(f"SKILL.md frontmatter is not valid YAML: {e}") from e
        if not isinstance(frontmatter, dict):
            raise ValueError("SKILL.md frontmatter must be a mapping with a name and a description")
        name = frontmatter.pop("name", None)
        description = frontmatter.pop("description", None)
        if not isinstance(name, str) or not isinstance(description, str):
            raise ValueError("SKILL.md frontmatter needs a name and a description")
        return cls(name=name, description=description, instructions=split[1], frontmatter=frontmatter, files=texts)


class _FrontmatterLoader(yaml.SafeLoader):
    """Safe YAML that reads dates and times as strings, as the skills service does."""


_FrontmatterLoader.yaml_implicit_resolvers = {
    first: [(tag, pattern) for tag, pattern in resolvers if tag != "tag:yaml.org,2002:timestamp"]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def _package_texts(path: Path) -> dict[str, str]:
    if path.is_dir():
        contents = {
            file.relative_to(path).as_posix(): file.read_bytes()
            for file in sorted(path.rglob("*"))
            if file.is_file() and not _is_skipped(file.relative_to(path).as_posix())
        }
    else:
        with zipfile.ZipFile(path) as archive:
            contents = {
                info.filename: archive.read(info)
                for info in archive.infolist()
                if not info.is_dir() and not _is_skipped(info.filename)
            }
    if _ENTRY_FILE not in contents:
        folders = {name.split("/", 1)[0] for name in contents}
        if len(folders) == 1 and f"{next(iter(folders))}/{_ENTRY_FILE}" in contents:
            prefix = f"{next(iter(folders))}/"
            contents = {name.removeprefix(prefix): data for name, data in contents.items()}
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


def _is_skipped(path: str) -> bool:
    return any(part.startswith(".") or part == "__MACOSX" for part in path.split("/"))


def _split_frontmatter(markdown: str) -> tuple[str, str] | None:
    """Split SKILL.md into its frontmatter and body, or None when it doesn't start with a frontmatter block."""
    lines = markdown.split("\n")
    if not _FRONTMATTER_DELIMITER.fullmatch(lines[0]):
        return None
    for index in range(1, len(lines)):
        if _FRONTMATTER_DELIMITER.fullmatch(lines[index]):
            return "\n".join(lines[1:index]), "\n".join(lines[index + 1 :])
    return None


def _state_update(skill_rid: str, state: skills_pb2.SkillState.ValueType) -> skills_pb2.UpdateSkillRequest:
    return skills_pb2.UpdateSkillRequest(
        skill=skills_pb2.Skill(skill_rid=skill_rid, state=state),
        update_mask=field_mask_pb2.FieldMask(paths=["state"]),
    )


def _searched_states(
    enabled: bool | None, archive_status: ArchiveStatusFilter
) -> list[skills_pb2.SkillState.ValueType]:
    """The skill states a search selects; empty when the filters exclude every skill."""
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
