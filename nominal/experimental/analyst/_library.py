from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from nominal.core import ArchiveStatusFilter, NominalClient, Workspace
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, paginate_grpc
from nominal.experimental.analyst._skill import _ENTRY_FILE, Skill, _create_skill, _read_skill_folder
from nominal.protos.skills.v1 import skills_pb2


def create_skill(
    client: NominalClient,
    content: str,
    *,
    files: Mapping[str, str] | None = None,
    enabled: bool = True,
    workspace: Workspace | str | None = None,
) -> Skill:
    """Create a skill from its SKILL.md and any reference files.

    The files are saved as written to ``skills/<name>`` in the workspace's skills drive, where `name` is the
    name in SKILL.md's frontmatter, and the skill is registered from that SKILL.md. An enabled skill reaches
    every Analyst user and connected agent in the workspace.

    If the skills service refuses the skill, the files this call saved are removed; if they can't be removed,
    the error's notes say where they are. After a timeout or outage the registration outcome may be unknown,
    so the files stay and the error's notes say so. If enabling the skill fails, the error's notes give its RID.
    Call `get_skill` to check its current state before retrying.

    Args:
        client: Authenticated Nominal client.
        content: The skill's SKILL.md, frontmatter included. Its frontmatter names and describes the skill.
        files: Reference files as UTF-8 text, keyed by path relative to the skill's folder, such as
            ``methods/limits.md``.
        enabled: Whether the skill starts turned on. Defaults to True.
        workspace: Workspace or workspace RID for the skill. Defaults to the client's default workspace.

    Raises:
        ValueError: If SKILL.md has no frontmatter with a valid name, `enabled` is True and SKILL.md has no
            description of 1 to 1024 UTF-16 code units, a file's path is SKILL.md or leads outside the skill's folder,
            or there are more than 99 reference files. Enabled entries must fit in 64 KiB and
            provide valid optional frontmatter fields. Nothing is saved.
        NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which creating a
            skill needs. Nothing is saved.
        NominalAlreadyExistsError: If the workspace already has a skill with this name. Nothing is saved.
    """
    return _create_skill(
        client._clients,
        content=content,
        files=files,
        enabled=enabled,
        workspace_rid=client._clients.resolve_default_workspace_rid()
        if workspace is None
        else rid_from_instance_or_string(workspace),
    )


def import_skill(
    client: NominalClient,
    path: PathLike,
    *,
    enabled: bool = True,
    workspace: Workspace | str | None = None,
) -> Skill:
    """Create a skill from a folder with SKILL.md at its top level, or a .zip holding one such folder.

    Files are read the way the app's Import Skills reads them, skipping paths with a part that starts with ".",
    such as ``.git``. Folder imports also skip symbolic links and files reached through them.
    Every file is read before anything is saved.

    Args:
        client: Authenticated Nominal client.
        path: The skill's folder, or a .zip of it.
        enabled: Whether the skill starts turned on. Defaults to True.
        workspace: Workspace or workspace RID for the skill. Defaults to the client's default workspace.

    The skill is created as :func:`create_skill` creates it.

    Raises:
        ValueError: If the folder or .zip has no SKILL.md, the .zip holds more than one skill folder, a file is
            not UTF-8 text, SKILL.md has no frontmatter with a valid name, or `enabled` is True and SKILL.md has
            no description of 1 to 1024 UTF-16 code units. Nothing is saved.
        NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which creating a
            skill needs. Nothing is saved.
        NominalAlreadyExistsError: If the workspace already has a skill with this name. Nothing is saved.
    """
    files = _read_skill_folder(Path(path))
    content = files.pop(_ENTRY_FILE)
    return create_skill(client, content, files=files, enabled=enabled, workspace=workspace)


def get_skill(client: NominalClient, rid: str) -> Skill:
    """Retrieve a skill by RID.

    Raises:
        NominalNotFoundError: If no skill with the given RID exists or it is not accessible to the user.
    """
    with translate_grpc_errors():
        response = client._clients.skills.GetSkill(skills_pb2.GetSkillRequest(skill_rid=rid))
    return Skill._from_proto(client._clients, response.skill)


def search_skills(
    client: NominalClient,
    search_text: str | None = None,
    *,
    enabled: bool | None = None,
    workspace: Workspace | str | None = None,
    archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
) -> Sequence[Skill]:
    """Search a workspace's skills by name and description, in name order.

    Args:
        client: Authenticated Nominal client.
        search_text: Text that a skill's name or description contains, ignoring case. Omit to list every
            skill.
        enabled: True for only skills that are turned on, False for only skills that are off. Omit for both.
        workspace: Workspace or workspace RID to search. Defaults to the client's default workspace.
        archive_status: Filter by archive status. Defaults to NOT_ARCHIVED.

    Returns:
        All skills matching all of the provided filters.
    """
    workspace_rid = (
        client._clients.resolve_default_workspace_rid() if workspace is None else rid_from_instance_or_string(workspace)
    )
    states = _searched_states(enabled, archive_status)
    # Empty states means all unarchived states to Scout.
    if not states:
        return []
    search_filter = skills_pb2.SkillSearchFilter(text=search_text, states=states)

    def factory(page_token: str | None) -> skills_pb2.SearchSkillsRequest:
        return skills_pb2.SearchSkillsRequest(
            workspace_rid=workspace_rid,
            filter=search_filter,
            sort=skills_pb2.SKILL_LIBRARY_SORT_NAME_ASC,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    return [
        Skill._from_proto(client._clients, skill)
        for page in paginate_grpc(client._clients.skills.SearchSkills, request_factory=factory)
        for skill in page.skills
    ]


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
