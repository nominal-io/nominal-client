from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from nominal.core import ArchiveStatusFilter, NominalClient, Workspace
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.experimental.analyst._skill import Skill, _create_skill, _get_skill, _import_skill, _iter_search_skills


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
    so the files stay and the error's notes say so. If the skill can't be turned on, it stays as a draft
    and the error's notes give its RID.

    Args:
        client: Authenticated Nominal client.
        content: The skill's SKILL.md, frontmatter included. Its frontmatter names and describes the skill.
        files: Reference files as UTF-8 text, keyed by path relative to the skill's folder, such as
            ``methods/limits.md``.
        enabled: Whether the skill starts turned on. Defaults to True.
        workspace: Workspace or workspace RID for the skill. Defaults to the client's default workspace.

    Raises:
        ValueError: If SKILL.md has no frontmatter with a valid name, `enabled` is True and SKILL.md has no
            description of 1 to 1024 characters, a file's path is SKILL.md or leads outside the skill's folder,
            or there are more than 99 reference files. Nothing is saved.
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
            no description of 1 to 1024 characters. Nothing is saved.
        NominalPermissionDeniedError: If the user can't edit the workspace's configuration, which creating a
            skill needs. Nothing is saved.
        NominalAlreadyExistsError: If the workspace already has a skill with this name. Nothing is saved.
    """
    return _import_skill(
        client._clients,
        Path(path),
        enabled=enabled,
        workspace_rid=client._clients.resolve_default_workspace_rid()
        if workspace is None
        else rid_from_instance_or_string(workspace),
    )


def get_skill(client: NominalClient, rid: str) -> Skill:
    """Retrieve a skill by RID.

    Raises:
        NominalNotFoundError: If no skill with the given RID exists or it is not accessible to the user.
    """
    return _get_skill(client._clients, rid)


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
    return list(
        _iter_search_skills(
            client._clients,
            search_text=search_text,
            enabled=enabled,
            archive_status=archive_status,
            workspace_rid=client._clients.resolve_default_workspace_rid()
            if workspace is None
            else rid_from_instance_or_string(workspace),
        )
    )
