from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core import ArchiveStatusFilter, SkillLoadStatus
from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.skill import Skill
from nominal.exceptions import NominalConflictError, NominalError
from nominal.protos.file_store.v1 import file_store_pb2
from nominal.protos.skills.v1 import skills_pb2

_RID = "ri.file-store.main.skill.0b8f6c1e-0d3c-4c43-9a55-6f0d3c1e2b7a"
_WORKSPACE = "ri.workspace.main.workspace.1"


def _proto_skill(**kwargs) -> skills_pb2.Skill:
    defaults = dict(
        skill_rid=_RID,
        workspace_rid=_WORKSPACE,
        state=skills_pb2.SKILL_STATE_ENABLED,
        version=7,
        name="post-test-review",
        description="Review a test run.",
        created=file_store_pb2.Attribution(),
        updated=file_store_pb2.Attribution(),
    )
    return skills_pb2.Skill(**{**defaults, **kwargs})


def _with_content(**kwargs) -> skills_pb2.Skill:
    return _proto_skill(
        instructions="Read the run.",
        files=[skills_pb2.SkillFileInfo(path="SKILL.md"), skills_pb2.SkillFileInfo(path="methods/a.md")],
        backing_status=skills_pb2.SKILL_BACKING_STATUS_AVAILABLE,
        **kwargs,
    )


def _client() -> tuple[NominalClient, MagicMock]:
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE
    return NominalClient(_clients=clients), clients


def test_unnamed_skill_converts_absent_fields_to_none() -> None:
    """A skill whose SKILL.md has no valid frontmatter has no name or description, not empty ones."""
    skill = Skill._from_proto(
        MagicMock(),
        skills_pb2.Skill(
            skill_rid=_RID,
            state=skills_pb2.SKILL_STATE_DRAFT,
            backing_status=skills_pb2.SKILL_BACKING_STATUS_INVALID,
        ),
    )

    assert skill.name is None
    assert skill.description is None
    assert not skill.is_enabled and not skill.is_archived
    assert skill.load_status is SkillLoadStatus.INVALID


def test_content_update_checks_the_version_and_state_update_does_not() -> None:
    """New instructions are saved against this object's version; turning a skill off applies to the current one."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(skill=_proto_skill(version=8))

    skill.update(instructions="New steps.")
    content_request = clients.skills.UpdateSkill.call_args.args[0]
    skill.update(enabled=False)
    state_request = clients.skills.UpdateSkill.call_args.args[0]

    assert list(content_request.update_mask.paths) == ["instructions"]
    assert content_request.expected_version == 7
    assert not content_request.skill.HasField("name")
    assert list(state_request.update_mask.paths) == ["state"]
    assert state_request.skill.state == skills_pb2.SKILL_STATE_DISABLED
    assert state_request.expected_version == 0
    assert skill.version == 8


def test_update_without_changes_sends_nothing() -> None:
    """An update with every field left as None makes no call."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())

    assert skill.update() is skill
    clients.skills.UpdateSkill.assert_not_called()


def test_stale_skill_write_raises_conflict(fake_rpc_error) -> None:
    """The skills service's stale-version error surfaces as NominalConflictError."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.BatchUpdateSkillFiles.side_effect = fake_rpc_error(grpc.StatusCode.ABORTED)

    with pytest.raises(NominalConflictError):
        skill.write_files({"methods/a.md": "text"})


def test_aborted_errors_from_other_services_are_unchanged(fake_rpc_error) -> None:
    """Only skill methods raise NominalConflictError; other calls keep raising the base NominalError."""
    client, clients = _client()
    clients.secrets.Get.side_effect = fake_rpc_error(grpc.StatusCode.ABORTED)

    with pytest.raises(NominalError) as raised:
        client.get_secret("ri.secret.test")

    assert type(raised.value) is NominalError


def test_get_instructions_fetches_content_for_a_search_result() -> None:
    """A search result has no instructions, so reading them fetches the skill and refreshes the same object."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_with_content(version=9))

    assert skill.get_instructions() == "Read the run."

    assert clients.skills.GetSkill.call_args.args[0].include_content
    assert skill.version == 9
    assert [file.path for file in skill.list_files()] == ["SKILL.md", "methods/a.md"]
    assert clients.skills.GetSkill.call_count == 1


def test_read_file_refuses_a_file_that_is_not_text() -> None:
    """The client reads text only; a binary file in the skill's folder raises an error naming it."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.BatchGetSkillFiles.return_value = skills_pb2.BatchGetSkillFilesResponse(
        files=[skills_pb2.SkillFile(path="diagram.png", size_bytes=10)]
    )

    with pytest.raises(NominalError, match="diagram.png"):
        skill.read_file("diagram.png")


def test_unarchive_restores_turned_off_and_skips_unarchived_skills() -> None:
    """Restoring brings an archived skill back turned off; a skill shown as unarchived is left alone."""
    clients = MagicMock()
    archived = Skill._from_proto(clients, _proto_skill(state=skills_pb2.SKILL_STATE_ARCHIVED))
    enabled = Skill._from_proto(clients, _proto_skill())
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(
        skill=_proto_skill(state=skills_pb2.SKILL_STATE_DISABLED)
    )

    archived.unarchive()
    enabled.unarchive()

    assert clients.skills.UpdateSkill.call_count == 1
    assert clients.skills.UpdateSkill.call_args.args[0].skill.state == skills_pb2.SKILL_STATE_DISABLED
    assert not archived.is_archived and not archived.is_enabled


@pytest.mark.parametrize(
    ("enabled", "archive_status", "states"),
    [
        (
            None,
            ArchiveStatusFilter.NOT_ARCHIVED,
            [skills_pb2.SKILL_STATE_DRAFT, skills_pb2.SKILL_STATE_ENABLED, skills_pb2.SKILL_STATE_DISABLED],
        ),
        (True, ArchiveStatusFilter.ANY, [skills_pb2.SKILL_STATE_ENABLED]),
        (
            False,
            ArchiveStatusFilter.ANY,
            [skills_pb2.SKILL_STATE_DRAFT, skills_pb2.SKILL_STATE_DISABLED, skills_pb2.SKILL_STATE_ARCHIVED],
        ),
        (None, ArchiveStatusFilter.ARCHIVED, [skills_pb2.SKILL_STATE_ARCHIVED]),
    ],
)
def test_search_selects_states_from_filters(enabled, archive_status, states) -> None:
    """The enabled and archive filters select the skill states the service searches."""
    client, clients = _client()
    clients.skills.SearchSkills.return_value = skills_pb2.SearchSkillsResponse()

    client.search_skills(enabled=enabled, archive_status=archive_status)

    assert list(clients.skills.SearchSkills.call_args.args[0].filter.states) == states


def test_search_with_no_selectable_state_makes_no_call() -> None:
    """Asking for enabled skills among archived ones matches nothing, so no request is sent."""
    client, clients = _client()

    assert client.search_skills(enabled=True, archive_status=ArchiveStatusFilter.ARCHIVED) == []
    clients.skills.SearchSkills.assert_not_called()


def test_search_follows_pages_ranks_and_asks_for_status() -> None:
    """Search returns every page, ranked by relevance, with each skill's load status."""
    client, clients = _client()
    clients.skills.SearchSkills.side_effect = [
        skills_pb2.SearchSkillsResponse(
            skills=[_proto_skill(backing_status=skills_pb2.SKILL_BACKING_STATUS_MISSING)], next_page_token="next"
        ),
        skills_pb2.SearchSkillsResponse(skills=[_proto_skill(skill_rid="ri.file-store.main.skill.b")]),
    ]

    results = client.search_skills("review")

    first, second = (call.args[0] for call in clients.skills.SearchSkills.call_args_list)
    assert [skill.rid for skill in results] == [_RID, "ri.file-store.main.skill.b"]
    assert results[0].load_status is SkillLoadStatus.MISSING
    assert first.filter.text == "review"
    assert first.sort == skills_pb2.SKILL_LIBRARY_SORT_RELEVANCE
    assert first.include_status
    assert first.workspace_rid == _WORKSPACE
    assert second.page_token == "next"


def test_search_refuses_all_workspaces() -> None:
    """The service searches one workspace at a time."""
    client, _ = _client()

    with pytest.raises(ValueError):
        client.search_skills(workspace=WorkspaceSearchType.ALL)


def _write_skill_folder(folder: Path) -> None:
    (folder / "methods").mkdir(parents=True)
    (folder / ".git").mkdir()
    (folder / ".git" / "HEAD").write_bytes(b"\xff\xfe")
    (folder / "SKILL.md").write_text(
        "---\nname: post-test-review\ndescription: Review a test run.\nlicense: MIT\nreviewed: 2026-10-01\n---\n\n"
        "Read the run.\n"
    )
    (folder / "methods" / "a.md").write_text("Method A.\n")


def test_import_splits_skill_md_and_skips_hidden_files(tmp_path: Path) -> None:
    """Importing a folder sends SKILL.md's parts, dates kept as text, and the text files outside hidden folders."""
    client, clients = _client()
    _write_skill_folder(tmp_path / "post-test-review")
    clients.skills.CreateSkill.return_value = skills_pb2.CreateSkillResponse(skill=_proto_skill())

    client.import_skill(tmp_path / "post-test-review", enabled=False)

    request = clients.skills.CreateSkill.call_args.args[0]
    assert request.skill.name == "post-test-review"
    assert request.skill.description == "Review a test run."
    assert request.skill.instructions == "\nRead the run.\n"
    assert dict(request.skill.frontmatter) == {"license": "MIT", "reviewed": "2026-10-01"}
    assert request.skill.state == skills_pb2.SKILL_STATE_DRAFT
    assert [(file.path, file.content) for file in request.files] == [("methods/a.md", "Method A.\n")]


def test_import_reads_a_zip_with_one_top_level_folder(tmp_path: Path) -> None:
    """A .zip whose files sit in one folder imports as if that folder were given."""
    client, clients = _client()
    _write_skill_folder(tmp_path / "post-test-review")
    archive_path = tmp_path / "skill.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for file in (tmp_path / "post-test-review").rglob("*"):
            if file.is_file():
                archive.write(file, file.relative_to(tmp_path).as_posix())
    clients.skills.CreateSkill.return_value = skills_pb2.CreateSkillResponse(skill=_proto_skill())

    client.import_skill(archive_path)

    request = clients.skills.CreateSkill.call_args.args[0]
    assert request.skill.name == "post-test-review"
    assert [file.path for file in request.files] == ["methods/a.md"]


def test_import_refuses_binary_files_before_saving(tmp_path: Path) -> None:
    """A file that isn't UTF-8 text stops the import before anything is sent."""
    client, clients = _client()
    _write_skill_folder(tmp_path)
    (tmp_path / "diagram.png").write_bytes(b"\x89PNG\r\n\x1a\n\xff")

    with pytest.raises(ValueError, match="diagram.png"):
        client.import_skill(tmp_path)
    clients.skills.CreateSkill.assert_not_called()


def test_export_writes_every_file_under_the_skill_name(tmp_path: Path) -> None:
    """Export writes the current files, SKILL.md included, into a folder named after the skill."""
    clients = MagicMock()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_with_content())
    clients.skills.BatchGetSkillFiles.return_value = skills_pb2.BatchGetSkillFilesResponse(
        files=[
            skills_pb2.SkillFile(path="SKILL.md", content="---\nname: post-test-review\n---\n"),
            skills_pb2.SkillFile(path="methods/a.md", content="Method A.\r\n"),
        ]
    )

    folder = skill.export(tmp_path)

    assert folder == tmp_path / "post-test-review"
    assert (folder / "SKILL.md").read_text() == "---\nname: post-test-review\n---\n"
    assert (folder / "methods" / "a.md").read_bytes() == b"Method A.\r\n"
    with pytest.raises(FileExistsError):
        skill.export(tmp_path)


def test_nominal_url_uses_the_apps_skill_reference() -> None:
    """The link encodes the skill the way the app's skill page expects, under the skill's own workspace."""
    clients = MagicMock()
    clients.app_base_url = "https://app.example.com/"
    skill = Skill._from_proto(clients, _proto_skill())

    assert skill.nominal_url == (
        "https://app.example.com/w/ri.workspace.main.workspace.1/analyst/skills/skill%3Av2%3AeyJza2lsbFJpZCI6InJpLm"
        "ZpbGUtc3RvcmUubWFpbi5za2lsbC4wYjhmNmMxZS0wZDNjLTRjNDMtOWE1NS02ZjBkM2MxZTJiN2EiLCJzb3VyY2UiOiJyZWNvcmQiLC"
        "J2IjoyfQ"
    )
