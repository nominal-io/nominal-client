from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from nominal.core import NominalClient
from nominal.experimental import analyst
from nominal.experimental.analyst import SkillLoadStatus


def _skill_md(name: str, description: str) -> str:
    return f"---\nname: {name}\ndescription: {description}\n---\n\nRead the run's summary, then check each channel.\n"


def test_skill_folder_round_trip(client: NominalClient, archive, tmp_path: Path) -> None:
    """A skill saves its folder in the skills drive, edits and renames it there, and exports what it holds."""
    name = f"sdk-e2e-{uuid4().hex[:8]}"
    # A draft never reaches the workspace's Analyst users.
    skill = analyst.create_skill(
        client, _skill_md(name, "Review an SDK test run."), files={"methods/a.md": "Method A\n"}, enabled=False
    )
    archive(skill)

    assert (skill.name, skill.is_enabled) == (name, False)
    assert [file.path for file in skill.list_files()] == ["SKILL.md", "methods/a.md"]
    assert skill.read_file("methods/a.md") == "Method A\n"
    assert skill.get_load_status() is SkillLoadStatus.AVAILABLE

    skill.write_files({"methods/a.md": "Method A, revised\n", "templates/b.md": "B\n"})
    skill.remove_files(["templates/b.md"])
    assert [file.path for file in skill.list_files()] == ["SKILL.md", "methods/a.md"]
    assert skill.read_file("methods/a.md") == "Method A, revised\n"

    renamed = f"{name}-renamed"
    skill.update(content=_skill_md(renamed, "Review an SDK test run, renamed."))
    assert (skill.name, skill.description) == (renamed, "Review an SDK test run, renamed.")
    assert skill.read_file("methods/a.md") == "Method A, revised\n"
    assert skill.rid in {found.rid for found in analyst.search_skills(client, renamed, enabled=False)}

    folder = skill.export(tmp_path)
    assert folder.name == renamed
    assert (folder / "methods" / "a.md").read_text() == "Method A, revised\n"


def test_enabled_skill_lifecycle(client: NominalClient, archive) -> None:
    """A newly enabled skill loads and can be disabled before teardown archives it."""
    name = f"sdk-e2e-{uuid4().hex[:8]}"
    skill = analyst.create_skill(client, _skill_md(name, "Check an SDK test skill."))
    archive(skill)
    assert skill.is_enabled
    assert skill.get_load_status() is SkillLoadStatus.AVAILABLE
    skill.update(enabled=False)
    assert not analyst.get_skill(client, skill.rid).is_enabled
