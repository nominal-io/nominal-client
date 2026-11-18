---
myst:
  html_meta:
    description: "Find, read, create, import, edit and export the skills Analyst and connected agents follow."
---

# Skills

{.lead}
A skill is a folder with a SKILL.md of instructions, and any reference files, that Analyst and connected agents follow for a repeatable task, such as a post-test review.
Skills use the [Agent Skills](https://agentskills.io/specification) format, and each workspace has a skills library that every Analyst user and connected agent in the workspace shares.
Each skill's files are in the folder `skills/<name>` of the workspace's skills drive.
Creating and changing skills needs permission to edit the workspace's configuration.
Without it, the methods that save raise {py:class}`~nominal.exceptions.NominalPermissionDeniedError` before saving anything.

```{include} /guides/_snippets/connect-the-client.md
```

These APIs are experimental and may change between releases.

```python
from nominal.experimental import analyst
```

## Find and read skills

{py:func}`~nominal.experimental.analyst.search_skills` finds skills whose name or description contains your text, ignoring case, in name order.
Without text, it lists every skill:

```python
skills = analyst.search_skills(client, "review")
for skill in skills:
    print(skill.name, skill.is_enabled, skill.get_load_status())
```

{py:meth}`~nominal.experimental.analyst.Skill.get_load_status` checks SKILL.md and returns a {py:class}`~nominal.experimental.analyst.SkillLoadStatus`, such as `AVAILABLE`, `INVALID`, or `MISSING`.
Pass `enabled=True` for only the skills that are turned on, or `archive_status` to include archived skills.

Read a skill's SKILL.md, and its reference files:

```python
skill = analyst.get_skill(client, "<SKILL_RID>")
print(skill.get_content())
for file in skill.list_files():
    print(file.path, file.size_bytes)
print(skill.read_file("methods/limits.md"))
```

## Create or import a skill

{py:func}`~nominal.experimental.analyst.create_skill` saves a skill from its SKILL.md and any reference files, keyed by path.
SKILL.md's frontmatter names and describes the skill:

```python
skill_md = """---
name: post-test-review
description: Review a test run against its test plan and flag anomalies.
---

1. Read the test plan.
2. Compare each channel against its limits in methods/limits.md.
"""
skill = analyst.create_skill(
    client,
    skill_md,
    files={"methods/limits.md": "Use the limits in the test plan's appendix.\n"},
)
```

Names use lowercase letters, digits and single hyphens, are at most 64 characters long, and must be unused in the workspace.
Skills hold UTF-8 text files only.
Pass `enabled=False` to save a skill without turning it on.

The files are saved in one batch, then the skill is registered and enabled.
If registration or enabling fails, read the error and its notes before retrying. A timeout can leave the outcome unknown: the files may remain, or the skill may already exist.
When the notes give a skill RID, fetch it with `analyst.get_skill(client, rid)` to check its state.

{py:func}`~nominal.experimental.analyst.import_skill` creates a skill from a folder with SKILL.md at its top level, or a .zip holding one:

```python
skill = analyst.import_skill(client, "skills/post-test-review")
```

Import reads every file before saving any, and skips paths with a part that starts with ".", such as `.git`. Folder imports also skip symbolic links.

## Edit a skill

{py:meth}`~nominal.experimental.analyst.Skill.update` replaces SKILL.md and turns the skill on or off.
A new name in SKILL.md's frontmatter renames the skill and moves its folder.
{py:meth}`~nominal.experimental.analyst.Skill.write_files` adds or replaces reference files, and {py:meth}`~nominal.experimental.analyst.Skill.remove_files` removes them:

```python
skill.update(content=skill_md.replace("flag anomalies", "flag and summarize anomalies"))
skill.write_files({"templates/summary.md": "## Summary\n"})
skill.remove_files(["methods/limits.md"])
```

`update` saves only if the skill hasn't changed since you fetched it, and saves SKILL.md and a rename's folder move together or not at all.
Edits made to the skill's files in Drives don't count as a change to the skill, so read SKILL.md with {py:meth}`~nominal.experimental.analyst.Skill.get_content` before replacing it.
Each call to `write_files` or `remove_files` saves its files together or not at all, replacing each file as it is when you call it, and is refused if another save changes one of them at the same moment.
Both raise {py:class}`~nominal.exceptions.NominalConflictError` and save nothing.
Refresh the skill, read SKILL.md again, and apply your change to what you read, so you keep the other change:

```python
from nominal.exceptions import NominalConflictError


def summarize_anomalies(skill_md: str) -> str:
    return skill_md.replace("flag anomalies", "flag and summarize anomalies")


try:
    skill.update(content=summarize_anomalies(skill.get_content()))
except NominalConflictError:
    skill.refresh()
    skill.update(content=summarize_anomalies(skill.get_content()))
```

`update` saves the files before updating the skill record. If the second step fails, it raises {py:class}`~nominal.exceptions.NominalError` and reports that SKILL.md was saved. Fetch the skill and read its content before retrying; the conflict-retry example above does not handle this partial-success case.

## Archive and restore a skill

{py:meth}`~nominal.experimental.analyst.Skill.archive` hides a skill from the library, Analyst and connected agents.
{py:meth}`~nominal.experimental.analyst.Skill.unarchive` restores it turned off:

```python
skill.archive()
skill.unarchive()
skill.update(enabled=True)
```

## Export a skill

{py:meth}`~nominal.experimental.analyst.Skill.export` writes the skill's folder, named after the skill, and returns its path:

```python
folder = skill.export("exported-skills")
```

The folder holds SKILL.md and every reference file as stored, so you can keep it in git or import it into another workspace.
Export creates a new folder named after the skill inside the supplied path and refuses to overwrite an existing folder.
