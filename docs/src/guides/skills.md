---
myst:
  html_meta:
    description: "Find, read, create, import, edit and export the skills Analyst and connected agents follow."
---

# Skills

{.lead}
A skill is a folder with a SKILL.md of instructions, and any reference files, that Analyst and connected agents follow for a repeatable task, such as a post-test review.
Skills use the [Agent Skills](https://agentskills.io/specification) format, and each workspace has a skills library that every Analyst user and connected agent in the workspace shares.

```{include} /guides/_snippets/connect-the-client.md
```

## Find and read skills

{py:meth}`~nominal.core.NominalClient.search_skills` ranks skills by how well their names and descriptions match your words, best match first.
Without words, it lists every skill by name:

```python
skills = client.search_skills("post-test review")
for skill in skills:
    print(skill.name, skill.is_enabled, skill.load_status)
```

{py:attr}`~nominal.core.Skill.load_status` says whether Analyst can load the skill, and why not when it can't.
Pass `enabled=True` for only the skills that are turned on, or `archive_status` to include archived skills.

Read a skill's files, SKILL.md included:

```python
skill = client.get_skill("<SKILL_RID>")
for file in skill.list_files():
    print(file.path, file.size_bytes)
print(skill.read_file("SKILL.md"))
```

## Create or import a skill

{py:meth}`~nominal.core.NominalClient.create_skill` saves a skill from its SKILL.md and any reference files, keyed by path.
SKILL.md's frontmatter names and describes the skill:

```python
skill_md = """---
name: post-test-review
description: Review a test run against its test plan and flag anomalies.
---

1. Read the test plan.
2. Compare each channel against its limits in methods/limits.md.
"""
skill = client.create_skill(
    skill_md,
    files={"methods/limits.md": "Use the limits in the test plan's appendix.\n"},
)
```

Names use lowercase letters, digits and hyphens, and must be unused in the workspace.
Skills hold UTF-8 text files only.
Pass `enabled=False` to save a skill without turning it on.

{py:meth}`~nominal.core.NominalClient.import_skill` creates a skill from a folder with SKILL.md at its top level, or a .zip holding one:

```python
skill = client.import_skill("skills/post-test-review")
```

Import reads every file before saving any, and skips paths with a part that starts with ".", such as `.git`.

## Edit a skill

{py:meth}`~nominal.core.Skill.update` replaces SKILL.md and turns the skill on or off.
A new name in SKILL.md's frontmatter renames the skill.
{py:meth}`~nominal.core.Skill.write_files` adds or replaces reference files, and {py:meth}`~nominal.core.Skill.remove_files` removes them.
Each saves all or nothing:

```python
skill.update(content=skill_md.replace("flag anomalies", "flag and summarize anomalies"))
skill.write_files({"templates/summary.md": "## Summary\n"})
skill.remove_files(["methods/limits.md"])
```

These save only if the skill hasn't changed since you fetched it, including edits made to its folder in Drives.
When it has, they raise {py:class}`~nominal.exceptions.NominalConflictError`; refresh the skill and apply your change again:

```python
from nominal.exceptions import NominalConflictError

try:
    skill.update(content=new_skill_md)
except NominalConflictError:
    skill.refresh()
    skill.update(content=new_skill_md)
```

Turning a skill on or off applies to its current version, without that check.

## Archive and restore a skill

{py:meth}`~nominal.core.Skill.archive` hides a skill from the library, Analyst and connected agents.
{py:meth}`~nominal.core.Skill.unarchive` restores it turned off:

```python
skill.archive()
skill.unarchive()
skill.update(enabled=True)
```

## Export a skill

{py:meth}`~nominal.core.Skill.export` writes the skill's folder, named after the skill, and returns its path:

```python
folder = skill.export("exported-skills")
```

The folder holds SKILL.md as stored and every reference file, so you can keep it in git or import it into another workspace.
