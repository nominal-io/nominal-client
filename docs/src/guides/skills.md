---
myst:
  html_meta:
    description: "Find, create, edit and share reusable instructions for Analyst with the Nominal Python Client."
---

# Analyst skills

{.lead}
An Analyst skill holds your team's instructions for a repeatable task, such as a post-test review. Analyst (Nominal's AI assistant) and connected agents follow the skills in their workspace.
Creating or changing a skill needs permission to edit the workspace's configuration.

```{include} /guides/_snippets/connect-the-client.md
```

Import the experimental Analyst module. Its interfaces may change between releases:

```python
from nominal.experimental import analyst
```

## Find and read skills

Use {py:func}`~nominal.experimental.analyst.search_skills` to find skills by name or description:

```python
skills = analyst.search_skills(client, "review")  # Omit the text to list all unarchived skills.
for skill in skills:
    # Load status is an enum such as AVAILABLE, INVALID, or MISSING.
    print(skill.name, skill.is_enabled, skill.get_load_status())
```

Read a skill's SKILL.md and its reference files:

```python
skill = analyst.get_skill(client, "<SKILL_RID>")  # Or use a skill returned by search_skills.
# The current SKILL.md.
print(skill.get_content())
# Or read all skill files.
for file in skill.list_files():
    print(skill.read_file(file.path))
```

## Create or import a skill

Use {py:func}`~nominal.experimental.analyst.create_skill` to save instructions and reference files.
SKILL.md follows the [Agent Skills](https://agentskills.io/specification) format, with a name and description in its YAML frontmatter.
A new skill is turned on, and reaches every Analyst user and connected agent in the workspace. Pass `enabled=False` to keep it off while you prepare it:

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
    # Reference files are keyed by path within the skill's folder.
    files={"methods/limits.md": "Use the limits in the test plan's appendix.\n"},
    enabled=False,
)
```

Choose a unique name in the workspace using lowercase letters, digits and single hyphens.
See {py:func}`~nominal.experimental.analyst.create_skill` for validation rules.

If you already have a folder with SKILL.md at its top level, or a .zip containing one, use {py:func}`~nominal.experimental.analyst.import_skill` instead:

```python
skill = analyst.import_skill(client, "skills/post-test-review", enabled=False)
```

Import skips dot-prefixed files and folders, such as `.git`. Every other file must be text, or the import stops without saving anything.

## Edit a skill

Use {py:meth}`~nominal.experimental.analyst.Skill.update` to replace SKILL.md or turn a skill on or off.
Changing the name in its frontmatter renames the skill.
Use {py:meth}`~nominal.experimental.analyst.Skill.write_files` to add or replace reference files, and {py:meth}`~nominal.experimental.analyst.Skill.remove_files` to remove them:

```python
content = skill.get_content()  # Start from the saved instructions, including edits made outside this script.
skill.update(content=content.replace("flag anomalies", "flag and summarize anomalies"))
skill.write_files({"templates/summary.md": "## Summary\n"})
skill.remove_files(["templates/summary.md"])
skill.update(enabled=True)  # Make the prepared skill available to Analyst.
```

If the skill has changed since you fetched it, `update` raises {py:class}`~nominal.exceptions.NominalConflictError` without saving your edit.
Call `skill.refresh()`, read `skill.get_content()` again, and reapply your edit to that content before retrying.

## Archive and restore a skill

Use {py:meth}`~nominal.experimental.analyst.Skill.archive` to hide a skill from the skills library, Analyst and connected agents.
{py:meth}`~nominal.experimental.analyst.Skill.unarchive` restores it and leaves it turned off, so turn it on again to use it:

```python
skill.archive()
skill.unarchive()
skill.update(enabled=True)
```

To find an archived skill, include archived results in your search:

```python
from nominal.core import ArchiveStatusFilter

archived_skills = analyst.search_skills(client, archive_status=ArchiveStatusFilter.ARCHIVED)
```

## Export a skill

Use {py:meth}`~nominal.experimental.analyst.Skill.export` to save a local copy of SKILL.md and its reference files.
Export writes a new folder named after the skill, and stops if that folder already exists.
Keep the folder in git, or import it into another workspace:

```python
folder = skill.export("exported-skills")  # exported-skills/post-test-review
analyst.import_skill(client, folder, workspace="<WORKSPACE_RID>", enabled=False)
```
