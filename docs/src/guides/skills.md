---
myst:
  html_meta:
    description: "Find, create, edit and share reusable instructions for Analyst with the Nominal Python Client."
---

# Skills

{.lead}
A skill gives Analyst, Nominal's AI assistant, instructions for a repeatable task such as a post-test review.
Creating or changing a skill requires permission to edit the workspace's configuration.

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
# Add enabled=True to search only skills that are turned on.
for skill in skills:
    # Load status is an enum such as AVAILABLE, INVALID, or MISSING.
    print(skill.name, skill.is_enabled, skill.get_load_status())
```

Read the instructions in SKILL.md and the files the skill refers to:

```python
skill = analyst.get_skill(client, "<SKILL_RID>")  # Or use a skill returned by search_skills.
print(skill.get_content())  # The current SKILL.md.
for file in skill.list_files():
    print(file.path, file.size_bytes)
    if file.path != "SKILL.md":
        print(skill.read_file(file.path))
```

## Create or import a skill

Use {py:func}`~nominal.experimental.analyst.create_skill` to save instructions and reference files.
SKILL.md follows the [Agent Skills](https://agentskills.io/specification) format, with a name and description in its YAML frontmatter.
Each workspace shares a skills library. Skills are enabled by default, so pass `enabled=False` while preparing instructions you are not ready for others to use:

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
The client reads and writes UTF-8 text files. See {py:func}`~nominal.experimental.analyst.create_skill` for validation rules.

If you already have a folder with SKILL.md at its top level, or a .zip containing one, use {py:func}`~nominal.experimental.analyst.import_skill` instead:

```python
skill = analyst.import_skill(client, "skills/post-test-review", enabled=False)
```

Import skips dot-prefixed files and folders, such as `.git`, and folder imports skip symbolic links.
If creation or import fails, check the error's notes before retrying. A timeout can leave a skill or its files saved. When the notes give a skill RID, fetch it with `analyst.get_skill(client, rid)` to check its state.

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
`write_files` and `remove_files` can also raise this error when a file changes during a save. Review the current files before retrying those operations.

If an error says SKILL.md was saved but the skill record could not be updated, fetch the skill and read its content before retrying. Some changes have already been saved.

## Archive and restore a skill

Use {py:meth}`~nominal.experimental.analyst.Skill.archive` to hide a skill from the library and stop making it available to agents.
{py:meth}`~nominal.experimental.analyst.Skill.unarchive` brings it back turned off:

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
Export creates a new folder named after the skill and refuses to overwrite an existing folder.
Keep the folder in git, or import it into another workspace:

```python
folder = skill.export("exported-skills")  # exported-skills/post-test-review
analyst.import_skill(client, folder, workspace="<WORKSPACE_RID>", enabled=False)
```
