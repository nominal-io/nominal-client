# Analyst

Manage workspace skills and their reference files.
These APIs are experimental and may change between releases.

Import from `nominal.experimental.analyst`. Creation and lookup functions take an authenticated
{py:class}`~nominal.core.NominalClient` as their first argument.
See the {doc}`skills guide </guides/skills>` for skill authoring examples.

Use {py:meth}`~nominal.experimental.analyst.Skill.as_prompt` to format text that selects a skill
for an Analyst prompt. It makes no request and does not run the skill:

```python
from nominal.experimental import analyst

skill = analyst.get_skill(client, "<SKILL_RID>")
prompt = skill.as_prompt("Review the run against its test plan.")
```

Analyst resolves the skill's current content and checks access when a supported caller submits
the prompt. The skill must be enabled and readable at that time.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.analyst.rst
```
