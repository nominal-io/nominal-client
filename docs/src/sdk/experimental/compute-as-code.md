# Compute as code

Author reusable {term}`checklist` definitions, evaluate compute expressions, and publish derived {term}`datasets <dataset>`
with `nominal.experimental.compute_as_code`. This module is experimental and requires
`pip install 'nominal[compute]'`.

For the checklist workflow, see [Authoring checklists as code](../../guides/checklists/authoring.md).
The propulsion test-stand script {download}`deploy_checklist.py </examples/deploy_checklist.py>` creates and revises
thermal, pressure, and controller {term}`checks <check>`, evaluates a {term}`run`, and inspects generated {term}`events <event>`.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.compute_as_code.rst
```
