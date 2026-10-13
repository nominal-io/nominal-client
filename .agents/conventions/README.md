# Repository conventions

These guides explain how we build and review this SDK. They are for contributors and
the tools helping them. Start with [AGENTS.md](../../AGENTS.md), then read the guides
relevant to your change alongside the code.

| Guide | Owns |
| --- | --- |
| [Python quality](python.md) | PEP/style baseline, compatibility, package ownership, and performance/resource contracts |
| [SDK contracts](sdk.md) | Adding objects/routes, deprecation, public compatibility, conversion/refresh, and failure ownership |
| [Test policy](testing.md) | Meaningful coverage, authoring, mocking, fixtures, and pruning |
| [Documentation policy](documentation.md) | Docstring semantics, notes, rendering, and generator complexity |

## Where guidance belongs

| Information | One owning location |
| --- | --- |
| Setup and commands | `CONTRIBUTING.md`, backed by `justfile` and `pyproject.toml` |
| PR descriptions and review evidence | `.github/pull_request_template.md`, linked from `CONTRIBUTING.md` for human and AI authors |
| Shared defaults and task discovery | Root `AGENTS.md`; detailed SDK, test, and doc policy in the guides above |
| Package-specific contracts | A `CONVENTIONS.md` beside the source, routed from `AGENTS.md` when needed |
| Reusable multi-step procedures | `.agents/skills/<name>/SKILL.md`; tool discovery files only point to the source |
| Review execution and evaluation | `.github/workflows/claude-review.yml` owns CI behavior; PR descriptions record review experiments |

Start with an existing owner. Add a new guide only for a distinct topic with enough
substance to justify another file. Add scoped `AGENTS.md` files only when they improve
local discovery, and link to the owning policy instead of copying it. Reviews spanning
packages must follow those links rather than rely on a tool loading every nested file.
Personal settings, permissions, credentials, and scratch plans stay local.

## Changing a shared convention

Improve these guides through normal PR review. Explain why a convention helps, where it
applies, and what adopting it asks of contributors:

- **Problem and scope:** the failure, unnecessary complexity, or unclear design it addresses.
  A shared design standard can improve code without preventing a specific bug; show a
  concrete example rather than relying on one reviewer's preferred spelling.
- **Strength and exceptions:** whether it is a requirement or a preferred default, why,
  and an example where a superficially similar change should not be flagged. Use clear
  requirement language only when the consequence warrants it.
- **Adoption cost:** whether existing code differs, whether a migration is needed, and
  how new changes should behave during it. Do not require unrelated cleanup or describe
  a target convention as established implementation behavior.
- **Evidence:** a concrete example of the improvement and where the rule should stop.
  Put supporting review history and experiment results in the PR description.

Keep this explanation proportional to the change; a few sentences may be enough.
A broad or controversial policy change should be reviewable separately from the feature
that motivates it. Merged policy is the baseline; a proposed relaxation does not silently
excuse its own PR. Reviewers resolve intentional exceptions in the PR discussion. Promote
an exception to a new shared default only through an explicit guidance change.

Update the owner, affected routes, and examples together. Remove obsolete guidance when
replacing it. If source and policy disagree, identify whether this is a regression, an
accepted exception, or an unfinished migration; avoid guessing or rewriting unrelated code.

## Maintaining discovery

Keep one canonical skill under `.agents/skills/` and expose it using the repository's
existing relative `.claude/skills/` links. Add narrow `.gitignore` exceptions for promoted
skills; do not start tracking all local tool settings. If a platform cannot use those
links, use a thin discovery adapter pointing to the same source rather than copying it.

For instruction-only changes, check relative links and heading anchors, referenced
source paths, skill adapters, and documented recipes. Validate changed workflows with
actionlint. When routing changes, verify that the CI prompt passes the actual policies
to reviewers and that local sessions can identify the relevant source. A valid link is
necessary but does not prove a model read or applied the rule.

These conventions live in `.agents/conventions/` and serve human contributors, local agents,
and CI review. `AGENTS.md` routes to them, and `CLAUDE.md` imports that entry point.
Keep `docs/` for the published SDK documentation. Contributor links included in that site
use repository URLs to reach these guides.
