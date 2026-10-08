# Repository conventions

These conventions apply to new and changed code. Read neighboring implementations and
scoped instructions before choosing a pattern. Do not demand unrelated cleanup or undo
an explicitly agreed design decision without new evidence. Examples below illustrate
contracts; they are not templates every class or test must reproduce.

## Shared standards, not personal preferences

These guides serve all contributors, with or without AI. Requirements protect named
contracts; defaults such as "prefer" allow a justified alternative. Reviewers must
distinguish a defect or explicit requirement violation from an optional design suggestion.
Do not turn a default, a personal instruction, or an isolated review comment into a blocker.
For structural findings, identify the concrete maintenance cost and a simpler alternative.

Use merged guidance as the team's baseline. Changes to these rules are reviewable proposals
until accepted through the normal PR process; do not weaken a rule in a patch and then
declare that patch compliant. Explain deliberate exceptions in the PR for human review.
An accepted exception applies to that change, not every future use. When guidance conflicts,
state the conflict rather than guessing a new team-wide policy. See
[maintaining shared guidance](docs/development/README.md#changing-a-shared-convention).

## Read by task

Before implementation or review, read the applicable guides below and any scoped
instructions for affected paths. These linked guides are required repository conventions,
not optional background. Read only relevant routes; changes can need more than one.

| Task | Required guidance |
| --- | --- |
| Change public behavior, resource wrappers, conversion, or failure semantics | [SDK contracts](docs/development/sdk.md), relevant callers and neighboring wrappers |
| Add/change nontrivial behavior, fix a regression, or author/review/prune tests | [Test policy](docs/development/testing.md), existing coverage for the affected contract |
| Change public docstrings, examples, or documentation rendering | [Documentation policy](docs/development/documentation.md), the branch's renderer configuration |
| Upgrade `nominal-api` or `nominal-api-protos` | [API upgrade skill](.agents/skills/reviewing-nominal-api-bumps/SKILL.md), plus affected contracts above |
| Review or simplify structure | [Structural review skill](.agents/skills/thermo-nuclear-code-quality-review/SKILL.md), plus affected contracts above |
| Change AI instructions, review automation, or shared skills | [Guidance ownership](docs/development/README.md), [review calibration](.github/review-calibration.md) |

A production-code change needs the test route even if it changes no test files; a public
contract change needs the documentation route even if it changes no documentation files.
If a required guide is missing or conflicts with source, report the mismatch and resolve
which behavior is intended; do not silently skip the policy or claim full conformance.

## Local development with AI

These instructions guide implementation and local review as well as CI review. Start with
`CONTRIBUTING.md` and `just --list` for setup and commands; do not maintain a second set of
build commands here. The root package is `nominal`; extension distributions live in
`packages/` and share the uv workspace.

1. Before editing, inspect the working tree, relevant callers/tests, and a neighboring
   implementation. Identify the existing owner of the behavior and any public contract
   being changed. For substantial design changes, explain the proposed boundary and what
   complexity it removes before implementing; keep routine fixes lightweight.
2. Implement the smallest cohesive change, updating behavior, meaningful tests, and
   caller-facing documentation together. Follow the applicable conventions while writing code,
   rather than waiting for a review to find violations. Preserve unrelated local changes.
3. Validate according to the change: focused `uv run pytest <path>` during iteration,
   `just verify` for Python changes before handoff, and `just build-docs` plus representative
   rendered-page inspection for documentation rendering changes. Instruction/workflow-only
   changes need link/configuration checks, not the Python suite. Report commands actually run,
   failures, and omitted checks; do not claim validation from inspection alone.
4. Before handing off substantial changes, review the final diff against the applicable
   conventions and use the thermo-nuclear review skill for structure. Fix supported findings and
   remove unnecessary scaffolding; report unresolved decisions and validation gaps. If asked
   for review only, keep the working tree unchanged. A local review does not authorize posting
   GitHub comments or changing PR state.

Skills live in `.agents/skills/`; `.claude/skills/` exposes the same files to Claude.
Use `reviewing-nominal-api-bumps` when changing `nominal-api` or `nominal-api-protos`, and
`thermo-nuclear-code-quality-review` for deep structural review. If skill invocation is not
available in the current tool, read the relevant `SKILL.md` directly.

The CI review job is explicitly static and read-only; its no-execution rule does not apply
to local implementation work. An explicit review-only/no-execution request still takes
precedence locally. Keep personal credentials and machine-specific preferences out of these
shared instructions.

## Keep the implementation direct

- Reuse the canonical implementation before adding a helper. Small duplication is preferable
  to a generic abstraction that hides differing contracts or adds unions, modes, and casts.
  Point to the concrete code that could disappear when proposing a structural simplification.
- Keep feature-specific decisions in their owning module rather than scattering checks
  through shared paths. Do not add pass-through wrappers or inheritance layers without
  a clear responsibility. Apply the structural review skill in
  `.agents/skills/thermo-nuclear-code-quality-review/SKILL.md` with this evidence bar.
- Prefer top-level imports, but retain justified local imports for genuine cycles or optional
  dependencies. Preserve the `nominal` namespace package; do not add a root `__init__.py` to
  paper over an import problem. Use leading underscores for implementation privacy and
  explicit exports in the appropriate package `__init__.py`, not new per-module `__all__` lists.
- Repository development commands use `uv run` (or existing `just` recipes). `uv run python
  -m ...` and subprocesses using `sys.executable` are valid; do not flag them as bare Python.
  Ruff and mypy own mechanical formatting and type errors; review type boundaries rather
  than repeating their diagnostics.

## Maintaining this guidance

Keep repository-wide defaults and task routes here. Put detailed policy in the owning
[development guide](docs/development/README.md), package-specific contracts near their
source, and reusable procedures in `.agents/skills/`. Link instead of copying rules.
Update the owner and its routes in the same patch when behavior or paths change. Describe
current contracts separately from proposed standards and unfinished migrations. Keep
historical PR discussion in calibration/evidence records rather than normative policy.
