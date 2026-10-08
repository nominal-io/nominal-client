Developer workflows are run with [`just`](https://github.com/casey/just). You can use `just -l` to list commands, and view the `justfile` for the commands.

We use `uv` for packaging and developing. This repository is a uv workspace: the root package is `nominal`, and extension distributions live in `packages/`. Add a dependency with `uv add dep`, `uv add --dev dep` for a dev dependency, or `uv add --package PACKAGE dep` when the dependency belongs to a workspace package.

We use `ruff` for formatting and imports, `mypy` for static typing, and `pytest` for testing.

To run all tests and checks: `just verify`. To include e2e tests (for Nominal developers): `just verify-e2e`.

Use `just build` to build all workspace distributions.

As a rule, all tools should be configured via pyproject.toml, and should prefer configuration over parameters for project information. For example, `uv run mypy` should work without having to run `uv run mypy nominal`.

Tests are written with `pytest`. By default, `pytest` runs all the tests in `tests/` except the end-to-end (e2e) tests in `tests/e2e`. To run e2e tests, `pytest` needs to be passed the e2e test directory with command-line arguments for connecting to the Nominal platform to test against.

The preferred way is to use a named Nominal profile:

```sh
uv run pytest tests/e2e --profile PROFILE_NAME
```

or simply with `just test-e2e <profile>`.

Alternatively, a raw auth token can be supplied directly:

```sh
uv run pytest tests/e2e --auth-token AUTH_TOKEN [--base-url BASE_URL]
```

or with `just test-e2e-token <token>`.

## Working with AI

[AGENTS.md](AGENTS.md) is the shared implementation and review guide. Codex loads it
as repository instructions; [CLAUDE.md](CLAUDE.md) imports the same file for Claude Code.
The CI reviewer uses these conventions too. Start a fresh session after instruction changes;
ask the assistant which repository instructions it loaded if behavior seems inconsistent.
For another editor, explicitly include `AGENTS.md` if it does not load that file itself.

Give the assistant the intended behavior and relevant constraints, then let it inspect the
existing code. For example:

> Implement this change following AGENTS.md. First identify the existing wrapper/helper
> that owns the behavior and the public contracts to preserve. Add only meaningful tests,
> update the docstrings, and validate the final change. Summarize any unresolved gaps.

Before opening a PR, use a fresh review context when practical:

> Review my branch against origin/main, including uncommitted changes, using AGENTS.md
> and the thermo-nuclear-code-quality-review skill. Trace the changed behavior through
> callers and tests. Keep this review read-only and local; report actionable findings
> with file locations, the reviewed commit and dirty-file scope, and validation gaps.

The shared skills are [structural review](.agents/skills/thermo-nuclear-code-quality-review/SKILL.md)
and [API dependency upgrades](.agents/skills/reviewing-nominal-api-bumps/SKILL.md).
They live in `.agents/skills/` with discovery links under `.claude/skills/`; edit the source
files rather than making tool-specific copies. Local implementation should run the relevant
checks above; CI's static-review restriction is specific to that job.

When human review uncovers a recurring expectation, propose a concise rule or example in
`AGENTS.md` (or a scoped instruction file for a local concern). Include its rationale and
exceptions; do not turn every one-off preference into a repository-wide rule. Update the
[review calibration cases](.github/review-calibration.md) when it changes what should be
flagged. Keep credentials, private context, and personal preferences out of shared files.

Instruction-loading references: [Codex](https://developers.openai.com/codex/guides/agents-md/)
and [Claude Code](https://code.claude.com/docs/en/memory#share-one-file-with-other-coding-tools).
