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

## Shared conventions

All contributors use the same [SDK, test, and documentation conventions](https://github.com/nominal-io/nominal-client/blob/main/.agents/conventions/README.md).
Read the guide relevant to your change. These distinguish requirements from preferred
defaults; explain a justified exception in the PR. Changes to shared standards follow the
normal review process, including their rationale and adoption cost. AI use is optional.

## Preparing a pull request

Use the [PR template](https://github.com/nominal-io/nominal-client/blob/main/.github/pull_request_template.md)
as a guide: explain the problem and resulting behavior, then give actual validation evidence
and material gaps. Small changes need only a few sentences. Add compatibility or review notes
when they matter; omit unused sections and boilerplate. CLI and AI-created PRs should follow
the same guidance even when the template is not inserted automatically.

## Working with AI

[AGENTS.md](https://github.com/nominal-io/nominal-client/blob/main/AGENTS.md) is the shared implementation and review guide. Codex loads it
as repository instructions; [CLAUDE.md](https://github.com/nominal-io/nominal-client/blob/main/CLAUDE.md) imports the same file for Claude Code.
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

The shared skills are [structural review](https://github.com/nominal-io/nominal-client/blob/main/.agents/skills/thermo-nuclear-code-quality-review/SKILL.md)
and [API dependency upgrades](https://github.com/nominal-io/nominal-client/blob/main/.agents/skills/reviewing-nominal-api-bumps/SKILL.md).
They live in `.agents/skills/` with discovery links under `.claude/skills/`; edit the source
files rather than making tool-specific copies. Local implementation should run the relevant
checks above; CI's static-review restriction is specific to that job.

When review exposes a recurring gap, follow
[changing a shared convention](https://github.com/nominal-io/nominal-client/blob/main/.agents/conventions/README.md#changing-a-shared-convention).
Keep the rule in its owning guide so local tools, human reviewers, and CI use the same standard.

Instruction-loading references: [Codex](https://developers.openai.com/codex/guides/agents-md/)
and [Claude Code](https://code.claude.com/docs/en/memory#share-one-file-with-other-coding-tools).

Docs live in `docs/`: one Sphinx site with the guides, examples (generated from the scripts in `examples/`), and the API reference. `just build-docs` builds it strictly (warnings fail, as in CI) and `just serve-docs` live-previews it; both need Python >=3.12. Since the API reference is built from docstrings, a docstring that isn't valid Google style fails the docs build. See [`docs/AGENTS.md`](docs/AGENTS.md) for the layout and conventions.
