Developer workflows are run with [`just`](https://github.com/casey/just). You can use `just -l` to list commands, and view the `justfile` for the commands.

We use `uv` for packaging and developing. Add a dependency with `uv add dep`, or `uv add --dev dep` for a dev dependency.

We use `ruff` for formatting and imports, `mypy` for static typing, and `pytest` for testing.

To run all tests and checks: `just verify`. To include e2e tests (for Nominal developers): `just verify-e2e`.

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

## Evaluating `ty` alongside `mypy`

We currently use `mypy` for static typing. `ty` (Astral, alpha) is installed as a dev dependency for evaluation. It is **not** wired into CI and is **not** required to pass — `just check-types` is still the gate.

- `just check-types-ty` — run ty on its own.
- `just check-types-both` — run mypy then ty back-to-back. Each tool's output is also written to `.types-mypy.out` and `.types-ty.out` (gitignored) so you can diff them directly.
- `just time-types` — clear the mypy cache, then run mypy (cold), mypy (warm), ty (cold), ty (warm) and print wall-clock time for each.

Notes:
- `ty` is scoped to `nominal/` via `[tool.ty.src]` in `pyproject.toml` to mirror `[tool.mypy].packages`. Its strictness defaults differ from mypy's `strict = true` and the per-module `[[tool.mypy.overrides]]` are not yet translated, so expect ty to surface diagnostics that mypy doesn't.
- `# type: ignore[<code>]` codes are mypy-specific and won't be recognised by ty; ty uses `# ty: ignore[<rule>]`. Don't bulk-rewrite them while ty is still being evaluated.
