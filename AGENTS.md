# AGENTS.md

## Cursor Cloud specific instructions

This is the **Nominal Python SDK** (`nominal-client`), a pure Python client library. No Docker, databases, or external services are needed for unit tests.

### Quick reference

- **Package manager**: `uv` (lockfile: `uv.lock`)
- **Task runner**: `just` — run `just --list` for all commands, or see `justfile` and `CONTRIBUTING.md`
- **Lint**: `uv run ruff format --check` and `uv run ruff check` (or `just check-format` / `just check-imports`)
- **Type check**: `uv run mypy` (or `just check-types`)
- **Unit tests**: `uv run pytest` (or `just test`) — 61 tests, no external dependencies
- **All checks**: `just verify` (runs install + test + check)
- **CLI**: `uv run nom --help`

### Important notes

- E2E tests (`tests/e2e/`) are opt-in and require a live Nominal platform instance with auth. They are excluded from the default `pytest` run via `norecursedirs` in `pyproject.toml`. Use `just test-e2e <profile>` or `just test-e2e-token <token>` to run them.
- `uv` must be on `$PATH` — it installs to `$HOME/.local/bin`. Ensure `export PATH="$HOME/.local/bin:$PATH"` if commands are not found.
- `mypy` takes ~12s on first run; subsequent runs are faster due to caching.
- Coverage reports are generated to `htmlcov/` by default (configured in `pyproject.toml` via `--cov` pytest addopts).
