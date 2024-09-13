Developer workflows are run with [`just`](https://github.com/casey/just). You can use `just -l` to list commands, and view the `justfile` for the commands.

We use `poetry` for packaging and developing. Add a depenency with `poetry add dep`, or `poetry add --group dev dep` for a dev dependency.

We use `ruff` for formatting and imports, `mypy` for static typing, and `pytest` for testing.

To run all tests and checks: `just verify`. To include e2e tests (for Nominal developers): `just verify-e2e`.

As a rule, all tools should be configured via pyproject.toml, and should prefer configuration over parameters for project information. For example, `poetry run mypy` should work without having to run `poetry run mypy nominal`.

Tests are written with `pytest`. By default, `pytest` runs all the tests in `tests/` except the end-to-end (e2e) tests in `tests/e2e`. To run e2e tests, `pytest` needs to be passed the e2e test directory with command-line arguments for connecting to the Nominal platform to test against. The e2e tests can be ran manually as:

```sh
poetry run pytest tests/e2e --auth-token AUTH_TOKEN [--base-url BASE_URL]
```

or simply with `just test-e2e <token>`.
