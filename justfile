set positional-arguments

# Default command is no subcommand given to list available commands
default:
    @just --list

# development install with dependencies
install:
    uv sync

# Execute the python CLI
cli *args='--help':
    uv run nom "$@"

# Enter into the python interpreter with all dependencies loaded
python *args:
    uv run python "$@"

# run unit tests
test:
    uv run pytest

# run e2e tests | token: auth token for api.nominal.test
test-e2e token:
    uv run pytest tests/e2e --auth-token {{token}}

# check static typing
check-types:
    uv run mypy

# check static typing across all supported python versions
check-types-all:
    uv run mypy --python-version 3.13
    uv run mypy --python-version 3.12
    uv run mypy --python-version 3.11
    uv run mypy --python-version 3.10
    uv run mypy --python-version 3.9

# check code formatting | fix with `just fix-format`
check-format:
    uv run ruff format --check

# check import ordering | fix with `just fix-imports`
check-imports:
    uv run ruff check

# run all static analysis checks
check: check-format check-types check-imports

# fixes out-of-order imports (note: mutates the code)
fix-imports:
    uv run ruff check --fix

# fixes code formatting (note: mutates the code)
fix-format:
    uv run ruff format

# fix imports and formatting
fix: fix-format fix-imports

# run all tests and checks, except e2e tests
verify: install test check

# run all tests and checks, including e2e tests
verify-e2e token: install check test (test-e2e token)

# clean up uv environments
clean:
    uv cache clean

# build docs
build-docs:
    uv run mkdocs build
