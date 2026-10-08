set positional-arguments := true

# Default command is no subcommand given to list available commands
default:
    @just --list

# development install with dependencies
install:
    uv sync --all-packages --all-extras

# Execute the python CLI
cli *args='--help':
    uv run nom "$@"

# Enter into the python interpreter with all dependencies loaded
python *args:
    uv run python "$@"

# run unit tests
test:
    uv run pytest

# run e2e tests using a named Nominal profile (preferred)
test-e2e profile:
    uv run pytest tests/e2e --profile {{ profile }} --no-cov -v

# run e2e tests using a raw auth token
test-e2e-token token:
    uv run pytest tests/e2e --auth-token {{ token }} --no-cov -v

# run migration e2e tests using named Nominal profiles (source is prod, dest is staging)
test-e2e-migration source-profile dest-profile:
    uv run pytest tests/e2e/migration \
        --source-profile {{ source-profile }} \
        --dest-profile {{ dest-profile }} \
        --no-cov -v

# check static typing
check-types:
    uv run mypy

# check static typing across all supported python versions
check-types-all:
    uv run mypy --python-version 3.14
    uv run mypy --python-version 3.13
    uv run mypy --python-version 3.12
    uv run mypy --python-version 3.11
    uv run mypy --python-version 3.10

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
verify-e2e profile: install check test (test-e2e profile)

# build all workspace packages
build:
    uv build --all-packages

# clean up uv environments
clean:
    uv cache clean

# the docs group is empty below Python 3.12, so sphinx-build would fail with an opaque "not found"
_check-docs-python:
    uv run python -c "import sys; sys.exit(0 if sys.version_info >= (3, 12) else f'The docs toolchain needs Python >= 3.12 in the project environment (found {sys.version.split()[0]}). Recreate it with: uv sync --python 3.13 --all-packages --all-extras --group docs')"

# Generated sources and HTML are disposable; always rebuild them from current Python exports and examples.
_clean-docs:
    find docs/src/reference -type d -name generated -prune -exec rm -rf {} +
    rm -rf docs/src/examples docs/_build/dirhtml

# build the docs site (guides, examples, API reference) into docs/_build/dirhtml; warnings fail the build
build-docs: _check-docs-python _clean-docs
    uv run --all-packages --all-extras --group docs sphinx-build -E -W --keep-going -j auto -b dirhtml -c docs docs/src docs/_build/dirhtml

# live-preview the docs on http://127.0.0.1:8000, rebuilding on page, example, or docstring changes
serve-docs: _check-docs-python
    mkdir -p examples
    uv run --all-packages --all-extras --group docs --with sphinx-autobuild sphinx-autobuild -E -j auto -b dirhtml -c docs docs/src docs/_build/dirhtml --pre-build "just _clean-docs" --watch nominal --watch packages --watch examples --watch docs/conf.py --watch docs/_ext --watch docs/_templates --watch docs/_static --watch CHANGELOG.md --watch LICENSE --ignore docs/src/examples --re-ignore "/generated(/|$)"
