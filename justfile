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

# run e2e tests using a named Nominal profile (preferred)
test-e2e profile:
    uv run pytest tests/e2e --profile {{profile}} --no-cov -v

# run e2e tests using a raw auth token
test-e2e-token token:
    uv run pytest tests/e2e --auth-token {{token}} --no-cov -v

# run migration e2e tests using named Nominal profiles (source is prod, dest is staging)
test-e2e-migration source-profile dest-profile:
    uv run pytest tests/e2e/migration \
        --source-profile {{source-profile}} \
        --dest-profile {{dest-profile}} \
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

# check static typing with ty (Astral, alpha — for evaluation alongside mypy)
check-types-ty:
    uv run ty check

# run mypy and ty back to back, capturing each output to a file
check-types-both:
    @echo "=== mypy ==="
    -uv run mypy 2>&1 | tee .types-mypy.out
    @echo ""
    @echo "=== ty ==="
    -uv run ty check --output-format concise 2>&1 | tee .types-ty.out
    @echo ""
    @echo "Outputs written to .types-mypy.out and .types-ty.out"

# time mypy vs ty: each runs cold (cache cleared) then warm (cache primed)
time-types:
    @echo "Clearing mypy cache..."
    rm -rf .mypy_cache
    @echo ""
    @echo "=== mypy (cold) ==="
    time uv run mypy >/dev/null 2>&1 || true
    @echo ""
    @echo "=== mypy (warm) ==="
    time uv run mypy >/dev/null 2>&1 || true
    @echo ""
    @echo "=== ty (cold) ==="
    time uv run ty check >/dev/null 2>&1 || true
    @echo ""
    @echo "=== ty (warm) ==="
    time uv run ty check >/dev/null 2>&1 || true

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

# clean up uv environments
clean:
    uv cache clean

# build docs
build-docs:
    uv run mkdocs build --config-file docs/mkdocs.yml

# serve docs locally
serve-docs:
    uv run mkdocs serve --config-file docs/mkdocs.yml
