# development install with dependencies
install:
    poetry install

# run unit tests
test:
    poetry run pytest --cov=nominal --cov-branch --cov-report=html --cov-report=term --cov-config=.coveragerc

# run e2e tests | token: auth token for api.nominal.test
test-e2e token:
    poetry run pytest tests/e2e --auth-token {{token}}

# check static typing
check-types:
    poetry run mypy

# check static typing across all supported python versions
check-types-all:
    poetry run mypy --python-version 3.12
    poetry run mypy --python-version 3.11
    poetry run mypy --python-version 3.10
    poetry run mypy --python-version 3.9

# check code formatting | fix with `just fix-format`
check-format:
    poetry run ruff format --check

# check import ordering | fix with `just fix-imports`
check-imports:
    poetry run ruff check --select I --select F401

# run all static analysis checks
check: check-format check-types check-imports

# fixes out-of-order imports (note: mutates the code)
fix-imports:
    poetry run ruff check --select I --select F401 --fix

# fixes code formatting (note: mutates the code)
fix-format:
    poetry run ruff format

# fix imports and formatting
fix: fix-imports fix-format

# run all tests and checks, except e2e tests
verify: install test check

# run all tests and checks, including e2e tests
verify-e2e token: install check test (test-e2e token)

# clean up poetry environments
clean:
    poetry env remove --all

# build docs
build-docs:
    poetry run mkdocs build
