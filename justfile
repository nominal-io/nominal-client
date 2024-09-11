# development install with dependencies
install:
    poetry install

# run only unit tests
test-unit:
    poetry run pytest

# run e2e tests | token: auth token for api.nominal.test
test-e2e token:
    poetry run pytest tests/e2e --auth-token {{token}}

# run all tests | token: auth token for api.nominal.test
test-all token: test-unit (test-e2e token)

# check static typing
check-types:
    poetry run mypy

# check code formatting | fix with `just fix-format`
check-format:
    poetry run ruff format --check

# check import ordering | fix with `just fix-imports`
check-imports:
    poetry run ruff check --select I

# run all static analysis checks
check-all: check-format check-types check-imports

# fixes out-of-order imports (note: mutates the code)
fix-imports:
    poetry run ruff check --select I --fix

# fixes code formatting (note: mutates the code)
fix-format:
    poetry run ruff format

fix-all: fix-imports fix-format

# run all tests and checks, except e2e tests
validate: install test-unit check-all

# run all tests and checks, including e2e tests
validate-e2e token: install (test-all token) check-all
