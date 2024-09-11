# ⬖ Nominal

Python client for Nominal test data, storage, and compute.

## Install

```sh
python3 -m pip install nominal --upgrade
```

## Usage

```py
import nominal as nm
```

### Connecting to Nominal

Retrieve your API key from [/sandbox](https://app.gov.nominal.io/sandbox) on your Nominal tenant. Then, set the Nominal connection parameters in a terminal:

```sh
python3 -m nominal auth set-token
```

This sets the auth token on your system, which can be updated with the same command as needed.

### Upload a Dataset

```py
dataset = nm.upload_csv(
    '../path/to/data.csv',
    name='Stand A',
    timestamp_column='timestamp',
    timestamp_type='epoch_seconds',
)
print('Uploaded dataset:', dataset.rid)
```

### Create a Run

```py
run = nm.create_run(
    name='Run A',
    start='2024-09-09T12:35:00Z',
    start='2024-09-09T13:18:00Z',
)
print("Created run:", run.rid)
```

### Update metadata of an existing Run

```py
run = nm.get_run('ri.scout.gov-staging.run.ce205f7e-9ef1-4a8b-92ae-11edc77441c6')
run.update(name='New Run Title')
```

### Change default Nominal tenant

By default, the library uses `https://api.gov.nominal.io/api` as the base url to the Nominal platform. Your scripts can change the URL they use with `set_base_url()`. For example, to use the staging URL:

```py
nm.set_base_url('https://api-staging.gov.nominal.io/api')
```

## Development

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
