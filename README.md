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

By default, the library uses `https://api.gov.nominal.io/api` as the base url to the Nominal platform. Your scripts can change the URL they use with `set_base_url()`. For example, to use the staging URL:

```py
nm.set_base_url('https://api-staging.gov.nominal.io/api')
```

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

## Development

### Testing

Tests are written with `pytest`. By default, `pytest` runs all the tests in `tests/` except the end-to-end tests in `tests/e2e`. Run unit tests with

```sh
poetry run pytest
```

To run end-to-end (e2e) tests, you need to point `pytest` the e2e test directory and specify command-line arguments for connecting to the Nominal platform to test against. Run the e2e tests with

```sh
poetry run pytest tests/e2e --auth-token AUTH_TOKEN [--base-url BASE_URL]
```

### Static typing

```sh
poetry run mypy nominal
```

### Formatting

To check (`I` is for `isort`):

```sh
poetry run ruff format --check
poetry run ruff check --select I
```

To fix (`I` is for `isort`):

```sh
poetry run ruff format
poetry run ruff check --select I --fix
```
