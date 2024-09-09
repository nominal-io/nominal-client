import random
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from nominal import _utils
from nominal.sdk import NominalClient


def _create_random_start_end():
    random_epoch_start = int(datetime(2020, 1, 1).timestamp())
    random_epoch_end = int(datetime(2025, 1, 1).timestamp())
    epoch_start = random.randint(random_epoch_start, random_epoch_end)
    start = datetime.fromtimestamp(epoch_start)
    end = start + timedelta(hours=1)
    return start, end


def pytest_addoption(parser):
    parser.addoption("--base-url", default="https://api.nominal.test")
    parser.addoption("--auth-token", required=True)


@pytest.fixture(scope="session")
def auth_token(pytestconfig):
    return pytestconfig.getoption("auth_token")


@pytest.fixture(scope="session")
def base_url(pytestconfig):
    return pytestconfig.getoption("base_url")


@pytest.fixture(scope="session")
def client(base_url, auth_token):
    return NominalClient.create(base_url=base_url, token=auth_token)


@pytest.fixture(scope="session")
def run(client: NominalClient):
    title = f"run-{uuid4()}"
    desc = f"run description {uuid4()}"
    start, end = _create_random_start_end()
    run = client.create_run(
        title=title,
        description=desc,
        start=start,
        end=end,
    )
    assert len(run.rid) >= 0
    assert run.title == title
    assert run.description == desc
    assert run.start == _utils._datetime_to_integral_nanoseconds(start)
    assert run.end == _utils._datetime_to_integral_nanoseconds(end)
    assert len(run.labels) == 0
    assert len(run.properties) == 0
    return run
