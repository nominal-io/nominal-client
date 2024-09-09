import pytest
from nominal.sdk import NominalClient


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
