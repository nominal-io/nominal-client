import pytest
import nominal as nm
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


@pytest.fixture(scope="session", autouse=True)
def set_conn(base_url, auth_token):
    nm.set_default_connection(base_url, auth_token)


@pytest.fixture(scope="session")
def csv_data():
    return b"""\
timestamp,temperature,humidity
2024-09-05T18:00:00Z,20,50
2024-09-05T18:01:00Z,21,49
2024-09-05T18:02:00Z,22,48
2024-09-05T18:03:00Z,23,47
2024-09-05T18:04:00Z,24,46
2024-09-05T18:05:00Z,25,45
2024-09-05T18:06:00Z,26,44
2024-09-05T18:07:00Z,27,43
2024-09-05T18:08:00Z,28,42
2024-09-05T18:09:00Z,29,41
"""
