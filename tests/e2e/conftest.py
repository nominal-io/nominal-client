from typing import Iterator
from unittest import mock

import pytest

import nominal as nm
from nominal.core import NominalClient


def pytest_addoption(parser):
    parser.addoption("--base-url", default="https://api.nominal.test")
    parser.addoption("--auth-token", required=True)


@pytest.fixture(scope="session")
def auth_token(pytestconfig):
    return pytestconfig.getoption("auth_token")


@pytest.fixture(scope="session")
def base_url(pytestconfig):
    return pytestconfig.getoption("base_url")


@pytest.fixture(scope="session", autouse=True)
def set_connection(base_url, auth_token) -> Iterator[None]:
    client = NominalClient.create(base_url=base_url, token=auth_token)
    with mock.patch("nominal.nominal.get_default_client", return_value=client):
        yield


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


@pytest.fixture(scope="session")
def csv_data2():
    return b"""\
timestamp,temperature,humidity
2024-09-05T18:10:00Z,30,40
2024-09-05T18:11:00Z,31,39
2024-09-05T18:12:00Z,32,38
2024-09-05T18:13:00Z,33,37
2024-09-05T18:14:00Z,34,36
2024-09-05T18:15:00Z,35,35
2024-09-05T18:16:00Z,36,34
2024-09-05T18:17:00Z,37,33
2024-09-05T18:18:00Z,38,32
2024-09-05T18:19:00Z,39,31
"""
