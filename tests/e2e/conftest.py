import gzip
from pathlib import Path
from typing import Iterator
from unittest import mock

import pytest

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


@pytest.fixture(scope="session")
def client(base_url, auth_token) -> Iterator[None]:
    return NominalClient.create(base_url=base_url, token=auth_token)


@pytest.fixture(scope="session", autouse=True)
def set_connection(client) -> Iterator[None]:
    with mock.patch("nominal.nominal.get_default_client", return_value=client):
        yield


@pytest.fixture(scope="session")
def csv_data():
    return b"""\
timestamp,relative_minutes,temperature,humidity
2024-09-05T18:00:00Z,0,20,50
2024-09-05T18:01:00Z,1,21,49
2024-09-05T18:02:00Z,2,22,48
2024-09-05T18:03:00Z,3,23,47
2024-09-05T18:04:00Z,4,24,46
2024-09-05T18:05:00Z,5,25,45
2024-09-05T18:06:00Z,6,26,44
2024-09-05T18:07:00Z,7,27,43
2024-09-05T18:08:00Z,8,28,42
2024-09-05T18:09:00Z,9,29,41
"""


@pytest.fixture(scope="session")
def csv_data2():
    return b"""\
timestamp,relative_minutes,temperature,humidity
2024-09-05T18:10:00Z,10,30,40
2024-09-05T18:11:00Z,11,31,39
2024-09-05T18:12:00Z,12,32,38
2024-09-05T18:13:00Z,13,33,37
2024-09-05T18:14:00Z,14,34,36
2024-09-05T18:15:00Z,15,35,35
2024-09-05T18:16:00Z,16,36,34
2024-09-05T18:17:00Z,17,37,33
2024-09-05T18:18:00Z,18,38,32
2024-09-05T18:19:00Z,19,39,31
"""


@pytest.fixture(scope="session")
def csv_gz_data(csv_data):
    return gzip.compress(csv_data)


@pytest.fixture(scope="session")
def mp4_data():
    """From chromium tests: https://github.com/chromium/chromium/blob/main/media/test/data/bear-1280x720.mp4

    To download:
        curl https://raw.githubusercontent.com/chromium/chromium/main/media/test/data/bear-1280x720.mp4 -o data/bear-1280x720.mp4
    """
    path = Path(__file__).parent / "data/bear-1280x720.mp4"
    with open(path, "rb") as f:
        return f.read()
