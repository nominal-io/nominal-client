from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from conjure_python_client import ConjureHTTPError
from requests import HTTPError

from nominal.cli.auth import set_token


@pytest.fixture()
def runner():
    yield CliRunner()


@pytest.fixture()
def mock_client():
    with patch("nominal.NominalClient.from_url") as mock_create:
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        yield mock_client


def mock_conjure_http_error(status_code):
    """Helper to create a ConjureHTTPError with a mocked HTTPError."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_http_error = HTTPError(response=mock_response)
    return ConjureHTTPError(mock_http_error)


def test_invalid_token(mock_client: MagicMock, runner: CliRunner):
    mock_client.get_user.side_effect = mock_conjure_http_error(401)

    result = runner.invoke(set_token, ["-t", "invalid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "The authorization token may be invalid" in result.output
    assert result.exit_code == 1


def test_invalid_url(mock_client: MagicMock, runner: CliRunner):
    mock_client.get_user.side_effect = mock_conjure_http_error(404)

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://invalid-url"])

    assert "The base_url may be incorrect" in result.output
    assert result.exit_code == 1


def test_bad_request(mock_client: MagicMock, runner: CliRunner):
    mock_client.get_user.side_effect = mock_conjure_http_error(500)

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "misconfiguration between the base_url and token" in result.output
    assert result.exit_code == 1


def test_good_request(mock_client: MagicMock, runner: CliRunner):
    mock_client.get_user.return_value = {}

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "Successfully set token" in result.output
    assert result.exit_code == 0
