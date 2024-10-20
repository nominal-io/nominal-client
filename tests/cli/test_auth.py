from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from conjure_python_client import ConjureHTTPError
from requests import HTTPError

from nominal.cli.auth import set_token

runner = CliRunner()


def mock_conjure_http_error(status_code):
    """Helper to create a ConjureHTTPError with a mocked HTTPError."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_http_error = HTTPError(response=mock_response)
    return ConjureHTTPError(mock_http_error)


@patch("nominal.NominalClient.create")
def test_invalid_token(mock_create: MagicMock):
    mock_client = MagicMock()
    mock_create.return_value = mock_client
    mock_client.get_user.side_effect = mock_conjure_http_error(401)

    result = runner.invoke(set_token, ["-t", "invalid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "Your authorization token seems to be incorrect" in result.output
    assert result.exit_code == 1


@patch("nominal.NominalClient.create")
def test_invalid_url(mock_create: MagicMock):
    mock_client = MagicMock()
    mock_create.return_value = mock_client
    mock_client.get_user.side_effect = mock_conjure_http_error(404)

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://invalid-url"])

    assert "Your base_url is not correct" in result.output
    assert result.exit_code == 1


@patch("nominal.NominalClient.create")
def test_bad_request(mock_create: MagicMock):
    mock_client = MagicMock()
    mock_create.return_value = mock_client
    mock_client.get_user.side_effect = mock_conjure_http_error(500)

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "misconfiguration between your base_url and token" in result.output
    assert result.exit_code == 1


@patch("nominal.NominalClient.create")
def test_good_request(mock_create: MagicMock):
    mock_client = MagicMock()
    mock_create.return_value = mock_client
    mock_client.get_user.return_value = {}

    result = runner.invoke(set_token, ["-t", "valid-token", "-u", "https://api.gov.nominal.io/api"])

    assert "Successfully set token" in result.output
    assert result.exit_code == 0
