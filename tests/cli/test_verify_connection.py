from __future__ import annotations

from unittest.mock import MagicMock, patch

import click
import pytest
from conjure_python_client import ConjureHTTPError
from requests import HTTPError, Response

from nominal.cli.util.verify_connection import validate_token_url
from nominal.core.exceptions import NominalConfigError


def _conjure_error(status_code: int) -> ConjureHTTPError:
    response = Response()
    response.status_code = status_code
    return ConjureHTTPError(HTTPError(response=response))


def test_validate_token_url_accepts_valid_credentials() -> None:
    """Successful auth and workspace resolution should not emit user-facing errors."""
    client = MagicMock()

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client) as create_client,
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    create_client.assert_called_once_with("https://api.gov.nominal.io/api", "token", ssl_context_provider=None)
    client.get_user.assert_called_once_with()
    client.get_workspace.assert_called_once_with(None)
    secho.assert_not_called()


def test_validate_token_url_passes_ssl_context_provider() -> None:
    """Validation should use the same ssl context provider as the profile being validated."""
    client = MagicMock()
    ssl_context_provider = MagicMock()

    with patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client) as create_client:
        validate_token_url(
            "token",
            "https://api.gov.nominal.io/api",
            None,
            ssl_context_provider=ssl_context_provider,
        )

    create_client.assert_called_once_with(
        "https://api.gov.nominal.io/api",
        "token",
        ssl_context_provider=ssl_context_provider,
    )


@pytest.mark.parametrize(
    ("status_code", "expected_message"),
    [
        (401, "authorization token may be invalid"),
        (404, "base_url may be incorrect"),
        (500, "misconfiguration between the base_url and token"),
    ],
)
def test_validate_token_url_surfaces_user_lookup_failures(status_code: int, expected_message: str) -> None:
    """User lookup failures should be translated into actionable click errors."""
    client = MagicMock()
    client.get_user.side_effect = _conjure_error(status_code)

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client),
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
        pytest.raises(click.ClickException, match="Failed to authenticate"),
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    assert expected_message in secho.call_args.args[0].lower()
    assert secho.call_args.kwargs == {"err": True, "fg": "red"}


def test_validate_token_url_surfaces_missing_default_workspace() -> None:
    """Missing default workspace resolution should be rewritten into a user-facing config error."""
    client = MagicMock()
    client.get_workspace.side_effect = NominalConfigError("no default workspace")

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client),
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
        pytest.raises(click.ClickException, match="Failed to authenticate"),
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    assert "workspace not provided" in secho.call_args.args[0].lower()
    assert secho.call_args.kwargs == {"err": True, "fg": "red"}


@pytest.mark.parametrize(
    ("status_code", "expected_message"),
    [
        (404, "base_url may be incorrect"),
        (500, "misconfiguration; received status_code=500"),
    ],
)
def test_validate_token_url_surfaces_workspace_lookup_http_failures(status_code: int, expected_message: str) -> None:
    """Workspace lookup HTTP failures should be translated into actionable click errors."""
    client = MagicMock()
    client.get_workspace.side_effect = _conjure_error(status_code)

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client),
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
        pytest.raises(click.ClickException, match="Failed to authenticate"),
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    assert expected_message in secho.call_args.args[0].lower()
    assert secho.call_args.kwargs == {"err": True, "fg": "red"}
