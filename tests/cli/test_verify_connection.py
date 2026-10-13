from __future__ import annotations

from unittest.mock import MagicMock, patch

import click
import pytest

from nominal.cli.util.verify_connection import validate_token_url
from nominal.exceptions import (
    NominalAuthenticationError,
    NominalConfigError,
    NominalError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)


def test_validate_token_url_reports_invalid_base_url() -> None:
    with pytest.raises(click.ClickException, match="Invalid client configuration.*localhost") as exc:
        validate_token_url("token", "http://localhost:20000/api", None)
    assert isinstance(exc.value.__cause__, NominalConfigError)


@pytest.mark.parametrize("scheme", ["http", "https"])
def test_validate_token_url_does_not_expose_url_credentials(scheme: str) -> None:
    with pytest.raises(click.ClickException, match="must not contain user information") as exc:
        validate_token_url("token", f"{scheme}://secret-user:secret-password@127.0.0.1:20000/api", None)
    assert "secret-user" not in str(exc.value)
    assert "secret-password" not in str(exc.value)


def test_validate_token_url_accepts_valid_credentials() -> None:
    """Successful auth and workspace resolution should not emit user-facing errors."""
    client = MagicMock()

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client) as create_client,
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    create_client.assert_called_once_with("https://api.gov.nominal.io/api", "token")
    client.get_user.assert_called_once_with()
    client.get_workspace.assert_called_once_with(None)
    secho.assert_not_called()


@pytest.mark.parametrize(
    ("exc", "expected_message"),
    [
        (NominalAuthenticationError("16: unauthenticated"), "authorization token may be invalid"),
        (NominalPermissionDeniedError("7: permission denied"), "misconfiguration between the base_url and token"),
        (NominalError("12: unimplemented"), "misconfiguration between the base_url and token"),
    ],
)
def test_validate_token_url_surfaces_user_lookup_failures(exc: NominalError, expected_message: str) -> None:
    """gRPC-translated user lookup failures should be translated into actionable click errors."""
    client = MagicMock()
    client.get_user.side_effect = exc

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
    ("exc", "expected_message"),
    [
        (NominalNotFoundError("not found"), "base_url may be incorrect"),
        (NominalError("13: internal"), "misconfiguration resolving the workspace"),
    ],
)
def test_validate_token_url_surfaces_workspace_lookup_failures(exc: NominalError, expected_message: str) -> None:
    """Workspace lookup gRPC-translated failures should be translated into actionable click errors."""
    client = MagicMock()
    client.get_workspace.side_effect = exc

    with (
        patch("nominal.cli.util.verify_connection.NominalClient.create", return_value=client),
        patch("nominal.cli.util.verify_connection.click.secho") as secho,
        pytest.raises(click.ClickException, match="Failed to authenticate"),
    ):
        validate_token_url("token", "https://api.gov.nominal.io/api", None)

    assert expected_message in secho.call_args.args[0].lower()
    assert secho.call_args.kwargs == {"err": True, "fg": "red"}
