from unittest.mock import MagicMock

from nominal_api import authentication_api

from nominal.core.client import NominalClient
from nominal.core.user import User
from nominal.experimental.migration import batch_preregister_users


def test_batch_preregister_users_forwards_emails_and_maps_users() -> None:
    """Migration preregistration should preserve request order and map API users into SDK users."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.authentication.batch_preregister_users.return_value = authentication_api.BatchPreregisterUsersResponse(
        users={
            "new@example.com": authentication_api.UserV2(
                rid="ri.authn.dev.user.new",
                display_name="new@example.com",
                email="new@example.com",
                avatar_url=None,
                org_rid="ri.authentication.dev.organization.primary",
            )
        }
    )
    client = NominalClient(_clients=clients)

    result = batch_preregister_users(client, ["new@example.com", "existing@example.com"])

    clients.authentication.batch_preregister_users.assert_called_once()
    request = clients.authentication.batch_preregister_users.call_args.args[1]
    assert request.emails == ["new@example.com", "existing@example.com"]
    assert result == {
        "new@example.com": User(
            rid="ri.authn.dev.user.new",
            display_name="new@example.com",
            email="new@example.com",
        )
    }
