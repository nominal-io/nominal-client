from unittest.mock import MagicMock

from nominal.core.client import NominalClient
from nominal.core.user import User
from nominal.experimental.migration import preregister_users
from nominal.protos.authentication.users.v1 import users_pb2


def test_preregister_users_forwards_emails_and_keys_created_users_by_email() -> None:
    """Migration preregistration should preserve request order and key the created users by their email."""
    clients = MagicMock()
    clients.users.PreregisterUsers.return_value = users_pb2.PreregisterUsersResponse(
        users=[
            users_pb2.User(
                rid="ri.authn.dev.user.new",
                org_rid="ri.authentication.dev.organization.primary",
                email="new@example.com",
                display_name="new@example.com",
            )
        ]
    )
    client = NominalClient(_clients=clients)

    result = preregister_users(client, ["new@example.com", "existing@example.com"])

    clients.users.PreregisterUsers.assert_called_once_with(
        users_pb2.PreregisterUsersRequest(emails=["new@example.com", "existing@example.com"])
    )
    assert result == {
        "new@example.com": User(
            rid="ri.authn.dev.user.new",
            display_name="new@example.com",
            email="new@example.com",
        )
    }
