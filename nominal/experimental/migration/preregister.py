from __future__ import annotations

from typing import Mapping, Sequence

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.client import NominalClient
from nominal.core.user import User
from nominal.protos.authentication.users.v1 import users_pb2


def preregister_users(client: NominalClient, emails: Sequence[str]) -> Mapping[str, User]:
    """Preregister users for stack migrations before their first login.

    This is intended for migration workflows that need destination-tenant user RIDs ahead of login so migrated
    resources can preserve `created_by` attribution.

    Args:
        client: Destination tenant client. The caller must be an org admin in that tenant.
        emails: Email addresses to preregister. Accepts at most 1000 emails per request.

    Returns:
        A mapping from email address to newly created user details. Emails that already belong to existing
        accounts are omitted from the response.

    Raises:
        NominalError: If the preregistration request fails, including when the caller is not an org admin or more
            than 1000 emails are given.
    """
    request = users_pb2.PreregisterUsersRequest(emails=emails)
    with translate_grpc_errors():
        response = client._clients.users.PreregisterUsers(request)
    # The service stores each email exactly as requested, so a created user's email is the requested address.
    return {raw_user.email: User._from_proto(raw_user) for raw_user in response.users}
