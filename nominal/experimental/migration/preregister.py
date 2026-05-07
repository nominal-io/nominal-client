from __future__ import annotations

from typing import Mapping, Sequence

from nominal_api import authentication_api

from nominal.core.client import NominalClient
from nominal.core.user import User


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
    """
    request = authentication_api.BatchPreregisterUsersRequest(emails=list(emails))
    response = client._clients.authentication.batch_preregister_users(client._clients.auth_header, request)
    return {email: User._from_conjure(raw_user) for email, raw_user in response.users.items()}
