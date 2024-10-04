from dataclasses import dataclass

from .._api.combined import authentication_api
from ._clientsbunch import ClientsBunch
from ._utils import HasRid


@dataclass(frozen=True)
class User(HasRid):
    rid: str
    display_name: str
    email: str


def _get_user(clients: ClientsBunch) -> User:
    """Retrieve the user with the set auth token"""
    response = clients.authentication.get_my_profile(clients.auth_header)
    return User(rid=response.rid, display_name=response.display_name, email=response.email)


def _get_user_rid_from_email(clients: ClientsBunch, user_email: str) -> str:
    request = authentication_api.SearchUsersRequest(
        query=authentication_api.SearchUsersQuery(
            exact_match=user_email,
        )
    )
    response = clients.authentication.search_users_v2(clients.auth_header, request)
    if len(response.results) == 0:
        raise ValueError(f"user {user_email!r} not found")
    if len(response.results) > 1:
        raise ValueError(f"found multiple users with email {user_email!r}")
    return response.results[0].rid
