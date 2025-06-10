from __future__ import annotations

from dataclasses import dataclass

from nominal_api import authentication_api
from typing_extensions import Self

from nominal.core._utils import HasRid


@dataclass(frozen=True)
class User(HasRid):
    rid: str
    display_name: str
    email: str

    @classmethod
    def _from_conjure(cls, raw_user: authentication_api.UserV2) -> Self:
        return cls(rid=raw_user.rid, display_name=raw_user.display_name, email=raw_user.email)


def _get_user(
    auth_header: str,
    client: authentication_api.AuthenticationServiceV2,
) -> User:
    """Retrieve the user with the set auth token"""
    response = client.get_my_profile(auth_header)
    return User._from_conjure(response)


def _get_user_rid_from_email(
    auth_header: str, client: authentication_api.AuthenticationServiceV2, user_email: str
) -> str:
    request = authentication_api.SearchUsersRequest(
        query=authentication_api.SearchUsersQuery(
            exact_match=user_email,
        )
    )
    response = client.search_users_v2(auth_header, request)
    if len(response.results) == 0:
        raise ValueError(f"user {user_email!r} not found")
    if len(response.results) > 1:
        raise ValueError(f"found multiple users with email {user_email!r}")
    return response.results[0].rid


def _get_user_with_fallback(
    auth_header: str,
    client: authentication_api.AuthenticationServiceV2,
    user_email: str | None,
    user_rid: str | None,
) -> str:
    """Get the user RID for the user, falling back to the current user if not provided.

    If both user_email and user_rid are provided, raise a ValueError.
    """
    if user_email is not None and user_rid is not None:
        raise ValueError("only one of user_email or user_rid should be provided")
    if user_email is not None:
        return _get_user_rid_from_email(auth_header, client, user_email)
    if user_rid is not None:
        return user_rid
    return _get_user(auth_header, client).rid
