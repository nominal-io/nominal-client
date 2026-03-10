from __future__ import annotations

from nominal.core._clientsbunch import ON_BEHALF_OF_USER_RID_HEADER
from nominal.core.client import NominalClient


def as_user(client: NominalClient, user_rid: str) -> NominalClient:
    """Return an experimental derived client for user impersonation.

    The returned client currently injects the on-behalf-of header only for catalog-backed operations.
    """
    clients = client._clients.with_catalog_request_headers({ON_BEHALF_OF_USER_RID_HEADER: user_rid})
    return NominalClient(_clients=clients, _profile=client._profile)
