from __future__ import annotations

from nominal.core._clientsbunch import ON_BEHALF_OF_USER_RID_HEADER
from nominal.core.client import NominalClient


def as_user(client: NominalClient, user_rid: str) -> NominalClient:
    """Return an experimental derived client for user impersonation.

    The returned client injects the on-behalf-of header for all service requests.
    """
    security = client._clients._service_config.security
    return NominalClient.from_token(
        client._clients._token,
        client._clients._api_base_url,
        workspace_rid=client._clients.workspace_rid,
        trust_store_path=security.trust_store_path if security is not None else None,
        connect_timeout=client._clients._service_config.connect_timeout,
        extra_headers={ON_BEHALF_OF_USER_RID_HEADER: user_rid},
        ssl_context_provider=client._clients.ssl_context_provider,
        _profile=client._profile,
    )
