"""Temporary credentials owned by the session fixture that creates each E2E client."""

from __future__ import annotations

from functools import partial
from uuid import uuid4

import pytest
from nominal_api import authorization

from nominal.core import NominalClient


def with_temporary_api_key(client: NominalClient, request: pytest.FixtureRequest) -> NominalClient:
    """Opt into a one-day key, registering revocation before any subsequent setup can fail."""
    if not request.config.getoption("temporary_api_key"):
        return client

    key = client._clients.authorization.create_api_key(
        client._clients.auth_header,
        authorization.CreateApiKeyRequest(api_key_name=f"e2e-{uuid4().hex}", expires_after_days=1),
    )
    request.addfinalizer(
        partial(client._clients.authorization.revoke_api_key, client._clients.auth_header, key.api_key_metadata.rid)
    )

    security = client._clients._service_config.security
    return NominalClient.from_token(
        key.api_key_value,
        client._clients.api_base_url,
        workspace_rid=client._clients.workspace_rid,
        trust_store_path=security.trust_store_path if security is not None else None,
        connect_timeout=client._clients._service_config.connect_timeout,
        extra_headers=client._clients.header_provider,
    )
