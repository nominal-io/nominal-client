"""Temporary credentials owned by the session fixture that creates each E2E client."""

from __future__ import annotations

from functools import partial
from uuid import uuid4

import pytest
from nominal_api import authorization

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch


def with_temporary_api_key(client: NominalClient, request: pytest.FixtureRequest) -> NominalClient:
    """Opt into a one-day key, registering revocation before any subsequent setup can fail."""
    if not request.config.getoption("temporary_api_key"):
        return client

    clients = client._clients
    service = clients.authorization
    key = service.create_api_key(
        clients.auth_header,
        authorization.CreateApiKeyRequest(api_key_name=f"e2e-{uuid4().hex}", expires_after_days=1),
    )
    request.addfinalizer(partial(service.revoke_api_key, clients.auth_header, key.api_key_metadata.rid))

    return NominalClient(
        _clients=ClientsBunch.from_config(
            clients._service_config,
            clients.api_base_url,
            clients._user_agent,
            key.api_key_value,
            clients.workspace_rid,
            header_provider=clients.header_provider,
        ),
        _profile=client._profile,
    )
