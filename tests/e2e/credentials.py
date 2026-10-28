"""Temporary credentials owned by the session fixture that creates each E2E client."""

from __future__ import annotations

from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.authorization.apikeys.v1 import api_keys_pb2


def with_temporary_api_key(client: NominalClient, request: pytest.FixtureRequest) -> NominalClient:
    """Opt into a one-day key, registering revocation before any subsequent setup can fail."""
    if not request.config.getoption("temporary_api_key"):
        return client

    with translate_grpc_errors():
        key = client._clients.api_keys.CreateApiKey(
            api_keys_pb2.CreateApiKeyRequest(
                api_key=api_keys_pb2.ApiKey(display_name=f"e2e-{uuid4().hex}"), expires_after_days=1
            )
        )

    def revoke_key() -> None:
        with translate_grpc_errors():
            client._clients.api_keys.DeleteApiKey(api_keys_pb2.DeleteApiKeyRequest(api_key_rid=key.api_key.rid))

    request.addfinalizer(revoke_key)

    security = client._clients._service_config.security
    return NominalClient.from_token(
        key.api_key_value,
        client._clients.api_base_url,
        workspace_rid=client._clients.workspace_rid,
        trust_store_path=security.trust_store_path if security is not None else None,
        connect_timeout=client._clients._service_config.connect_timeout,
        extra_headers=client._clients.header_provider,
    )
