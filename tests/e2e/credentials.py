"""Temporary credentials owned by the session fixture that creates each E2E client."""

from __future__ import annotations

import os
import sys
from functools import partial
from uuid import uuid4

import pytest
from nominal_api import authorization

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.networking import create_conjure_service_client


def with_temporary_api_key(client: NominalClient, request: pytest.FixtureRequest) -> NominalClient:
    """Opt into a one-day key, registering revocation before any subsequent setup can fail."""
    if not request.config.getoption("temporary_api_key"):
        return client

    clients = client._clients
    service = create_conjure_service_client(
        authorization.AuthorizationService,
        clients._user_agent,
        clients._service_config,
        header_provider=clients.header_provider,
    )
    key = service.create_api_key(
        clients.auth_header,
        authorization.CreateApiKeyRequest(api_key_name=f"e2e-{uuid4().hex}", expires_after_days=1),
    )
    request.addfinalizer(partial(service.revoke_api_key, clients.auth_header, key.api_key_metadata.rid))

    if os.environ.get("GITHUB_ACTIONS") == "true":
        # xdist forwards uncaptured stderr; ordinary worker stdout never reaches the runner.
        capture = request.config.pluginmanager.getplugin("capturemanager")
        with capture.global_and_fixture_disabled():
            print(f"::add-mask::{key.api_key_value}", file=sys.stderr, flush=True)

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
