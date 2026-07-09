"""End-to-end tests for the experimental HTTP/JSON transcoding shim against a live Nominal instance.

Each test drives a real gRPC-backed operation through with_http_shim and asserts it works over HTTP
(the workspace, registry, and containerized-extractor services), including impersonation composition.
Requires an instance whose transcoding routes are deployed; run opt-in like the rest of tests/e2e.
"""

from __future__ import annotations

import pytest

from nominal.core import NominalClient
from nominal.experimental import as_user
from nominal.experimental.grpc_hacks import with_http_shim


@pytest.fixture(scope="session")
def http_client(client: NominalClient) -> NominalClient:
    """The e2e client with its gRPC services routed over HTTP transcoding."""
    return with_http_shim(client)


def test_workspace_resolution_over_http_matches_grpc(client: NominalClient, http_client: NominalClient) -> None:
    """Resolving the default workspace over the shim returns the same workspace as gRPC."""
    assert http_client.get_workspace().rid == client.get_workspace().rid


def test_search_container_images_over_http(http_client: NominalClient) -> None:
    """Searching container images works over the shim (registry service via transcoding)."""
    http_client.search_container_images()


def test_search_containerized_extractors_over_http(http_client: NominalClient) -> None:
    """Searching containerized extractors works over the shim."""
    http_client.search_containerized_extractors()


def test_impersonation_composes_with_shim(client: NominalClient) -> None:
    """A shimmed, impersonating client sends the on-behalf-of header without auth errors over HTTP."""
    me = client.get_user()
    impersonated = with_http_shim(as_user(client, me.rid))
    # A shimmed workspace call carrying the on-behalf-of header must be accepted by the REST auth filter.
    assert impersonated.get_workspace().rid == client.get_workspace().rid
