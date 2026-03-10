from dataclasses import fields

from nominal.core._clientsbunch import (
    ON_BEHALF_OF_USER_RID_HEADER,
    ClientsBunch,
    api_base_url_to_app_base_url,
)
from nominal.core.client import NominalClient
from nominal.experimental import as_user


class _FakeSession:
    def __init__(self) -> None:
        self.headers = {"User-Agent": "test-agent"}


class _FakeCatalogService:
    def __init__(self) -> None:
        self._requests_session = _FakeSession()


def test_api_app_url_conversion():
    c = api_base_url_to_app_base_url
    assert c("https://api.gov.nominal.io/api") == "https://app.gov.nominal.io"
    assert c("https://api-staging.gov.nominal.io/api") == "https://app-staging.gov.nominal.io"
    assert c("https://api.nominal.test") == "https://app.nominal.test"
    assert c("https://api-customer.eu.nominal.io/api") == "https://app-customer.eu.nominal.io"
    assert c("https://api-customer.gov.nominal.io/api") == "https://app-customer.gov.nominal.io"
    assert c("https://api.nominal.gov.deployment.customer.com/api") == "https://app.nominal.gov.deployment.customer.com"
    assert c("https://api.nominal.customer.internal/api") == "https://app.nominal.customer.internal"
    assert c("https://unknown") == ""


def test_with_catalog_request_headers_clones_only_catalog_session():
    catalog = _FakeCatalogService()
    kwargs = {field.name: object() for field in fields(ClientsBunch)}
    kwargs["auth_header"] = "Bearer token"
    kwargs["workspace_rid"] = None
    kwargs["app_base_url"] = "https://app.nominal.test"
    kwargs["catalog"] = catalog
    clients = ClientsBunch(**kwargs)

    cloned = clients.with_catalog_request_headers({ON_BEHALF_OF_USER_RID_HEADER: "ri.authn.dev.user.target"})

    assert cloned is not clients
    assert cloned.catalog is not clients.catalog
    assert cloned.catalog._requests_session is not clients.catalog._requests_session
    assert ON_BEHALF_OF_USER_RID_HEADER not in clients.catalog._requests_session.headers
    assert cloned.catalog._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == "ri.authn.dev.user.target"
    assert cloned.catalog._requests_session.headers["User-Agent"] == "test-agent"


def test_experimental_as_user_returns_derived_nominal_client():
    catalog = _FakeCatalogService()
    kwargs = {field.name: object() for field in fields(ClientsBunch)}
    kwargs["auth_header"] = "Bearer token"
    kwargs["workspace_rid"] = None
    kwargs["app_base_url"] = "https://app.nominal.test"
    kwargs["catalog"] = catalog
    client = NominalClient(_clients=ClientsBunch(**kwargs))

    impersonated = as_user(client, "ri.authn.dev.user.target")

    assert isinstance(impersonated, NominalClient)
    assert impersonated is not client
    assert ON_BEHALF_OF_USER_RID_HEADER not in client._clients.catalog._requests_session.headers
    assert impersonated._clients.catalog._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == (
        "ri.authn.dev.user.target"
    )
