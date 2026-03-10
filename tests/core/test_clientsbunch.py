from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import (
    ON_BEHALF_OF_USER_RID_HEADER,
    ClientsBunch,
    api_base_url_to_app_base_url,
)
from nominal.core.client import NominalClient
from nominal.experimental import as_user


class _FakeSession:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = {"User-Agent": "test-agent", **(headers or {})}


class _FakeCatalogService:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self._requests_session = _FakeSession(headers)


class _FakeService:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self._requests_session = _FakeSession(headers)


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


def test_with_catalog_request_headers_recreates_clients_from_config(monkeypatch):
    def fake_create_conjure_client_factory(
        *,
        user_agent,
        service_config,
        return_none_for_unknown_union_types=False,
        default_headers=None,
    ):
        del user_agent, service_config, return_none_for_unknown_union_types

        def factory(service_class):
            if service_class.__name__ == "CatalogService":
                return _FakeCatalogService(default_headers)
            return _FakeService(default_headers)

        return factory

    monkeypatch.setattr("nominal.core._clientsbunch.create_conjure_client_factory", fake_create_conjure_client_factory)

    clients = ClientsBunch.from_config(
        ServiceConfiguration(uris=["https://api.nominal.test"]),
        "https://api.nominal.test",
        "test-agent",
        "token",
        None,
    )

    cloned = clients.with_catalog_request_headers({ON_BEHALF_OF_USER_RID_HEADER: "ri.authn.dev.user.target"})

    assert cloned is not clients
    assert cloned.catalog is not clients.catalog
    assert ON_BEHALF_OF_USER_RID_HEADER not in clients.catalog._requests_session.headers
    assert cloned.catalog._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == "ri.authn.dev.user.target"
    assert cloned.catalog._requests_session.headers["User-Agent"] == "test-agent"
    assert ON_BEHALF_OF_USER_RID_HEADER not in cloned.assets._requests_session.headers


def test_experimental_as_user_returns_derived_nominal_client(monkeypatch):
    def fake_create_conjure_client_factory(
        *,
        user_agent,
        service_config,
        return_none_for_unknown_union_types=False,
        default_headers=None,
    ):
        del user_agent, service_config, return_none_for_unknown_union_types

        def factory(service_class):
            if service_class.__name__ == "CatalogService":
                return _FakeCatalogService(default_headers)
            return _FakeService(default_headers)

        return factory

    monkeypatch.setattr("nominal.core._clientsbunch.create_conjure_client_factory", fake_create_conjure_client_factory)

    client = NominalClient(
        _clients=ClientsBunch.from_config(
            ServiceConfiguration(uris=["https://api.nominal.test"]),
            "https://api.nominal.test",
            "test-agent",
            "token",
            None,
        )
    )

    impersonated = as_user(client, "ri.authn.dev.user.target")

    assert isinstance(impersonated, NominalClient)
    assert impersonated is not client
    assert ON_BEHALF_OF_USER_RID_HEADER not in client._clients.catalog._requests_session.headers
    assert impersonated._clients.catalog._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == (
        "ri.authn.dev.user.target"
    )
