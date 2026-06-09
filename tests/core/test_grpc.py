from __future__ import annotations

from typing import Any

import pytest
from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import RoleServiceClient
from nominal.core._grpc import GrpcClient, api_base_url_to_grpc_target
from nominal.core._utils.networking import StaticHeaderProvider
from nominal.core.exceptions import HeaderConflictError


class _FakeChannel:
    def __enter__(self) -> "_FakeChannel":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeStub:
    def __init__(self, channel: _FakeChannel) -> None:
        self.channel = channel

    def get(
        self,
        request: object,
        *,
        metadata: tuple[tuple[str, str], ...],
    ) -> tuple[object, tuple[tuple[str, str], ...]]:
        return request, metadata


def _client(*, extra_headers: dict[str, str] | None = None) -> GrpcClient:
    return GrpcClient(
        auth_header="Bearer token",
        api_base_url="https://api.gov.nominal.io/api",
        service_config=ServiceConfiguration(uris=["https://api.gov.nominal.io/api"]),
        user_agent="test-agent",
        header_provider=None if extra_headers is None else StaticHeaderProvider(extra_headers),
    )


def test_api_base_url_to_grpc_target_strips_scheme_and_api_suffix() -> None:
    assert api_base_url_to_grpc_target("https://api.gov.nominal.io/api") == "api.gov.nominal.io"
    assert api_base_url_to_grpc_target("https://api.gov.nominal.io") == "api.gov.nominal.io"
    assert api_base_url_to_grpc_target("http://localhost:8080/api") == "localhost:8080"


def test_grpc_client_invokes_stub_with_auth_and_extra_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    import grpc  # type: ignore[import-untyped]

    secure_channel_calls: list[tuple[str, object, tuple[tuple[str, str], ...]]] = []

    def fake_secure_channel(
        target: str,
        credentials: object,
        *,
        options: tuple[tuple[str, str], ...],
    ) -> _FakeChannel:
        secure_channel_calls.append((target, credentials, options))
        return _FakeChannel()

    monkeypatch.setattr(grpc, "secure_channel", fake_secure_channel)
    monkeypatch.setattr(GrpcClient, "_channel_credentials", lambda self, grpc_module: "creds")

    request = object()
    response_request, metadata = _client(extra_headers={"X-Nominal-Test": "value"}).invoke(
        _FakeStub,
        lambda stub: stub.get,
        request,
    )

    assert response_request is request
    assert metadata == (("authorization", "Bearer token"), ("x-nominal-test", "value"))
    assert secure_channel_calls == [("api.gov.nominal.io", "creds", (("grpc.primary_user_agent", "test-agent"),))]


def test_grpc_client_rejects_authorization_header_override() -> None:
    with pytest.raises(HeaderConflictError, match="Authorization"):
        _client(extra_headers={"Authorization": "Bearer other"})._metadata()


def test_role_service_client_builds_get_resource_roles_request(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeGrpcClient:
        def invoke(self, stub_class: type[Any], method: object, request: object) -> object:
            del stub_class, method
            assert request.resource == "ri.catalog.dataset.test"  # type: ignore[attr-defined]
            return object()

    assert RoleServiceClient(FakeGrpcClient()).get_resource_roles("ri.catalog.dataset.test") is not None  # type: ignore[arg-type]
