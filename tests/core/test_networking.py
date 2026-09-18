from __future__ import annotations

import io
from collections.abc import Iterator
from unittest.mock import MagicMock, patch, sentinel

import pytest
import requests
import zstandard
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration
from requests.adapters import HTTPAdapter

from nominal.core._utils.networking import (
    HeaderProviderSession,
    SslBypassRequestsAdapter,
    create_conjure_service_client,
)
from nominal.core.exceptions import HeaderConflictError


@pytest.fixture
def session() -> Iterator[requests.Session]:
    """Use the HTTP session configured for SDK service clients."""
    service_class = MagicMock()
    create_conjure_service_client(service_class, "test", ServiceConfiguration(uris=["https://example.com"]))
    with service_class.call_args.args[0] as configured_session:
        yield configured_session


@pytest.fixture
def transport() -> Iterator[MagicMock]:
    """Replace network I/O with a successful empty response."""
    response = requests.Response()
    response.status_code = 204
    response._content = b""
    with patch.object(SslBypassRequestsAdapter, "send", return_value=response) as send:
        yield send


@pytest.mark.parametrize("body", ["hello world", b"hello world"])
def test_requests_use_zstd(session: requests.Session, transport: MagicMock, body: str | bytes) -> None:
    """Text and byte uploads reach the server as correctly framed zstd payloads."""
    session.post("https://example.com", data=body)
    request = transport.call_args.args[0]
    assert zstandard.ZstdDecompressor().decompress(request.body) == b"hello world"
    assert request.headers["Content-Encoding"] == "zstd"
    assert request.headers["Content-Length"] == str(len(request.body))
    assert "Transfer-Encoding" not in request.headers
    assert request.headers["Accept-Encoding"] == "gzip"


def test_response_streaming_preserves_uncompressed_upload(session: requests.Session, transport: MagicMock) -> None:
    """Response streaming retains the existing uncompressed upload behavior."""
    session.post("https://example.com", data=b"payload", stream=True)
    request = transport.call_args.args[0]
    assert request.body == b"payload"
    assert "Content-Encoding" not in request.headers


@pytest.mark.parametrize("encoding", ["zstd", "identity"])
def test_explicit_request_encoding_is_preserved(session: requests.Session, transport: MagicMock, encoding: str) -> None:
    """Caller-encoded uploads keep their body and encoding while still requesting gzip responses."""
    body = zstandard.ZstdCompressor().compress(b"payload") if encoding == "zstd" else b"payload"
    session.post("https://example.com", data=body, headers={"Content-Encoding": encoding})
    request = transport.call_args.args[0]
    assert request.body == body
    assert request.headers["Content-Encoding"] == encoding
    assert request.headers["Accept-Encoding"] == "gzip"


def test_explicit_response_preference_is_preserved(session: requests.Session, transport: MagicMock) -> None:
    """Callers can request uncompressed responses while their uploads use zstd."""
    session.post("https://example.com", data=b"payload", headers={"Accept-Encoding": "identity"})
    request = transport.call_args.args[0]
    assert request.headers["Accept-Encoding"] == "identity"
    assert request.headers["Content-Encoding"] == "zstd"


@pytest.mark.parametrize("status", [302, 307])
def test_redirect_preserves_valid_request_encoding(
    session: requests.Session, transport: MagicMock, status: int
) -> None:
    """Redirected uploads remain decodable, and redirected GETs carry no body encoding."""
    redirect = requests.Response()
    redirect.status_code = status
    redirect.headers["Location"] = "https://example.com/redirected"
    redirect._content = b""
    transport.side_effect = [redirect, transport.return_value]
    session.post("https://example.com", data=b"payload")
    request = transport.call_args.args[0]
    if status == 307:
        assert request.method == "POST"
        assert request.headers["Content-Encoding"] == "zstd"
        assert zstandard.ZstdDecompressor().decompress(request.body) == b"payload"
    else:
        assert request.method == "GET"
        assert request.body is None
        assert "Content-Encoding" not in request.headers


@pytest.mark.parametrize("kind", ["file", "iterator"])
def test_upload_streams_remain_readable(session: requests.Session, transport: MagicMock, kind: str) -> None:
    """File and iterator uploads reach the transport without being consumed or compressed."""
    body = io.BytesIO(b"payload") if kind == "file" else iter([b"pay", b"load"])
    session.post("https://example.com", data=body)
    request = transport.call_args.args[0]
    assert b"".join(request.body) == b"payload"
    assert "Content-Encoding" not in request.headers


def test_ssl_adapter_proxy_uses_own_ssl_context() -> None:
    """Proxied connections must use the adapter's ThreadSafeSSLContext, not any context supplied by the caller."""
    adapter = SslBypassRequestsAdapter()
    foreign_ctx = MagicMock()

    with patch.object(HTTPAdapter, "proxy_manager_for", autospec=True, return_value=MagicMock()) as super_proxy:
        adapter.proxy_manager_for("https://proxy.example.com", ssl_context=foreign_ctx)

    kwargs = super_proxy.call_args.kwargs
    assert kwargs["ssl_context"] is adapter._ssl_context
    assert kwargs["ssl_context"] is not foreign_ctx


def test_create_conjure_service_client_passes_trust_store_path_as_verify() -> None:
    """A service config with a security section should pass its trust_store_path as verify."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(
        security=SslConfiguration(trust_store_path="/etc/ssl/ca.pem"),
        uris=["https://api.example.com"],
    )

    create_conjure_service_client(
        service_class=service_class,
        user_agent="test",
        service_config=service_config,
    )

    session, _uris, _ct, _rt, verify, _rn = service_class.call_args.args
    assert verify == "/etc/ssl/ca.pem"
    session.close()


def test_create_conjure_service_client_passes_none_verify_when_security_is_absent() -> None:
    """A service config without a security section should produce a client with verify=None."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(uris=["https://api.example.com"])

    create_conjure_service_client(
        service_class=service_class,
        user_agent="test",
        service_config=service_config,
    )

    session, _uris, _ct, _rt, verify, _rn = service_class.call_args.args
    assert verify is None
    session.close()


def test_header_provider_session_evaluates_headers_per_request() -> None:
    """Dynamic headers are refreshed for each request."""

    class DynamicHeaders:
        value = "first"

        def headers(self) -> dict[str, str]:
            return {"X-Test": self.value}

    provider = DynamicHeaders()
    session = HeaderProviderSession(provider)

    first = session.prepare_request(requests.Request("GET", "https://example.com"))
    provider.value = "second"
    second = session.prepare_request(requests.Request("GET", "https://example.com"))

    assert first.headers["X-Test"] == "first"
    assert second.headers["X-Test"] == "second"
    session.close()


def test_header_provider_session_raises_for_explicit_request_header_conflict() -> None:
    """Explicit request headers cannot be overwritten by a header provider."""

    class DynamicHeaders:
        def headers(self) -> dict[str, str]:
            return {"X-Test": "default"}

    session = HeaderProviderSession(DynamicHeaders())

    with pytest.raises(
        HeaderConflictError,
        match="HeaderProvider returned header 'X-Test', but the request already set that header; "
        "HeaderProvider cannot override explicit request headers.",
    ):
        session.prepare_request(requests.Request("GET", "https://example.com", headers={"X-Test": "explicit"}))
    session.close()


def test_header_provider_session_can_override_session_default_headers() -> None:
    """Header providers can override session defaults."""

    class DynamicHeaders:
        def headers(self) -> dict[str, str]:
            return {"User-Agent": "provider-agent"}

    session = HeaderProviderSession(DynamicHeaders())
    session.headers["User-Agent"] = "session-agent"

    prepared = session.prepare_request(requests.Request("GET", "https://example.com"))

    assert prepared.headers["User-Agent"] == "provider-agent"
    session.close()
