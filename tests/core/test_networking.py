from __future__ import annotations

import gzip
import ssl
from unittest.mock import MagicMock, patch, sentinel

import pytest
import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration
from requests.adapters import HTTPAdapter

from nominal.core._utils.networking import (
    HeaderProviderSession,
    NominalRequestsAdapter,
    NominalSslRequestsAdapter,
    SslContextProvider,
    create_conjure_service_client,
    create_multipart_request_session,
)
from nominal.core.exceptions import HeaderConflictError


def _prepared_request(body: object) -> requests.PreparedRequest:
    return requests.Request("POST", "https://example.com", data=body).prepare()


class _FakeSslContextProvider(SslContextProvider):
    def __init__(self) -> None:
        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def create_ssl_context(self):
        return self.ssl_context


def test_gzip_adapter_updates_content_length_after_compression() -> None:
    """Content-Length must reflect the compressed body size, not the original."""
    adapter = NominalRequestsAdapter()
    request = _prepared_request("hello world" * 50)

    adapter.add_headers(request)

    with patch("nominal.core._utils.networking.NominalSslRequestsAdapter.send", autospec=True) as super_send:
        super_send.return_value = requests.Response()
        adapter.send(request)

    sent_request = super_send.call_args.args[1]
    compressed_body = sent_request.body

    assert isinstance(compressed_body, bytes)
    assert gzip.decompress(compressed_body) == ("hello world" * 50).encode("utf-8")
    assert sent_request.headers["Content-Encoding"] == "gzip"
    assert sent_request.headers["Content-Length"] == str(len(compressed_body))


def test_gzip_adapter_compresses_bytes_body() -> None:
    """Bytes bodies must be compressed directly without an encode step."""
    adapter = NominalRequestsAdapter()
    raw = b"binary payload" * 50
    request = _prepared_request(raw)

    adapter.add_headers(request)

    with patch("nominal.core._utils.networking.NominalSslRequestsAdapter.send", autospec=True) as super_send:
        super_send.return_value = requests.Response()
        adapter.send(request)

    sent_request = super_send.call_args.args[1]
    compressed_body = sent_request.body

    assert isinstance(compressed_body, bytes)
    assert gzip.decompress(compressed_body) == raw
    assert sent_request.headers["Content-Length"] == str(len(compressed_body))


def test_gzip_adapter_skips_compression_for_streaming_requests() -> None:
    """Streaming requests must pass through unmodified so the consumer controls the body."""
    adapter = NominalRequestsAdapter()
    request = _prepared_request("plain text body")
    original_body = request.body

    adapter.add_headers(request, stream=True)

    assert "Content-Encoding" not in request.headers

    with patch("nominal.core._utils.networking.NominalSslRequestsAdapter.send", autospec=True) as super_send:
        super_send.return_value = requests.Response()
        adapter.send(request, stream=True)

    assert super_send.call_args.args[1].body == original_body


def test_ssl_adapter_proxy_uses_own_ssl_context() -> None:
    """Proxied connections must use the adapter's ThreadSafeSSLContext, not any context supplied by the caller."""
    adapter = NominalSslRequestsAdapter()
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


def test_create_conjure_service_client_uses_ssl_context_from_provider() -> None:
    """API clients should use the ssl_context supplied by the provider."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(uris=["https://api.example.com"])
    provider = _FakeSslContextProvider()

    create_conjure_service_client(
        service_class=service_class,
        user_agent="test",
        service_config=service_config,
        ssl_context_provider=provider,
    )

    session = service_class.call_args.args[0]
    adapter = session.adapters["https://api.example.com"]
    assert adapter._ssl_context is provider.ssl_context
    session.close()


def test_create_multipart_request_session_uses_ssl_context_from_provider() -> None:
    """Object-store sessions should use the ssl_context supplied by the provider."""
    provider = _FakeSslContextProvider()

    session = create_multipart_request_session(
        pool_size=7,
        num_retries=3,
        ssl_context_provider=provider,
    )

    adapter = session.adapters["https://"]
    assert adapter._ssl_context is provider.ssl_context
    session.close()


def test_header_provider_session_evaluates_headers_per_request() -> None:
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
    class DynamicHeaders:
        def headers(self) -> dict[str, str]:
            return {"User-Agent": "provider-agent"}

    session = HeaderProviderSession(DynamicHeaders())
    session.headers["User-Agent"] = "session-agent"

    prepared = session.prepare_request(requests.Request("GET", "https://example.com"))

    assert prepared.headers["User-Agent"] == "provider-agent"
    session.close()
