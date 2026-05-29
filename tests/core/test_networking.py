from __future__ import annotations

import gzip
import ssl
from unittest.mock import MagicMock, patch, sentinel

import pytest
import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nominal.core import TransportProvider
from nominal.core._utils.networking import (
    HeaderProviderSession,
    NominalRequestsAdapter,
    NominalSslRequestsAdapter,
    ThreadSafeSSLContext,
    create_conjure_service_client,
    create_multipart_request_session,
)
from nominal.core.exceptions import HeaderConflictError


def _prepared_request(body: object) -> requests.PreparedRequest:
    return requests.Request("POST", "https://example.com", data=body).prepare()


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


def test_ssl_adapter_stores_provided_ssl_context() -> None:
    """An explicitly provided ssl_context should be stored as-is and not replaced."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    adapter = NominalSslRequestsAdapter(ssl_context=ctx)
    assert adapter._ssl_context is ctx


def test_ssl_adapter_defaults_to_thread_safe_ssl_context_when_none_provided() -> None:
    """When no ssl_context is given the adapter should create a ThreadSafeSSLContext for thread safety."""
    adapter = NominalSslRequestsAdapter()
    assert isinstance(adapter._ssl_context, ThreadSafeSSLContext)


def test_create_conjure_service_client_calls_create_http_adapter_exactly_once() -> None:
    """The provider's create_http_adapter() must be called once at session build time, not per-request."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(uris=["https://api.example.com"])
    provider = MagicMock(spec=TransportProvider)
    provider.create_http_adapter.return_value = MagicMock(spec=HTTPAdapter)

    create_conjure_service_client(
        service_class=service_class,
        user_agent="test",
        service_config=service_config,
        transport_provider=provider,
    )

    provider.create_http_adapter.assert_called_once()
    service_class.call_args.args[0].close()


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


def test_create_multipart_request_session_uses_custom_adapter_from_provider() -> None:
    """When create_multipart_adapter() returns an adapter it must be mounted for https://."""
    custom_adapter = MagicMock(spec=HTTPAdapter)
    provider = MagicMock(spec=TransportProvider)
    provider.create_multipart_adapter.return_value = custom_adapter

    session = create_multipart_request_session(
        pool_size=4,
        num_retries=3,
        transport_provider=provider,
    )

    assert session.adapters["https://"] is custom_adapter
    provider.create_multipart_adapter.assert_called_once()
    session.close()


def test_create_multipart_request_session_does_not_call_http_adapter() -> None:
    """Multipart sessions must not call create_http_adapter() — that's for API traffic only."""
    provider = MagicMock(spec=TransportProvider)
    provider.create_multipart_adapter.return_value = MagicMock(spec=HTTPAdapter)

    session = create_multipart_request_session(
        pool_size=4,
        num_retries=3,
        transport_provider=provider,
    )

    provider.create_http_adapter.assert_not_called()
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


def test_create_conjure_service_client_uses_custom_adapter_from_provider() -> None:
    """When create_http_adapter() returns an adapter it must be mounted for the service URI."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(uris=["https://api.example.com"])

    custom_adapter = MagicMock(spec=HTTPAdapter)
    provider = MagicMock(spec=TransportProvider)
    provider.create_http_adapter.return_value = custom_adapter

    create_conjure_service_client(
        service_class=service_class,
        user_agent="custom-agent",
        service_config=service_config,
        transport_provider=provider,
    )

    session = service_class.call_args.args[0]
    assert session.adapters["https://api.example.com"] is custom_adapter
    assert session.headers["User-Agent"] == "custom-agent"
    session.close()


def test_create_conjure_service_client_trust_store_passed_through_for_custom_adapter() -> None:
    """The trust store path from ServiceConfiguration must be forwarded even when using a custom adapter."""
    service_class = MagicMock(return_value=sentinel.client)
    service_config = ServiceConfiguration(
        security=SslConfiguration(trust_store_path="/etc/ssl/ca.pem"),
        uris=["https://api.example.com"],
    )
    provider = MagicMock(spec=TransportProvider)
    provider.create_http_adapter.return_value = MagicMock(spec=HTTPAdapter)

    create_conjure_service_client(
        service_class=service_class,
        user_agent="test",
        service_config=service_config,
        transport_provider=provider,
    )

    _session, _uris, _ct, _rt, verify, _rn = service_class.call_args.args
    assert verify == "/etc/ssl/ca.pem"
    _session.close()


def test_transport_provider_default_multipart_adapter_uses_thread_safe_ssl_context() -> None:
    """The default multipart adapter is a NominalSslRequestsAdapter with a ThreadSafeSSLContext."""
    provider = TransportProvider()

    adapter = provider.create_multipart_adapter(max_retries=Retry(total=3), pool_size=5)

    assert isinstance(adapter, NominalSslRequestsAdapter)
    assert isinstance(adapter._ssl_context, ThreadSafeSSLContext)


def test_transport_provider_default_http_adapter_is_nominal_requests_adapter() -> None:
    """The default HTTP adapter is a NominalRequestsAdapter (gzip-enabled)."""
    provider = TransportProvider()

    adapter = provider.create_http_adapter(max_retries=Retry(total=3))

    assert isinstance(adapter, NominalRequestsAdapter)
    assert isinstance(adapter._ssl_context, ThreadSafeSSLContext)
