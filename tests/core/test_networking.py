from __future__ import annotations

import gzip
from unittest.mock import MagicMock, patch, sentinel

import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration
from requests.adapters import HTTPAdapter

from nominal.core._utils.networking import (
    NominalRequestsAdapter,
    SslBypassRequestsAdapter,
    create_conjure_service_client,
)


def _prepared_request(body: object) -> requests.PreparedRequest:
    return requests.Request("POST", "https://example.com", data=body).prepare()


def test_gzip_adapter_updates_content_length_after_compression() -> None:
    """Content-Length must reflect the compressed body size, not the original."""
    adapter = NominalRequestsAdapter()
    request = _prepared_request("hello world" * 50)

    adapter.add_headers(request)

    with patch("nominal.core._utils.networking.SslBypassRequestsAdapter.send", autospec=True) as super_send:
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

    with patch("nominal.core._utils.networking.SslBypassRequestsAdapter.send", autospec=True) as super_send:
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

    with patch("nominal.core._utils.networking.SslBypassRequestsAdapter.send", autospec=True) as super_send:
        super_send.return_value = requests.Response()
        adapter.send(request, stream=True)

    assert super_send.call_args.args[1].body == original_body


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
