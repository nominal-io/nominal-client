"""Exercise the adapter over real HTTP sockets, including urllib3 retries and Requests redirects."""

from __future__ import annotations

import gzip
import io
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core._utils.networking import NominalRequestsAdapter, create_conjure_service_client


@pytest.fixture
def http_server():
    """Record complete request bodies without mocking Requests or urllib3."""
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.headers.get("Transfer-Encoding") == "chunked" and "Content-Length" not in self.headers:
                chunks = []
                while size := int(self.rfile.readline().strip(), 16):
                    chunks.append(self.rfile.read(size))
                    self.rfile.read(2)
                self.rfile.readline()
                body = b"".join(chunks)
            else:
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            received.append((self.command, self.path, dict(self.headers), body))
            attempts = sum(path == self.path for _, path, _, _ in received)
            if self.path == "/retry" and attempts == 1:
                status = 503
            elif self.path == "/fail":
                status = 503
            elif self.path.startswith("/redirect/"):
                status = int(self.path.rsplit("/", 1)[1])
            else:
                status = 204
            self.send_response(status)
            if 300 <= status < 400:
                self.send_header("Location", "/final")
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = do_POST

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", received
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


def test_configured_post_retry_reuses_encoded_bytes(http_server):
    """The configured Conjure client retries POST once with identical encoded bytes and headers."""
    url, received = http_server
    service = create_conjure_service_client(
        ProtoWriteService,
        "test-agent",
        ServiceConfiguration(
            uris=[url],
            max_num_retries=1,
            backoff_slot_size=0,
            connect_timeout=2,
            read_timeout=2,
        ),
    )
    service._request("POST", url + "/retry", headers={"Authorization": "Bearer synthetic"}, data=b"payload" * 100)
    assert len(received) == 2
    assert received[0] == received[1]
    _, _, headers, body = received[0]
    assert gzip.decompress(body) == b"payload" * 100
    assert headers["Content-Length"] == str(len(body))
    assert "Transfer-Encoding" not in headers
    assert headers["Authorization"] == "Bearer synthetic"
    assert headers["User-Agent"] == "test-agent"


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirect_wire_body_matches_encoding(http_server, status):
    """A redirected request's actual wire body agrees with its encoding and framing headers."""
    url, received = http_server
    with requests.Session() as session:
        session.mount(url, NominalRequestsAdapter())
        response = session.post(url + f"/redirect/{status}", data=b"payload", timeout=2)
    assert response.status_code == 204
    assert len(received) == 2
    method, _, headers, body = received[1]
    if status in (307, 308):
        assert method == "POST"
        assert body == received[0][3]
        assert gzip.decompress(body) == b"payload"
        assert headers["Content-Length"] == str(len(body))
    else:
        assert method == "GET"
        assert not body
        assert "Content-Encoding" not in headers


@pytest.mark.parametrize("kind", ["file", "generator"])
def test_upload_stream_is_not_buffered_or_compressed(http_server, kind):
    """File and generator uploads retain Requests' native streaming and framing behavior."""
    url, received = http_server
    payload = b"hello world" * 50
    body = io.BytesIO(payload) if kind == "file" else iter([payload[:100], payload[100:]])
    with requests.Session() as session:
        session.mount(url, NominalRequestsAdapter())
        response = session.post(url + "/upload", data=body, timeout=2)
    assert response.status_code == 204
    assert received[0][3] == payload
    headers = received[0][2]
    assert "Content-Encoding" not in headers
    if kind == "file":
        assert headers["Content-Length"] == str(len(payload))
        assert "Transfer-Encoding" not in headers
    else:
        assert headers["Transfer-Encoding"] == "chunked"
        assert "Content-Length" not in headers


def test_buffered_compression_has_one_framing_header(http_server):
    """Replacing a buffered body clears stale chunked framing before setting Content-Length."""
    url, received = http_server
    with requests.Session() as session:
        session.mount(url, NominalRequestsAdapter())
        response = session.post(url, data=b"payload", headers={"Transfer-Encoding": "chunked"}, timeout=2)
    assert response.status_code == 204
    headers, body = received[0][2:]
    assert "Transfer-Encoding" not in headers
    assert headers["Content-Length"] == str(len(body))
    assert gzip.decompress(body) == b"payload"


def test_shared_adapter_compresses_concurrent_requests_independently(http_server):
    """Concurrent sends use independent bodies and codec state."""
    url, received = http_server
    adapter = NominalRequestsAdapter()

    def upload(index):
        request = requests.Request("POST", url + f"/{index}", data=str(index) * 1000).prepare()
        with adapter.send(request, timeout=2) as response:
            assert response.status_code == 204

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(upload, range(8)))
    finally:
        adapter.close()
    assert len(received) == 8
    for _, path, headers, body in received:
        assert gzip.decompress(body) == (path[1:] * 1000).encode()
        assert headers["Content-Length"] == str(len(body))


@pytest.mark.parametrize("path", ["/fail", "/redirect/308"])
def test_configured_retry_policy_remains_bounded(http_server, path):
    """Configured status retries remain bounded, including the existing 308 policy."""
    url, received = http_server
    service = create_conjure_service_client(
        ProtoWriteService,
        "test-agent",
        ServiceConfiguration(
            uris=[url],
            max_num_retries=1,
            backoff_slot_size=0,
            connect_timeout=2,
            read_timeout=2,
        ),
    )
    with pytest.raises(requests.exceptions.RetryError):
        service._request("POST", url + path, data=b"payload")
    assert len(received) == 2
    assert received[0] == received[1]
