# Networking & TLS on corporate networks

The Nominal client talks to the platform over two transports: HTTP (conjure services) and gRPC
(e.g. the Role Service). HTTPS URLs verify the server's TLS certificate on both transports.

## Local plaintext HTTP deployments

An `http://` API base URL is accepted only when its host is a literal loopback IP address:
IPv4 `127.0.0.0/8` (normally `127.0.0.1`) or IPv6 `::1`. Use `https://` for remote deployments.
Other HTTP hosts raise `NominalConfigError` during client construction, even if you only intend to use
Conjure HTTP APIs. URLs with embedded user information are rejected for both HTTP and HTTPS; supply
your API token separately.
There is no remote-plaintext override or automatic fallback from TLS to plaintext.

Use `127.0.0.1` or `[::1]` rather than `localhost` or another hostname; the check does not trust DNS
resolution. Loopback HTTP still sends credentials and custom headers without TLS encryption.
This client-wide restriction does not add TLS to the Conjure HTTP transport.

For example, connect to a local runtime using its HTTP ingress URL, including `/api`:

```python
client = NominalClient.from_token(token, base_url="http://127.0.0.1:20000/api")
dataset = client.create_dataset("x")
```

For HTTP URLs, gRPC services use plaintext HTTP/2 on the same host and port as the HTTP API.
The runtime ingress must support both ordinary HTTP and plaintext gRPC, including the workspace
service used to resolve the client's workspace. Desktop Core provides these routes through its
loopback ingress. Authentication, custom headers, retries, deadlines, and Nominal exception
translation apply to both plaintext and TLS gRPC calls. HTTPS URLs continue to use TLS.

## How trust is established for HTTPS

- **HTTP** uses your operating system's trust store directly (via `truststore`), plus certifi.
- **gRPC** cannot use the OS trust store on demand, so at startup the client builds a CA bundle by
  unioning certifi with the OS trust anchors it can read with the Python standard library:
    - **Windows** — the `ROOT` and `CA` system stores (including GPO/MDM-pushed enterprise CAs).
    - **Linux** — the system `ca-certificates` bundle (including CAs added via `update-ca-certificates`).
    - **macOS** — certifi only; the macOS Keychain is not auto-detected (see below).

On Windows and Linux this means the client **just works** behind a corporate TLS-inspecting proxy
whose CA is installed in the host trust store — no configuration needed.

## macOS behind a corporate proxy

If you are on macOS behind a TLS-inspecting proxy, export your corporate root CA to a PEM file and
point the client at it:

```python
from nominal.core import NominalClient

client = NominalClient.from_token(token, trust_store_path="/path/to/corp-ca.pem")
```

Alternatively, set `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/path/to/corp-ca.pem` in your environment.

## Troubleshooting

A TLS verification failure surfaces as a gRPC `UNAVAILABLE` error mentioning an SSL/handshake/certificate
problem. The fix is the same as above: supply your corporate CA via `trust_store_path` (or
`GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`).
