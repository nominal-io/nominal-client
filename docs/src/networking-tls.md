# Networking & TLS on corporate networks

The Nominal client talks to the platform over two transports: HTTP (conjure services) and gRPC
(e.g. the Role Service). Both verify the server's TLS certificate.

## How trust is established

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
