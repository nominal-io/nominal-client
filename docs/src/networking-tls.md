---
myst:
  html_meta:
    description: "Connect through corporate TLS-inspecting proxies, and to local plaintext deployments."
---

# Networking and TLS

{.lead}
The client verifies Nominal's TLS certificate on both of its connections, HTTP and gRPC.
On Windows and Linux it trusts the certificate authorities installed on the machine. On macOS, behind a TLS-inspecting proxy, you give it your corporate CA.

## Corporate proxies

HTTP uses the operating system's trust store and [certifi](https://pypi.org/project/certifi/)'s certificate authorities.
gRPC can't use the operating system's trust store directly, so the client builds a bundle for it from certifi plus:

- **Windows:** the `ROOT` and `CA` system stores, including CAs installed by group policy or device management.
- **Linux:** the system `ca-certificates` bundle, including CAs added with `update-ca-certificates`.
- **macOS:** nothing more, because it can't read the Keychain.

So on Windows and Linux, a proxy whose CA is installed on the machine needs no setup.
On macOS, export the corporate root CA as a PEM file and pass it to the client:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default", trust_store_path="/path/to/corp-ca.pem")
```

Or set `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/path/to/corp-ca.pem` in the environment.

A missing CA shows up as a gRPC `UNAVAILABLE` error that mentions SSL, a handshake or a certificate.

## Local plaintext deployments

An `http://` base URL works only for a loopback IP address: `127.0.0.1`, or another `127.x.x.x` address, or `[::1]`.
Use the address, not `localhost`. Any other `http://` host raises `NominalConfigError`, and there's no fallback from TLS to plaintext.

```python
client = NominalClient.from_token(token, base_url="http://127.0.0.1:20000/api")
```

gRPC then uses plaintext HTTP/2 on the same host and port, so the local deployment must serve both HTTP and gRPC there, as Desktop Core does.
Plaintext still sends your API key and headers unencrypted.

Base URLs with a user name or password in them are rejected. Pass the API key separately.
