"""gRPC transport plumbing for the Nominal client.

This module is the gRPC analogue of the conjure stub factory in `_utils.networking`: it builds a single,
shared, fully-configured `grpc.Channel` and returns a `create_grpc_stub_factory(StubClass) -> stub`
closure, so each backend gRPC service is exposed as a generated stub bound to that one channel.

The channel is configured to track the conjure HTTP transport as closely as gRPC allows:

- TLS roots are the union of the configured trust store and the OS trust store (`_grpc_root_certificates`).
- Retry mirrors conjure's `RetryWithJitter` (`_service_config_json`).
- Per-call auth metadata and a default deadline are injected by client interceptors, so call sites never
  pass `metadata=` / `timeout=` themselves.
"""

from __future__ import annotations

import json
import ssl
import sys
from abc import abstractmethod
from collections import namedtuple
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, TypeVar
from urllib.parse import urlparse

import grpc  # type: ignore[import-untyped]
from conjure_python_client import ServiceConfiguration

from nominal.core._utils.networking import HeaderProvider, raise_header_conflict

TStub = TypeVar("TStub")

# gRPC channel-arg ints are int32; 2**31 - 1 (~2 GiB) lifts the 4 MB default receive cap without overflowing.
_MAX_MESSAGE_LENGTH = 2**31 - 1
# gRPC status codes we retry, mapped from conjure's retryable HTTP statuses (503 -> UNAVAILABLE,
# 429 -> RESOURCE_EXHAUSTED; 308 has no gRPC analogue). Connection/read failures also surface as UNAVAILABLE.
_RETRYABLE_STATUS = ("UNAVAILABLE", "RESOURCE_EXHAUSTED")
# Backoff ceiling; matches urllib3 Retry.DEFAULT_BACKOFF_MAX, the cap conjure's RetryWithJitter uses.
_MAX_BACKOFF_S = 120


def api_base_url_to_grpc_target(api_base_url: str) -> str:
    """Derive a gRPC target (``host:port``) from an API base URL, e.g. ``https://api.x/api`` -> ``api.x``."""
    parsed = urlparse(api_base_url)
    if not parsed.netloc:
        raise ValueError(f"Could not derive gRPC target from API base URL: {api_base_url}")
    return parsed.netloc


@lru_cache(maxsize=None)
def _grpc_root_certificates(trust_store_path: str | None) -> bytes | None:
    """Build the PEM trust bundle for the channel: the configured trust store UNION the OS trust store.

    This is the gRPC counterpart to the conjure path's `truststore` integration. gRPC (BoringSSL) cannot
    consult the OS trust store on demand, so it is materialised into a static PEM bundle using only the
    standard library (no subprocess, no FFI):

    - Windows: the ``ROOT``/``CA`` system stores via ``SSLContext.load_default_certs`` (which honours
      per-cert SERVER_AUTH trust, so GPO/MDM-pushed enterprise roots are included).
    - macOS: nothing extra — OpenSSL ignores the Keychain, so ``trust_store_path`` is the only source
      (corporate macOS users supply their CA via ``trust_store_path``; see docs/src/networking-tls.md).
    - Linux/other unix: the OS default CA bundle file (``ssl.get_default_verify_paths().cafile``).

    Returns ``None`` (not ``b""``) when no roots are found, so the caller falls back to gRPC's built-in
    defaults; an empty bundle would instead be an empty trust store that fails every handshake.

    Cached per process. The result also depends on ``sys.platform`` (constant per process), which is
    therefore intentionally omitted from the cache key.
    """
    parts: list[bytes] = []
    if trust_store_path is not None:
        parts.append(Path(trust_store_path).read_bytes())
    match sys.platform:
        case "win32":
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
            parts += [ssl.DER_cert_to_PEM_cert(der).encode() for der in ctx.get_ca_certs(binary_form=True)]
        case "darwin":
            pass  # OpenSSL can't read the Keychain; trust_store_path is the only macOS source
        case _:  # linux and other unix-likes
            cafile = ssl.get_default_verify_paths().cafile
            if cafile and Path(cafile).exists():
                parts.append(Path(cafile).read_bytes())
    return b"\n".join(parts) if parts else None


def _service_config_json(service_config: ServiceConfiguration) -> str:
    """Build the gRPC service config JSON: native retry applied uniformly to every method on the channel.

    This mirrors the conjure HTTP path (`RetryWithJitter` in `_utils.networking`): the same attempt count
    (`max_num_retries`), the same status mapping (503 -> UNAVAILABLE, 429 -> RESOURCE_EXHAUSTED; 308 has no
    gRPC analogue), jittered exponential backoff (`initialBackoff = backoff_slot_size`, x2 per attempt,
    capped at 120s), and retry for every method regardless of idempotency.

    Two intentional divergences from conjure's `RetryWithJitter`, accepted because gRPC's native retry is
    simpler/more robust and both are negligible for the unary RPCs this client makes today:

    - First-retry backoff: conjure's first retry is immediate (0); gRPC requires `initialBackoff > 0`, so
      our first retry waits ~uniform(0, backoff_slot_size) instead.
    - Commit state: gRPC only retries *uncommitted* RPCs (it stops once any response has been received),
      whereas conjure (urllib3) retries the whole request regardless. This only changes behaviour for
      streaming RPCs (a partial response received mid-stream); for unary calls the two are equivalent.
      Revisit (e.g. a `RetryWithJitter`-mirroring interceptor) if/when streaming gRPC calls are added.
    """
    return json.dumps(
        {
            "methodConfig": [
                {
                    "name": [{}],  # an empty name matches every method on the channel
                    "retryPolicy": {
                        "maxAttempts": service_config.max_num_retries + 1,  # gRPC counts the first try
                        "initialBackoff": f"{service_config.backoff_slot_size / 1000}s",
                        "maxBackoff": f"{_MAX_BACKOFF_S}s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": list(_RETRYABLE_STATUS),
                    },
                }
            ]
        }
    )


# The 6-field shape of grpc.ClientCallDetails. Subclassing the namedtuple lets interceptors build an
# immutable, attribute-compatible copy of the call details; grpc.ClientCallDetails itself is abstract.
class _ClientCallDetails(
    namedtuple("_ClientCallDetails", ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression")),
    grpc.ClientCallDetails,  # type: ignore[misc]  # grpc stubs type base classes as Any
):
    pass


def _replace_call_details(details: grpc.ClientCallDetails, **changes: Any) -> _ClientCallDetails:
    """Return a `_ClientCallDetails` copy of `details` with the named fields overridden.

    Centralises the 6-field copy so each interceptor only states the single field it changes.
    """
    fields: dict[str, Any] = {
        "method": details.method,
        "timeout": details.timeout,
        "metadata": details.metadata,
        "credentials": details.credentials,
        "wait_for_ready": details.wait_for_ready,
        "compression": details.compression,
    }
    fields.update(changes)
    return _ClientCallDetails(**fields)


class _ClientCallDetailsInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]  # grpc stubs type base classes as Any
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    """Base for interceptors that rewrite the outgoing `ClientCallDetails`.

    gRPC requires a separate ``intercept_*`` method per RPC kind (unary-unary, unary-stream, ...), but each
    does the same thing here: rewrite the call details, then continue. Subclasses implement only `_amend`
    and inherit all four methods, so an interceptor's intent lives in exactly one place.
    """

    @abstractmethod
    def _amend(self, details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        """Return the call details to use (a `_ClientCallDetails`, or `details` unchanged to pass through)."""

    def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return continuation(self._amend(client_call_details), request)

    def intercept_unary_stream(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return continuation(self._amend(client_call_details), request)

    def intercept_stream_unary(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return continuation(self._amend(client_call_details), request_iterator)

    def intercept_stream_stream(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return continuation(self._amend(client_call_details), request_iterator)


class _AuthMetadataInterceptor(_ClientCallDetailsInterceptor):
    """Adds the bearer auth header (and any HeaderProvider headers) to every call's metadata.

    The gRPC equivalent of conjure's `HeaderProviderSession`: the token and extra headers are injected here
    so call sites never pass `metadata=`. A HeaderProvider header that collides with an already-set header
    (including ``authorization``) raises `HeaderConflictError`, matching the HTTP path.
    """

    def __init__(self, auth_header: str, header_provider: HeaderProvider | None) -> None:
        self._auth_header = auth_header
        self._header_provider = header_provider

    def _amend(self, details: grpc.ClientCallDetails) -> _ClientCallDetails:
        # gRPC metadata keys must be lowercase ASCII, so keys are lowercased before comparison and use.
        metadata = list(details.metadata or [])
        existing = {key.lower() for key, _ in metadata}
        if "authorization" in existing:
            raise_header_conflict("authorization")
        metadata.append(("authorization", self._auth_header))
        existing.add("authorization")
        if self._header_provider is not None:
            for key, value in self._header_provider.headers().items():
                lowered = key.lower()
                if lowered in existing:
                    raise_header_conflict(key)
                metadata.append((lowered, value))
                existing.add(lowered)
        return _replace_call_details(details, metadata=tuple(metadata))


class _DefaultDeadlineInterceptor(_ClientCallDetailsInterceptor):
    """Applies a default per-call deadline when the caller didn't set one.

    Mirrors conjure passing `read_timeout` to each request. Because this interceptor runs per call (inside
    retry), every attempt gets a fresh deadline, matching conjure's per-request timeout. A deadline the
    caller set explicitly is left untouched.
    """

    def __init__(self, default_timeout_s: float) -> None:
        self._default_timeout_s = default_timeout_s

    def _amend(self, details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        if details.timeout is not None:
            return details
        return _replace_call_details(details, timeout=self._default_timeout_s)


def create_grpc_channel(
    *,
    api_base_url: str,
    service_config: ServiceConfiguration,
    user_agent: str,
    auth_header: str,
    header_provider: HeaderProvider | None,
) -> grpc.Channel:
    """Build the single shared, fully-configured secure gRPC channel for a client.

    Assembles everything the channel needs and returns it ready to host stubs: TLS credentials over the
    union trust bundle, gzip compression, native retry, lifted message-size limits, the SDK user-agent, and
    the auth-metadata + default-deadline interceptors. All of a client's gRPC stubs share this one channel.
    """
    credentials = grpc.ssl_channel_credentials(
        root_certificates=_grpc_root_certificates(
            None if service_config.security is None else service_config.security.trust_store_path
        )
    )
    options = [
        ("grpc.primary_user_agent", user_agent),
        ("grpc.enable_retries", 1),
        ("grpc.service_config", _service_config_json(service_config)),
        ("grpc.max_send_message_length", _MAX_MESSAGE_LENGTH),
        ("grpc.max_receive_message_length", _MAX_MESSAGE_LENGTH),
    ]
    channel = grpc.secure_channel(
        api_base_url_to_grpc_target(api_base_url), credentials, options=options, compression=grpc.Compression.Gzip
    )
    # The two interceptors are order-independent: one rewrites metadata, the other the deadline.
    return grpc.intercept_channel(
        channel,
        _AuthMetadataInterceptor(auth_header, header_provider),
        _DefaultDeadlineInterceptor(service_config.read_timeout),
    )


def create_grpc_stub_factory(
    *,
    api_base_url: str,
    service_config: ServiceConfiguration,
    user_agent: str,
    auth_header: str,
    header_provider: HeaderProvider | None,
) -> Callable[[type[TStub]], TStub]:
    """Build the shared channel once and return ``factory(StubClass) -> StubClass(channel)``.

    The gRPC analogue of `create_conjure_client_factory`: one shared channel, many stubs. Callers
    (`ClientsBunch`) bind each backend service's generated stub to the returned factory.
    """
    channel = create_grpc_channel(
        api_base_url=api_base_url,
        service_config=service_config,
        user_agent=user_agent,
        auth_header=auth_header,
        header_provider=header_provider,
    )

    def factory(stub_class: type[TStub]) -> TStub:
        return stub_class(channel)  # type: ignore[call-arg]  # grpc stub constructors are untyped

    return factory
