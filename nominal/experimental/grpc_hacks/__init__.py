"""Experimental: run the client's gRPC services over HTTP/JSON transcoding.

For deployment environments where gRPC/HTTP-2 is blocked but ordinary HTTPS works. The core
`NominalClient` is unchanged; `with_http_shim` returns a copy of a client whose gRPC-backed services
are rebound onto an `HttpTranscodeChannel`.

`with_http_shim` is a composable `client -> client` transform, so it works with any client:

    from nominal.core.client import NominalClient
    from nominal.experimental.grpc_hacks import with_http_shim

    client = with_http_shim(NominalClient.from_profile("prod"))

Composing with impersonation
----------------------------
Impersonation (`nominal.experimental.impersonation.as_user`) is just an on-behalf-of header, which
the shim propagates like any other. **Apply the shim last** so it inherits that header:

    from nominal.experimental.impersonation import as_user
    from nominal.experimental.grpc_hacks import with_http_shim

    client = with_http_shim(as_user(NominalClient.from_profile("prod"), user_rid))  # correct

The reverse order does NOT work: `as_user(...)` rebuilds the client from scratch with a real gRPC
channel, silently discarding a shim applied earlier. There is deliberately no guard against this — get
the order right.

Experimental: unary RPCs only, and this API may change or be removed.
"""

from __future__ import annotations

import dataclasses
import logging

from nominal.core.client import NominalClient
from nominal.experimental.grpc_hacks._transcode import HttpTranscodeChannel, TranscodeError

__all__ = [
    "with_http_shim",
    "HttpTranscodeChannel",
    "TranscodeError",
]

logger = logging.getLogger(__name__)


def _is_grpc_stub(value: object) -> bool:
    """True if `value` is a generated gRPC stub (`*ServiceStub` from a `*_pb2_grpc` module).

    Distinguishes gRPC stubs from conjure/HTTP service clients and plain fields on the ClientsBunch,
    so the shim can discover them without a hardcoded list.
    """
    cls = type(value)
    return cls.__name__.endswith("Stub") and "_pb2_grpc" in getattr(cls, "__module__", "")


def with_http_shim(client: NominalClient) -> NominalClient:
    """Return a copy of ``client`` whose gRPC services speak HTTP/JSON transcoding.

    A composable ``client -> client`` transform. It discovers every gRPC stub on the client (no
    hardcoded list) and rebinds each onto an ``HttpTranscodeChannel``, reusing the client's auth, TLS,
    config, and header provider (so an on-behalf-of header from ``as_user`` is carried over).
    Conjure/HTTP services are left untouched, and new gRPC services are picked up automatically.

    Apply this last in a transform chain: ``as_user(with_http_shim(client))`` silently drops the shim
    (``as_user`` rebuilds with a real gRPC channel), so use ``with_http_shim(as_user(client, rid))``.

    Args:
        client: A fully constructed client, e.g. from ``NominalClient.from_profile``.

    Returns:
        A copy of ``client`` with its gRPC services routed over HTTP transcoding.

    Raises:
        TranscodeError: If a gRPC service cannot be transcoded because its methods lack
            ``google.api.http`` annotations (the installed ``nominal-api-protos`` may predate them), or
            if the client exposes no gRPC services to shim. Fails here rather than on a later call.

    Note:
        Experimental and unary-only; this API may change or be removed.
    """
    cb = client._clients
    channel = HttpTranscodeChannel(
        api_base_url=cb._api_base_url,
        service_config=cb._service_config,
        user_agent=cb._user_agent,
        auth_header=cb.auth_header,
        header_provider=cb.header_provider,
    )
    replacements = {}
    for f in dataclasses.fields(cb):
        stub = getattr(cb, f.name)
        if not _is_grpc_stub(stub):
            continue
        try:
            replacements[f.name] = type(stub)(channel)  # eager: validates every method's annotation
        except TranscodeError as e:
            raise TranscodeError(f"cannot HTTP-shim gRPC service {type(stub).__name__} (field {f.name!r}): {e}") from e

    if not replacements:
        raise TranscodeError("no gRPC services found on the client to shim")

    logger.info(
        "HTTP-transcode shim active for %s: %d gRPC service(s) [%s]",
        cb._api_base_url,
        len(replacements),
        ", ".join(sorted(replacements)),
    )
    rebound = dataclasses.replace(cb, **replacements)
    return dataclasses.replace(client, _clients=rebound)
