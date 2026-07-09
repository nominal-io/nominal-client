"""An HTTP/JSON-transcoding drop-in for a gRPC channel (experimental).

Routes unary gRPC calls over the backend's ``google.api.http``-annotated JSON transcoding surface,
for environments where gRPC/HTTP-2 does not survive the network path but ordinary HTTPS does.

The seam: generated stubs are built as ``StubClass(channel)`` and only call
``channel.unary_unary(method, request_serializer, response_deserializer)``. A channel whose
``unary_unary`` returns a callable that transcodes to HTTP makes the whole stub speak HTTP, with no
call-site changes. The route table is built at runtime from the compiled proto descriptors (no
codegen).

Transport parity: the session is built from the same canonical pieces as the conjure HTTP path
(``SslBypassRequestsAdapter`` = OS/enterprise trust-store union via truststore; plus the shared
``create_retry_policy``), so TLS and retry behavior match the rest of the SDK — important because this
transport exists for restrictive/corporate networks.

Scope: unary-unary only (the client has no streaming RPCs). Field binding follows the
``google.api.http`` rules: path templates, body ``'*'``/``''``/named-field, ``response_body``
whole/named-field, and proto3-JSON query mapping (nested -> dotted, repeated -> repeated key).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable
from urllib.parse import quote

import grpc
import requests
from conjure_python_client import ServiceConfiguration

# Importing annotations_pb2 registers the google.api.http extension on MethodOptions.
from google.api import annotations_pb2  # type: ignore[import-untyped]
from google.protobuf import descriptor_pool, json_format, message_factory
from google.protobuf.descriptor import MethodDescriptor
from google.protobuf.message import Message

from nominal.core._utils.grpc_tools import _GRPC_STATUS_TO_EXCEPTION
from nominal.core._utils.networking import (
    HeaderProvider,
    HeaderProviderSession,
    SslBypassRequestsAdapter,
    create_retry_policy,
    raise_header_conflict,
)
from nominal.core.exceptions import (
    NominalAlreadyExistsError,
    NominalAuthenticationError,
    NominalError,
    NominalInvalidArgumentError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)

logger = logging.getLogger(__name__)

# JSON error bodies carry an errorCode/code whose values are the gRPC StatusCode names. Derive the
# code -> exception mapping from the gRPC status mapping so both transports surface the same exception
# type and the two tables cannot drift.
_ERROR_CODE_TO_EXCEPTION: dict[str, type[NominalError]] = {
    status.name: exc for status, exc in _GRPC_STATUS_TO_EXCEPTION.items()
}
# Fallback when the error body carries no recognizable code: coarse HTTP status -> exception.
_STATUS_TO_EXCEPTION: dict[int, type[NominalError]] = {
    400: NominalInvalidArgumentError,
    401: NominalAuthenticationError,
    403: NominalPermissionDeniedError,
    404: NominalNotFoundError,
    409: NominalAlreadyExistsError,
}

# Captures a path-template variable: group 1 = field path, group 2 = the "=pattern" part (if any),
# e.g. "{rid}" -> ("rid", None); "{name=**}" -> ("name", "=**").
_PATH_SEGMENT = re.compile(r"\{([^}=]+)(=[^}]*)?\}")


class TranscodeError(NominalError):
    """A transcoding/route-level failure (the transcoder did not claim the route), as opposed to a
    real JSON API error.
    """


def _http_rule(method_desc: MethodDescriptor) -> Any:
    # Read the extension directly (it yields a default-empty HttpRule when unset) and check for a
    # verb pattern, rather than HasExtension() — the latter can leave a stale CPython error indicator
    # under upb that surfaces as a SystemError with coverage's sys.monitoring tracer on Python 3.13+.
    rule = method_desc.GetOptions().Extensions[annotations_pb2.http]
    if not rule.WhichOneof("pattern"):
        raise TranscodeError(
            f"{method_desc.full_name} has no google.api.http annotation; cannot transcode. "
            "The installed nominal-api-protos may predate the annotations for this service "
            "(or the service is not allowlisted for the SDK)."
        )
    return rule


def _resolve_field(message: Message, dotted: str) -> Any:
    """Return the value at a (possibly dotted) field path within a proto message."""
    value: Any = message
    for part in dotted.split("."):
        value = getattr(value, part)
    return value


def _json_name(field_name: str) -> str:
    """Proto3 default JSON name (lowerCamelCase) for a snake_case field name.

    Computed rather than read from ``FieldDescriptor.json_name`` because that upb property access, done
    in ``__init__`` after reading the method's options, can leave a stale CPython error indicator that
    coverage's sys.monitoring tracer raises as a SystemError on Python 3.13+. The API's fields all use
    default JSON names, so this matches what ``MessageToDict`` produces.
    """
    head, *tail = field_name.split("_")
    return head + "".join(word[:1].upper() + word[1:] for word in tail)


def _json_names(field_names: set[str]) -> set[str]:
    """JSON (camelCase) names for a set of top-level proto field names."""
    return {_json_name(name) for name in field_names}


def _fill_path(template: str, message: Message) -> tuple[str, set[str]]:
    """Substitute ``{field}`` / ``{field=**}`` path segments; return the path and consumed field names.

    A ``=**`` pattern means the value may span multiple path segments, so its slashes are preserved;
    a plain ``{field}`` is a single segment and its slashes are percent-encoded. (Nested field paths
    are rejected up front in `_HttpUnaryUnary.__init__`, so `field_path` here is always top-level.)
    """
    consumed: set[str] = set()

    def sub(match: re.Match[str]) -> str:
        field_path = match.group(1)
        pattern = match.group(2) or ""
        consumed.add(field_path)
        safe = "/" if "**" in pattern else ""
        return quote(str(_resolve_field(message, field_path)), safe=safe)

    return _PATH_SEGMENT.sub(sub, template), consumed


def _flatten_query(prefix: str, value: Any, out: list[tuple[str, str]]) -> None:
    """Flatten a proto3-JSON value into query params: nested → dotted, repeated → repeated key."""
    if isinstance(value, dict):
        for key, sub in value.items():
            _flatten_query(f"{prefix}.{key}", sub, out)
    elif isinstance(value, list):
        for item in value:
            _flatten_query(prefix, item, out)
    elif isinstance(value, bool):
        out.append((prefix, "true" if value else "false"))  # str(True) would be "True"
    else:
        out.append((prefix, str(value)))


def _query_params(as_dict: dict[str, Any], exclude: set[str]) -> list[tuple[str, str]]:
    """Flatten a proto3-JSON dict into query params (camelCase), dropping the `exclude` JSON names."""
    params: list[tuple[str, str]] = []
    for key, value in as_dict.items():
        if key not in exclude:
            _flatten_query(key, value, params)
    return params


def _build_session(
    service_config: ServiceConfiguration,
    user_agent: str,
    auth_header: str,
    header_provider: HeaderProvider | None,
) -> requests.Session:
    """Build a requests session with the SDK's canonical trust-store + retry behavior.

    Reuses the conjure transport's `SslBypassRequestsAdapter` (OS/enterprise trust store via
    truststore) and the shared `create_retry_policy`, so TLS/retry match the rest of the client. (Uses
    the non-gzip adapter: the transcoding routes are not guaranteed to decode gzipped request bodies.)
    """
    retry = create_retry_policy(service_config)
    session = HeaderProviderSession(header_provider)
    session.headers.update({"User-Agent": user_agent, "authorization": auth_header})
    # Auth/user-agent are session defaults, which HeaderProviderSession's per-request conflict check
    # does not cover. Mirror the gRPC interceptor's policy — a header_provider may not override them.
    # Checked once here, so this assumes a static provider; a provider that returns `authorization`
    # only on some later call would not be caught (the realistic providers — extra_headers and
    # as_user's on-behalf-of header — are static).
    if header_provider is not None:
        reserved = {"authorization", "user-agent"}
        for key in header_provider.headers():
            if key.lower() in reserved:
                raise_header_conflict(key)
    adapter = SslBypassRequestsAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if service_config.security is not None and service_config.security.trust_store_path is not None:
        session.verify = service_config.security.trust_store_path
    return session


class _HttpUnaryUnary:
    """The transcoding replacement for a stub's unary-unary MultiCallable.

    Calling it (``stub.Method(request)``) issues the annotated HTTP request and returns the parsed
    response proto — exactly what the blocking gRPC MultiCallable returns.
    """

    def __init__(
        self,
        *,
        base_url: str,
        method_desc: MethodDescriptor,
        session: requests.Session,
        timeout: tuple[float, float] | float | None,
    ) -> None:
        self._base_url = base_url
        self._session = session
        self._timeout = timeout
        self._full_method = method_desc.full_name
        rule = _http_rule(method_desc)
        self._verb = rule.WhichOneof("pattern")
        self._path_template = getattr(rule, self._verb)
        # Fail fast on template shapes we don't support, rather than mis-building the request at call
        # time. Nested path fields ({a.b}) would need nested query-exclusion; none exist today.
        for seg in _PATH_SEGMENT.finditer(self._path_template):
            if "." in seg.group(1):
                raise TranscodeError(
                    f"{self._full_method}: nested path-field template '{{{seg.group(1)}}}' "
                    "is not supported by the HTTP shim"
                )
        self._body = rule.body  # '', '*', or a request field name
        self._response_body = rule.response_body  # '', '*', or a response field name
        # The response message class comes from the descriptor we already resolved — no reliance on the
        # serializer/deserializer the stub passes.
        self._response_cls = message_factory.GetMessageClass(method_desc.output_type)
        # JSON names for the named body / response_body fields (computed; see _json_name). The
        # repeated-vs-not default for an absent body field is resolved at call time from the request
        # descriptor, where upb field access is safe.
        self._body_json_name = _json_name(self._body) if self._body and self._body != "*" else None
        self._response_body_json_name = (
            _json_name(self._response_body) if self._response_body and self._response_body != "*" else None
        )

    def _build(self, request: Message) -> tuple[str, str, list[tuple[str, str]], str | None]:
        path, consumed = _fill_path(self._path_template, request)
        url = f"{self._base_url}{path}"
        as_dict = json_format.MessageToDict(request, preserving_proto_field_name=False)
        exclude = _json_names(consumed)
        params: list[tuple[str, str]] = []
        body_json: str | None = None
        if self._body == "*":
            # Whole message is the body, minus fields already bound to the path.
            body_json = json.dumps({key: value for key, value in as_dict.items() if key not in exclude})
        elif self._body:
            # A named field is the body ("subrequest"); other non-path fields become query params.
            # Read it from the JSON dict so repeated/map/scalar body fields work, not just messages.
            assert self._body_json_name is not None  # set whenever self._body is a named field
            if self._body_json_name in as_dict:
                body_json = json.dumps(as_dict[self._body_json_name])
            else:
                # Field omitted at its default. Send [] for a repeated (list) field, {} otherwise,
                # inferred from the value's type (repeated containers have append()) rather than a upb
                # descriptor-property access, which can trip coverage's sys.monitoring tracer on 3.13+.
                body_json = "[]" if hasattr(getattr(request, self._body), "append") else "{}"
            params = _query_params(as_dict, exclude | {self._body_json_name})
        else:
            params = _query_params(as_dict, exclude)
        return self._verb.upper(), url, params, body_json

    def __call__(self, request: Message, **_: Any) -> Message:
        verb, url, params, body_json = self._build(request)
        logger.debug("transcode %s -> %s %s", self._full_method, verb, url)
        resp = self._session.request(
            verb,
            url,
            params=params or None,
            data=body_json,
            headers={"content-type": "application/json"} if body_json is not None else None,
            timeout=self._timeout,
        )
        if resp.status_code >= 400:
            self._raise(resp)
        message = self._response_cls()
        text = resp.text.strip()
        if not text:
            return message
        if self._response_body_json_name is not None:
            # The HTTP body is a single response field ("subresponse"). Wrapping it under the field's
            # JSON name and using ParseDict handles singular, repeated, and map fields uniformly.
            wrapped = {self._response_body_json_name: json.loads(text)}
            json_format.ParseDict(wrapped, message, ignore_unknown_fields=True)
        else:
            json_format.Parse(text, message, ignore_unknown_fields=True)
        return message

    def _raise(self, resp: requests.Response) -> None:
        is_json = "json" in resp.headers.get("content-type", "")
        # A non-JSON 404/405 means the transcoder didn't claim the route (deploy/config issue),
        # distinct from a real API not-found, which comes back as a structured JSON error.
        if not is_json and resp.status_code in (404, 405):
            logger.error(
                "transcode route not claimed for %s: %s %s -> HTTP %s "
                "(is transcoding deployed for this service and is the base URL correct?)",
                self._full_method,
                resp.request.method,
                resp.url,
                resp.status_code,
            )
            raise TranscodeError(
                f"route-level {resp.status_code} for {resp.request.method} {resp.url} "
                f"(transcoder did not claim this route; body[:120]={resp.text[:120]!r})"
            )
        code = None
        if is_json:
            try:
                payload = json.loads(resp.text)
                if isinstance(payload, dict):
                    code = payload.get("errorCode") or payload.get("code")  # conjure / twirp shapes
            except ValueError:
                pass
        # `code` may be a non-string (e.g. a numeric gRPC-JSON status); only string codes are keys.
        code_key = code.upper() if isinstance(code, str) else ""
        exc_type = _ERROR_CODE_TO_EXCEPTION.get(code_key) or _STATUS_TO_EXCEPTION.get(resp.status_code, NominalError)
        # 5xx are server-side failures worth flagging; 4xx are usually client/business errors the caller
        # handles (e.g. NOT_FOUND existence checks, ALREADY_EXISTS in idempotent flows), so log those at
        # debug to avoid warning-spam — the raised exception still carries the full detail either way.
        log = logger.warning if resp.status_code >= 500 else logger.debug
        log(
            "transcode call failed: %s -> HTTP %s (errorCode=%s) body=%s",
            self._full_method,
            resp.status_code,
            code,
            resp.text[:200],
        )
        raise exc_type(f"HTTP {resp.status_code}: {resp.text[:500]}")


class HttpTranscodeChannel(grpc.Channel):
    """A gRPC channel that speaks HTTP/JSON transcoding instead of gRPC/HTTP-2.

    Mirrors ``create_grpc_channel``'s signature so it drops into the same place: bind any generated
    stub to it and every unary RPC transparently uses the annotated REST route.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        service_config: ServiceConfiguration,
        user_agent: str,
        auth_header: str,
        header_provider: HeaderProvider | None = None,
    ) -> None:
        """Build the transcoding channel.

        Args:
            api_base_url: Base URL of the Nominal API (e.g. ``https://api.example.com/api``). Annotated
                REST routes are resolved relative to it.
            service_config: Connection settings (timeouts, retries, TLS trust store), reused from the
                client so HTTP behavior matches the rest of the SDK.
            user_agent: Value sent as the ``User-Agent`` header on every request.
            auth_header: Value sent as the ``authorization`` header (e.g. ``"Bearer <token>"``).
            header_provider: Optional provider of extra headers added to every request. It may not
                override ``authorization`` or ``user-agent``.

        Raises:
            HeaderConflictError: If ``header_provider`` returns a reserved header
                (``authorization``/``user-agent``).
        """
        self._base_url = api_base_url.rstrip("/")
        self._pool = descriptor_pool.Default()  # type: ignore[no-untyped-call]
        self._session = _build_session(service_config, user_agent, auth_header, header_provider)
        self._timeout: tuple[float, float] = (service_config.connect_timeout, service_config.read_timeout)

    def _method_desc(self, method: str) -> MethodDescriptor:
        # method is "/pkg.Service/Method"
        _, service, meth = method.split("/")
        return self._pool.FindServiceByName(service).methods_by_name[meth]  # type: ignore[no-any-return]

    def unary_unary(  # type: ignore[override]
        self,
        method: str,
        request_serializer: Callable[[Message], bytes] | None = None,
        response_deserializer: Callable[[bytes], Message] | None = None,
        **_: Any,
    ) -> _HttpUnaryUnary:
        return _HttpUnaryUnary(
            base_url=self._base_url,
            method_desc=self._method_desc(method),
            session=self._session,
            timeout=self._timeout,
        )

    # --- streaming is out of scope (the client has no streaming RPCs) ----------------------------
    def unary_stream(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("HttpTranscodeChannel supports unary-unary only")

    def stream_unary(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("HttpTranscodeChannel supports unary-unary only")

    def stream_stream(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("HttpTranscodeChannel supports unary-unary only")

    # --- Channel lifecycle -----------------------------------------------------------------------
    def subscribe(self, callback: Any, try_to_connect: bool = False) -> None:
        pass

    def unsubscribe(self, callback: Any) -> None:
        pass

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> HttpTranscodeChannel:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
