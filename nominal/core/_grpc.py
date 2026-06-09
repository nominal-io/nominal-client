from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar
from urllib.parse import urlparse

from conjure_python_client import ServiceConfiguration

from nominal.core._utils.networking import HeaderProvider
from nominal.core.exceptions import HeaderConflictError

_ResponseT = TypeVar("_ResponseT")


class _GrpcStub(Protocol):
    def __init__(self, channel: Any) -> None: ...


def api_base_url_to_grpc_target(api_base_url: str) -> str:
    parsed = urlparse(api_base_url)
    if not parsed.netloc:
        raise ValueError(f"Could not derive gRPC target from API base URL: {api_base_url}")
    return parsed.netloc


@dataclass(frozen=True)
class GrpcClient:
    auth_header: str
    api_base_url: str
    service_config: ServiceConfiguration
    user_agent: str
    header_provider: HeaderProvider | None

    def invoke(
        self,
        stub_class: type[_GrpcStub],
        method: Callable[[Any], Callable[..., _ResponseT]],
        request: Any,
    ) -> _ResponseT:
        try:
            import grpc  # type: ignore[import-untyped]
        except ImportError as ex:
            raise ImportError("nominal[protos] is required to use gRPC APIs") from ex

        target = api_base_url_to_grpc_target(self.api_base_url)
        options = (("grpc.primary_user_agent", self.user_agent),)
        channel = grpc.secure_channel(target, self._channel_credentials(grpc), options=options)
        with channel:
            stub = stub_class(channel)
            return method(stub)(request, metadata=self._metadata())

    def _channel_credentials(self, grpc: Any) -> Any:
        if self.service_config.security is None:
            return grpc.ssl_channel_credentials()

        with open(self.service_config.security.trust_store_path, "rb") as root_certificates_file:
            root_certificates = root_certificates_file.read()
        return grpc.ssl_channel_credentials(root_certificates=root_certificates)

    def _metadata(self) -> tuple[tuple[str, str], ...]:
        metadata = {"authorization": self.auth_header}
        if self.header_provider is None:
            return tuple(metadata.items())

        for key, value in self.header_provider.headers().items():
            metadata_key = key.lower()
            if metadata_key in metadata:
                raise HeaderConflictError(
                    f"HeaderProvider returned header {key!r}, but the request already set that header; "
                    "HeaderProvider cannot override explicit request headers."
                )
            metadata[metadata_key] = value
        return tuple(metadata.items())
