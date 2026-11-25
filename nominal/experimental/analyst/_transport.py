from __future__ import annotations

import json
import threading
import time
from typing import TypeVar

import requests
from conjure_python_client import ServiceConfiguration
from google.protobuf.message import Message

from nominal.core._utils.grpc_tools import _GRPC_STATUS_TO_EXCEPTION
from nominal.core._utils.networking import HeaderProvider, create_multipart_request_session
from nominal.exceptions import (
    NominalAuthenticationError,
    NominalConflictError,
    NominalError,
    NominalPermissionDeniedError,
    NominalRequestThrottledError,
)

_Response = TypeVar("_Response", bound=Message)
_ERRORS: dict[str, type[NominalError]] = {
    **{status.name.lower(): error for status, error in _GRPC_STATUS_TO_EXCEPTION.items()},
    "aborted": NominalConflictError,
    "resource_exhausted": NominalRequestThrottledError,
}


class _AutomationTransport:
    """Client-scoped unary protobuf transport for the public automation Twirp route."""

    def __init__(
        self,
        base_url: str,
        auth_header: str,
        config: ServiceConfiguration,
        user_agent: str,
        header_provider: HeaderProvider | None,
    ) -> None:
        # Strip only the conventional API prefix; preserve deployment-specific path prefixes.
        self._base_url = base_url.rstrip("/").removesuffix("/api")
        self._auth_header = auth_header
        self._timeout = (config.connect_timeout, config.read_timeout)
        self._session = create_multipart_request_session(num_retries=0, header_provider=header_provider)
        self._session.headers["User-Agent"] = user_agent
        # Explicit Authorization headers must win over ambient .netrc credentials.
        self._session.auth = lambda prepared: prepared
        self._ca_bundle = config.security.trust_store_path if config.security is not None else None
        self._jwt: str | None = None
        self._expires_at = 0.0
        self._auth_lock = threading.Lock()

    def _request(self, method: str, path: str, authorization: str, *, data: bytes | None = None) -> requests.Response:
        with self._session.request(
            method,
            self._base_url + path,
            headers={
                "Authorization": authorization,
                "Content-Type": "application/protobuf" if path.startswith("/twirp/") else "application/json",
            },
            data=data,
            timeout=self._timeout,
            verify=self._ca_bundle,
            allow_redirects=False,
        ) as response:
            if response.status_code != 200:
                # Error bodies can echo request data or credentials. Retain only known protocol codes.
                try:
                    code = response.json().get("code")
                except (ValueError, AttributeError):
                    code = None
                error = _ERRORS.get(code, NominalError) if isinstance(code, str) else NominalError
                if response.status_code == 401:
                    error = NominalAuthenticationError
                elif response.status_code == 403:
                    error = NominalPermissionDeniedError
                if error is NominalAuthenticationError:
                    self._expires_at = 0.0
                detail = code if isinstance(code, str) and code in _ERRORS else "request_failed"
                raise error(f"Analyst request failed: {detail} (HTTP {response.status_code}).")
            # requests has already buffered the response before this context closes it.
            return response

    def _authorization(self) -> str:
        if not self._auth_header.startswith("Bearer nominal_api_key"):
            return self._auth_header
        with self._auth_lock:
            if self._jwt is None or time.time() >= self._expires_at - 60:
                profile = self._request("GET", "/api/authentication/v2/my/profile", self._auth_header).json()
                response = self._request(
                    "POST",
                    "/api/authorization/v1/set-user-org",
                    self._auth_header,
                    data=json.dumps({"orgRid": profile["orgRid"]}).encode(),
                ).json()
                self._jwt = str(response["accessToken"])
                self._expires_at = min(float(response["expiresAtSeconds"]), time.time() + 300)
            return "Bearer " + self._jwt

    def call(self, method: str, request: Message, response_type: type[_Response]) -> _Response:
        response = self._request(
            "POST",
            f"/twirp/nominal.ai.v1.AnalystAutomationService/{method}",
            self._authorization(),
            data=request.SerializeToString(),
        )
        return response_type.FromString(response.content)
