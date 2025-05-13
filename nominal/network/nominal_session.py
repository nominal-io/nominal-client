from __future__ import annotations

import gzip
import json
import logging
from typing import Type, TypeVar

import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import RetryWithJitter, TransportAdapter

logger = logging.getLogger(__name__)
T = TypeVar("T")


class GzipRequestsClient:
    """Wrapper around conjures RequestClient to automatically gzip post-requested data.

    In bandwidth constrained scenarios, this has been measured to have 5-6x speedups in
    uploading data, depending on its compressability.

    See: https://github.com/palantir/conjure-python-client/blob/60d6d7639502a3b0fe18fad388ce84cbc54eb613/conjure_python_client/_http/requests_client.py#L181
    """

    @classmethod
    def create(
        cls,
        service_class: Type[T],
        user_agent: str,
        service_config: ServiceConfiguration,
        return_none_for_unknown_union_types: bool = False,
        enable_keep_alive: bool = False,
        session: requests.Session | None = None,
    ) -> T:
        # setup retry to match java remoting
        # https://github.com/palantir/http-remoting/tree/3.12.0#quality-of-service-retry-failover-throttling
        retry = RetryWithJitter(
            total=service_config.max_num_retries,
            read=0,  # do not retry read errors
            status_forcelist=[308, 429, 503],
            backoff_factor=float(service_config.backoff_slot_size) / 1000,
        )
        transport_adapter = TransportAdapter(max_retries=retry, enable_keep_alive=enable_keep_alive)
        # create a session, for shared connection polling, user agent, etc
        if session is None:
            session = GzipRequestsSession()

        session.headers["User-Agent"] = user_agent
        if service_config.security is not None:
            verify = service_config.security.trust_store_path
        else:
            verify = None
        for uri in service_config.uris:
            session.mount(uri, transport_adapter)

        return service_class(  # type: ignore
            session,
            service_config.uris,
            service_config.connect_timeout,
            service_config.read_timeout,
            verify,
            return_none_for_unknown_union_types,
        )


class GzipRequestsSession(requests.Session):
    def request(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        method = args[0]
        if method == "POST":
            # Set headers to indicate gzipped content
            if "headers" in kwargs:
                kwargs["headers"]["Content-Encoding"] = "gzip"

            # Intercept data and gzip it into the body of the request
            # In the requests library, if data is _not_ provided, but json is,
            # then the json is used to create the body. If data _is_ provided, then
            # the json payload is ignored.
            if "data" not in kwargs and "json" in kwargs:
                logger.debug("Gzipping json POST contents")
                json_str = json.dumps(kwargs["json"]).encode("utf-8")
                kwargs["data"] = gzip.compress(json_str, compresslevel=1)
                del kwargs["json"]
            elif "data" in kwargs:
                logger.debug("Gzipping data POST contents")
                data = kwargs["data"]
                if isinstance(data, bytes):
                    kwargs["data"] = gzip.compress(data, compresslevel=1)
                elif isinstance(data, str):
                    kwargs["data"] = gzip.compress(data.encode("utf-8"), compresslevel=1)
                else:
                    json_str = json.dumps(kwargs["data"]).encode("utf-8")
                    kwargs["data"] = gzip.compress(json_str)

        return super().request(*args, **kwargs)
