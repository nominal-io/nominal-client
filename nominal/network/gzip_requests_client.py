from __future__ import annotations

import logging
from typing import Type, TypeVar

import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.requests_client import RetryWithJitter
from requests.adapters import CaseInsensitiveDict

from nominal.network.gzip_requests_adapter import GzipRequestsAdapter

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
    ) -> T:
        # setup retry to match java remoting
        # https://github.com/palantir/http-remoting/tree/3.12.0#quality-of-service-retry-failover-throttling
        retry = RetryWithJitter(
            total=service_config.max_num_retries,
            read=0,  # do not retry read errors
            status_forcelist=[308, 429, 503],
            backoff_factor=float(service_config.backoff_slot_size) / 1000,
        )
        transport_adapter = GzipRequestsAdapter(max_retries=retry, enable_keep_alive=enable_keep_alive)
        # create a session, for shared connection polling, user agent, etc
        session = requests.Session()
        session.headers = CaseInsensitiveDict({"User-Agent": user_agent})
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
