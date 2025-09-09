from __future__ import annotations

from typing import Iterable, Sequence

import nominal_api.module as module_api

from nominal.core._utils.pagination_tools import paginate_rpc
from nominal.core.client import NominalClient
from nominal.experimental.compute.module._types import Module


def get_module(client: NominalClient, rid: str) -> Module:
    request = module_api.BatchGetModulesRequest(requests=[module_api.GetModuleRequest(module_rid=rid)])
    modules = client._clients.module.batch_get_modules(client._clients.auth_header, request)
    if len(modules) == 0:
        raise ValueError(f"Module with RID {rid} not found")
    if len(modules) > 1:
        raise ValueError(f"Multiple modules found with RID {rid}")
    return Module._from_conjure(client._clients, modules[0].metadata)


def _iter_list_modules(client: NominalClient) -> Iterable[Module]:
    def request_factory(page_token: str | None) -> module_api.SearchModulesRequest:
        return module_api.SearchModulesRequest(
            page_size=100,
            query=module_api.SearchModulesQuery(search_text=""),
            next_page_token=page_token,
        )

    for response in paginate_rpc(
        client._clients.module.search_modules, client._clients.auth_header, request_factory=request_factory
    ):
        for summary in response.results:
            yield Module._from_conjure(client._clients, summary.metadata)


def list_modules(client: NominalClient) -> Sequence[Module]:
    return list(_iter_list_modules(client))
