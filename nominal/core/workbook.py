from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nominal._api.combined import (
    scout,
    scout_notebook_api
)
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Workbook(HasRid):
    rid: str
    charts: any
    layout: any
    content: any
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def workbook(self) -> scout.NotebookService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, workbook: scout_notebook_api.Notebook) -> Workbook:
        return cls(
            rid=workbook.rid,
            charts=workbook.charts,
            layout=workbook.layout,
            content=workbook.content,
            _clients=clients,
        )
