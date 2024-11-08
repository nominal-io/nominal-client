from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from typing_extensions import Self

from nominal._api.combined import scout, scout_notebook_api
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Workbook(HasRid):
    rid: str
    title: str
    description: str
    run_rid: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, notebook: scout_notebook_api.Notebook) -> Self:
        return cls(
            rid=notebook.rid,
            title=notebook.metadata.title,
            description=notebook.metadata.description,
            run_rid=notebook.metadata.run_rid,
            _clients=clients,
        )
