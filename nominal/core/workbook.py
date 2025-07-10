from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nominal_api import scout, scout_notebook_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Workbook(HasRid):
    rid: str
    title: str
    description: str
    run_rid: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook in the Nominal app"""
        return f"{self._clients.app_base_url}/workbooks/{self.rid}"

    def archive(self) -> None:
        """Archive this workbook.
        Archived workbooks are not deleted, but are hidden from the UI.
        """
        self._clients.notebook.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this workbook, allowing it to be viewed in the UI."""
        self._clients.notebook.unarchive(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, notebook: scout_notebook_api.Notebook) -> Self:
        return cls(
            rid=notebook.rid,
            title=notebook.metadata.title,
            description=notebook.metadata.description,
            run_rid=notebook.metadata.run_rid,
            _clients=clients,
        )
