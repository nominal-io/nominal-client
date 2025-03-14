from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nominal_api import scout, scout_notebook_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Workbook(HasRid):
    _rid: str
    _title: str
    _description: str
    _run_rid: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def title(self) -> str:
        return self._title

    @property
    def description(self) -> str:
        return self._description

    @property
    def run_rid(self) -> str | None:
        return self._run_rid

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook in the Nominal app"""
        # TODO (drake): move logic into _from_conjure() factory function to accomodate different URL schemes
        return f"https://app.gov.nominal.io/workbooks/{self.rid}"

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
            _rid=notebook.rid,
            _title=notebook.metadata.title,
            _description=notebook.metadata.description,
            _run_rid=notebook.metadata.run_rid,
            _clients=clients,
        )
