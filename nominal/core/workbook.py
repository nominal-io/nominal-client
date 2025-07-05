from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence

from nominal_api import scout, scout_notebook_api
from typing_extensions import Self

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid


@dataclass(frozen=True)
class Workbook(HasRid):
    rid: str
    title: str
    description: str
    run_rid: str | None
    asset_rids: Sequence[str] | None
    is_draft: bool
    is_locked: bool
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook in the Nominal app"""
        return f"{self._clients.app_base_url}/workbooks/{self.rid}"

    def update(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace workbook metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        NOTE: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in workbook.labels:
                new_labels.append(old_label)
            workbook = workbook.update(labels=new_labels)
        """
        # NOTE: not saving updated metadata response, as we deserialize from a notebook rather than
        #       from metadata
        self._clients.notebook.update_metadata(
            self._clients.auth_header,
            scout_notebook_api.UpdateNotebookMetadataRequest(
                title=title,
                description=description,
                is_draft=is_draft,
                labels=None if labels is None else [*labels],
                properties=None if properties is None else {**properties},
            ),
            self.rid,
        )
        notebook = self.__class__._from_conjure(
            self._clients, self._clients.notebook.get(self._clients.auth_header, self.rid)
        )
        update_dataclass(self, notebook, fields=self.__dataclass_fields__)
        return self

    def get_refnames(self) -> Sequence[str]:
        """Get the list of refnames used within the workbook."""
        return self._clients.notebook.get_used_ref_names(self._clients.auth_header, self.rid)

    def update_refnames(self, refname_map: Mapping[str, str]) -> None:
        """Updates refnames using a provided map of original refnames to the new refnames to replace them."""
        self._clients.notebook.update_ref_names(
            self._clients.auth_header, scout_notebook_api.UpdateRefNameRequest({**refname_map}), self.rid
        )

    def lock(self) -> None:
        """Locks the workbook, preventing changes from being made to it."""
        self._clients.notebook.lock(self._clients.auth_header, self.rid)

    def unlock(self) -> None:
        """Unlocks the workbook, allowing changes to be made to it."""
        self._clients.notebook.unlock(self._clients.auth_header, self.rid)

    def archive(self) -> None:
        """Archive this workbook.
        Archived workbooks are not deleted, but are hidden from the UI.
        """
        self._clients.notebook.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this workbook, allowing it to be viewed in the UI."""
        self._clients.notebook.unarchive(self._clients.auth_header, self.rid)

    def delete(self) -> None:
        """Delete the workbook permanently."""
        self._clients.notebook.delete(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, notebook: scout_notebook_api.Notebook) -> Self:
        return cls._from_notebook_metadata(
            clients, scout_notebook_api.NotebookMetadataWithRid(metadata=notebook.metadata, rid=notebook.rid)
        )

    @classmethod
    def _from_notebook_metadata(cls, clients: _Clients, notebook: scout_notebook_api.NotebookMetadataWithRid) -> Self:
        data_scope = notebook.metadata.data_scope
        if data_scope.run_rids is not None and len(data_scope.run_rids) != 1:
            raise ValueError("Workbooks with more than 1 run are not yet supported")

        return cls(
            rid=notebook.rid,
            title=notebook.metadata.title,
            description=notebook.metadata.description,
            run_rid=None if data_scope.run_rids is None else data_scope.run_rids[0],
            asset_rids=data_scope.asset_rids,
            is_draft=notebook.metadata.is_draft,
            is_locked=notebook.metadata.lock.is_locked,
            _clients=clients,
        )
