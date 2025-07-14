from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, Sequence

from nominal_api import scout, scout_notebook_api
from typing_extensions import Self, deprecated

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid

logger = logging.getLogger(__name__)


class WorkbookType(Enum):
    WORKBOOK = "WORKBOOK"
    COMPARISON_WORKBOOK = "COMPARISON_WORKBOOK"

    @classmethod
    def _from_conjure(cls, workbook_type: scout_notebook_api.NotebookType) -> WorkbookType:
        if workbook_type.value == "WORKBOOK":
            return cls.WORKBOOK
        elif workbook_type.value == "COMPARISON_WORKBOOK":
            return cls.COMPARISON_WORKBOOK
        else:
            raise ValueError(f"Unknown workbook type: {workbook_type}")

    def _to_conjure(self) -> scout_notebook_api.NotebookType:
        return {
            "WORKBOOK": scout_notebook_api.NotebookType.WORKBOOK,
            "COMPARISON_WORKBOOK": scout_notebook_api.NotebookType.COMPARISON_WORKBOOK,
        }[self.value]


@dataclass(frozen=True)
class Workbook(HasRid):
    rid: str
    title: str
    description: str
    workbook_type: WorkbookType

    run_rids: Sequence[str] | None
    """Mutually exclusive with `asset_rids`.

    May be empty when a workbook is a fresh comparison workbook.
    """

    asset_rids: Sequence[str] | None
    """Mutually exclusive with `run_rids`.

    May be empty when a workbook is a fresh comparison workbook.
    """

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook in the Nominal app"""
        return f"{self._clients.app_base_url}/workbooks/{self.rid}"

    @property
    @deprecated(
        "`Workbook.run_rid` is deprecated and will be removed in a future release: use Workbook.run_rids instead"
    )
    def run_rid(self) -> str | None:
        if self.run_rids is None:
            return None
        elif len(self.run_rids) == 0:
            return None
        elif len(self.run_rids) == 1:
            return self.run_rids[0]
        else:
            raise RuntimeError("Cannot access singular `run_rid`-- workbook has multiple run rids!")

    def update(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
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
        # TODO(drake): Support updating runs / assets on a workbook once behavior is more defined
        # NOTE: not saving updated metadata response, as we deserialize from a notebook rather than
        #       from metadata
        self._clients.notebook.update_metadata(
            self._clients.auth_header,
            scout_notebook_api.UpdateNotebookMetadataRequest(
                title=title,
                description=description,
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

    def clone(
        self,
        title: str | None = None,
        description: str | None = None,
    ) -> Self:
        """Create a new workbook copy from this workbook and return a reference to the cloned version.

        Args:
            title: New title for the cloned workbook.
                Defaults to "Workbook clone from '<title>'" for the current workbook title.
            description: New description for the cloned workbook. Defaults to the current description.

        Returns:
            Reference to the cloned workbook
        """
        raw_workbook = self._clients.notebook.get(self._clients.auth_header, self.rid)
        new_workbook = self._clients.notebook.create(
            self._clients.auth_header,
            scout_notebook_api.CreateNotebookRequest(
                title=f"Workbook clone from '{self.title}'" if title is None else title,
                description=self.description if description is None else description,
                is_draft=False,
                state_as_json=raw_workbook.state_as_json,
                data_scope=scout_notebook_api.NotebookDataScope(
                    run_rids=None if self.run_rids is None else [*self.run_rids],
                    asset_rids=None if self.asset_rids is None else [*self.asset_rids],
                ),
                layout=raw_workbook.layout,
                content_v2=raw_workbook.content_v2,
                event_refs=raw_workbook.event_refs,
                workspace=self._clients.workspace_rid,
            ),
        )

        return self._from_conjure(self._clients, new_workbook)

    def get_refnames(self) -> Sequence[str]:
        """Get the list of refnames used within the workbook."""
        return self._clients.notebook.get_used_ref_names(self._clients.auth_header, self.rid)

    def update_refnames(self, refname_map: Mapping[str, str]) -> None:
        """Updates refnames using a provided map of original refnames to the new refnames to replace them."""
        self._clients.notebook.update_ref_names(
            self._clients.auth_header, scout_notebook_api.UpdateRefNameRequest({**refname_map}), self.rid
        )

    def is_locked(self) -> bool:
        """Return whether or not the workbook is currently locked."""
        return self._clients.notebook.get(self._clients.auth_header, self.rid).metadata.lock.is_locked

    def is_archived(self) -> bool:
        """Return whether or not the workbook is currently archived."""
        return self._clients.notebook.get(self._clients.auth_header, self.rid).metadata.is_archived

    def lock(self) -> None:
        """Locks the workbook, preventing changes from being made to it.

        Note:
            Locking is an idemponent operation-- calling lock() on a locked workbook
            will result in the workbook staying locked.
        """
        self._clients.notebook.lock(self._clients.auth_header, self.rid)

    def unlock(self) -> None:
        """Unlocks the workbook, allowing changes to be made to it.

        Note:
            Unlocking is an idemponent operation-- calling unlock() on an unlocked workbook
            will result in the workbook staying unlocked.
        """
        self._clients.notebook.unlock(self._clients.auth_header, self.rid)

    def archive(self) -> None:
        """Archive this workbook.
        Archived workbooks are not deleted, but are hidden from the UI.

        Note:
            Archiving is an idemponent operation-- calling archive() on a archived workbook
            will result in the workbook staying archived.
        """
        self._clients.notebook.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this workbook, allowing it to be viewed in the UI.

        Note:
            Unarchiving is an idemponent operation-- calling unarchive() on a unarchived workbook
            will result in the workbook staying unarchived.
        """
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
        workbook_type = WorkbookType._from_conjure(notebook.metadata.notebook_type)
        return cls(
            rid=notebook.rid,
            title=notebook.metadata.title,
            description=notebook.metadata.description,
            run_rids=notebook.metadata.data_scope.run_rids,
            asset_rids=notebook.metadata.data_scope.asset_rids,
            workbook_type=workbook_type,
            _clients=clients,
        )
