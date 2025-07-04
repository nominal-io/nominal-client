from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence, overload

from nominal_api import scout, scout_notebook_api, scout_template_api, scout_workbookcommon_api
from typing_extensions import Self

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid
from nominal.core.workbook import Workbook


@dataclass(frozen=True)
class WorkbookTemplate(HasRid):
    rid: str
    title: str
    description: str
    labels: Sequence[str]
    properties: Mapping[str, str]
    published: bool
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def notebook(self) -> scout.NotebookService: ...
        @property
        def template(self) -> scout.TemplateService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook Template in the Nominal app"""
        return f"{self._clients.app_base_url}/workbooks/templates/{self.rid}"

    def update(
        self,
        *,
        description: str | None = None,
        title: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Self:
        """Replace template metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        NOTE: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in template.labels:
                new_labels.append(old_label)
            template = template.update(labels=new_labels)
        """
        # NOTE: not saving updated metadata response, as we deserialize from a template rather than
        #       from metadata
        self._clients.template.update_metadata(
            self._clients.auth_header,
            scout_template_api.UpdateMetadataRequest(
                description=description,
                title=title,
                labels=None if labels is None else [*labels],
                properties=None if properties is None else {**properties},
            ),
            self.rid,
        )
        template = self.__class__._from_conjure(
            self._clients, self._clients.template.get(self._clients.auth_header, self.rid)
        )
        update_dataclass(self, template, fields=self.__dataclass_fields__)
        return self

    def get_refnames(self) -> Sequence[str]:
        """Get the list of refnames used within the workbook."""
        return self._clients.template.get_used_ref_names(self._clients.auth_header, self.rid)

    def update_refnames(self, refname_map: Mapping[str, str]) -> None:
        """Updates refnames using a provided map of original refnames to the new refnames to replace them."""
        self._clients.template.update_ref_names(
            self._clients.auth_header, scout_template_api.UpdateRefNameRequest({**refname_map}), self.rid
        )

    @overload
    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
        run_rids: Sequence[str],
    ) -> Workbook: ...

    @overload
    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
        asset_rids: Sequence[str],
    ) -> Workbook: ...

    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
        run_rids: Sequence[str] | None = None,
        asset_rids: Sequence[str] | None = None,
    ) -> Workbook:
        """Create workbook from this workbook template

        Args:
            title: Title of the workbook to create. By default, uses the title of this template
            description: Description of the workbook to create. By default, uses the description of this template
            is_draft: If true, creates the workbook in "draft" mode
            run_rids: Runs to visualize in the workbook
                NOTE: may not be provided alongside `asset_rids`
                NOTE: only provide multiple run rids when instantiating a comparison workbook
            asset_rids: Assets to visualize in the workbook
                NOTE: may not be provided alongside `run_rids`

        Returns:
            The instantiated workbook
        """
        if run_rids is not None and asset_rids is not None:
            raise ValueError("Only one of `run_rids` and `asset_rids` may be used to create a workbook from a template")
        elif run_rids is None and asset_rids is None:
            raise ValueError("One of `run_rids` or `asset_rids` must be provided to create a workbook from a template")

        raw_template = self._clients.template.get(self._clients.auth_header, self.rid)
        request = scout_notebook_api.CreateNotebookRequest(
            title=title if title is not None else f"Workbook from {self.title}",
            description=description if description is not None else self.description,
            is_draft=is_draft,
            state_as_json="{}",
            data_scope=scout_notebook_api.NotebookDataScope(
                run_rids=[*run_rids] if run_rids else None, asset_rids=[*asset_rids] if asset_rids else None
            ),
            layout=raw_template.layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=raw_template.content),
            event_refs=[],
            workspace=self._clients.workspace_rid,
        )
        raw_notebook = self._clients.notebook.create(self._clients.auth_header, request)
        return Workbook._from_conjure(self._clients, raw_notebook)

    @classmethod
    def _from_conjure(cls, clients: _Clients, template: scout_template_api.Template) -> Self:
        return cls._from_template_summary(
            clients, scout_template_api.TemplateSummary(metadata=template.metadata, rid=template.rid)
        )

    @classmethod
    def _from_template_summary(cls, clients: _Clients, template: scout_template_api.TemplateSummary) -> Self:
        return cls(
            rid=template.rid,
            title=template.metadata.title,
            description=template.metadata.description,
            labels=template.metadata.labels,
            properties=template.metadata.properties,
            published=template.metadata.is_published,
            _clients=clients,
        )
