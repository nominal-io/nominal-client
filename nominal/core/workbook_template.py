from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence, overload

from nominal_api import scout, scout_notebook_api, scout_template_api, scout_workbookcommon_api
from typing_extensions import Self

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, rid_from_instance_or_string
from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook, WorkbookType


@dataclass(frozen=True)
class WorkbookTemplate(HasRid):
    rid: str
    title: str
    description: str
    labels: Sequence[str]
    properties: Mapping[str, str]
    workbook_type: WorkbookType
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
        run: Run | str,
    ) -> Workbook: ...

    @overload
    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        asset: Asset | str,
    ) -> Workbook: ...

    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        run: Run | str | None = None,
        asset: Asset | str | None = None,
    ) -> Workbook:
        """Create workbook from this workbook template

        Args:
            title: Title of the workbook to create. By default, uses the title of this template
            description: Description of the workbook to create. By default, uses the description of this template
            run: Run to visualize in the workbook
                NOTE: may not be provided alongside `asset`
            asset: Asset to visualize in the workbook
                NOTE: may not be provided alongside `run`

        NOTE: only supports singular `run` instead of a list of `runs` because workbook templates only support
              standard workbooks and not comparison workbooks.
        NOTE: only supports singular `asset` instead of a list of `assets` because workbook templates only support
              single asset workbooks.

        Returns:
            The instantiated workbook
        """
        if run is not None and asset is not None:
            raise ValueError("Only one of `run` and `asset` may be used to create a workbook from a template")
        elif run is None and asset is None:
            raise ValueError("One of `run` or `asset` must be provided to create a workbook from a template")

        raw_template = self._clients.template.get(self._clients.auth_header, self.rid)
        request = scout_notebook_api.CreateNotebookRequest(
            title=f"Workbook from '{self.title}'" if title is None else title,
            description=self.description if description is None else description,
            is_draft=False,
            state_as_json="{}",
            data_scope=scout_notebook_api.NotebookDataScope(
                run_rids=None if run is None else [rid_from_instance_or_string(run)],
                asset_rids=None if asset is None else [rid_from_instance_or_string(asset)],
            ),
            layout=raw_template.layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=raw_template.content),
            event_refs=[],
            workspace=self._clients.workspace_rid,
        )
        raw_notebook = self._clients.notebook.create(self._clients.auth_header, request)
        return Workbook._from_conjure(self._clients, raw_notebook)

    def is_published(self) -> bool:
        """Returns whether or not the workbook template has been published and can be viewed by other users."""
        raw_template = self._clients.template.get(self._clients.auth_header, self.rid)
        return raw_template.metadata.is_published

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
            workbook_type=WorkbookType.COMPARISON_WORKBOOK,
            _clients=clients,
        )
