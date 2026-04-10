from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence, overload

from nominal_api import (
    scout,
    scout_chartdefinition_api,
    scout_layout_api,
    scout_notebook_api,
    scout_template_api,
    scout_workbookcommon_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin, rid_from_instance_or_string
from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook, WorkbookType


def _rebind_video_datasources(
    content: scout_workbookcommon_api.WorkbookContent,
    asset_rid: str,
    run_rid: str | None,
) -> scout_workbookcommon_api.WorkbookContent:
    """Re-bind video panel v1 datasources with the new asset/run RIDs on template instantiation.

    Templates strip datasource RIDs on save; this restores them so the video panel
    can load the correct asset.

    # TODO(@seanmreidy): Remove once videos are migrated to channels.
    """
    new_charts: dict[str, scout_chartdefinition_api.VizDefinition] = {}
    for chart_id, viz in content.charts.items():
        if viz.video is not None and viz.video.v1 is not None:
            v1 = viz.video.v1
            ref_name = (v1.datasource.ref_name if v1.datasource is not None else None) or v1.ref_name or "default"
            new_v1 = scout_chartdefinition_api.VideoVizDefinitionV1(
                comparison_run_groups=v1.comparison_run_groups,
                datasource=scout_chartdefinition_api.VideoPanelDataSource(
                    asset_rid=asset_rid,
                    ref_name=ref_name,
                    run_rid=run_rid,
                ),
                events=v1.events,
                ref_name=v1.ref_name,
                title=v1.title,
            )
            new_charts[chart_id] = scout_chartdefinition_api.VizDefinition(
                video=scout_chartdefinition_api.VideoVizDefinition(v1=new_v1)
            )
        else:
            new_charts[chart_id] = viz

    return scout_workbookcommon_api.WorkbookContent(
        channel_variables=content.channel_variables,
        charts=new_charts,
        data_scope_inputs=content.data_scope_inputs,
        inputs=content.inputs,
        settings=content.settings,
    )


@dataclass(frozen=True)
class WorkbookTemplate(HasRid, RefreshableMixin[scout_template_api.Template]):
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
        def run(self) -> scout.RunService: ...
        @property
        def template(self) -> scout.TemplateService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Workbook Template in the Nominal app"""
        return f"{self._clients.app_base_url}/workbooks/templates/{self.rid}"

    def _get_latest_api(self) -> scout_template_api.Template:
        return self._clients.template.get(self._clients.auth_header, self.rid)

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
        return self.refresh()

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
        is_draft: bool = False,
    ) -> Workbook: ...

    @overload
    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        asset: Asset | str,
        is_draft: bool = False,
    ) -> Workbook: ...

    def create_workbook(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        run: Run | str | None = None,
        asset: Asset | str | None = None,
        is_draft: bool = False,
    ) -> Workbook:
        """Create workbook from this workbook template

        Args:
            title: Title of the workbook to create. By default, uses the title of this template
            description: Description of the workbook to create. By default, uses the description of this template
            run: Run to visualize in the workbook
                NOTE: may not be provided alongside `asset`
            asset: Asset to visualize in the workbook
                NOTE: may not be provided alongside `run`
            is_draft: Whether to create the workbook in draft state. Defaults to False.

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
        template_content = raw_template.content

        # Re-bind video panel datasources that were stripped when the template was saved.
        has_video = any(viz.video is not None and viz.video.v1 is not None for viz in template_content.charts.values())
        if has_video:
            run_rid = rid_from_instance_or_string(run) if run is not None else None
            video_asset_rid = None
            if asset is not None:
                video_asset_rid = rid_from_instance_or_string(asset)
            elif isinstance(run, Run) and run.assets:
                video_asset_rid = run.assets[0]
            elif run_rid is not None:
                raw_run = self._clients.run.get_run(self._clients.auth_header, run_rid)
                video_asset_rid = raw_run.assets[0] if raw_run.assets else None
            else:
                raise ValueError(
                    f"Could not resolve asset RID for video panel datasource re-binding. run={run!r}, asset={asset!r}"
                )
            if video_asset_rid is not None:
                template_content = _rebind_video_datasources(template_content, video_asset_rid, run_rid)

        request = scout_notebook_api.CreateNotebookRequest(
            title=f"Workbook from '{self.title}'" if title is None else title,
            description=self.description if description is None else description,
            is_draft=is_draft,
            state_as_json="{}",
            data_scope=scout_notebook_api.NotebookDataScope(
                run_rids=None if run is None else [rid_from_instance_or_string(run)],
                asset_rids=None if asset is None else [rid_from_instance_or_string(asset)],
            ),
            layout=raw_template.layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=template_content),
            event_refs=[],
            workspace=self._clients.resolve_default_workspace_rid(),
        )
        raw_notebook = self._clients.notebook.create(self._clients.auth_header, request)
        return Workbook._from_conjure(self._clients, raw_notebook)

    def is_published(self) -> bool:
        """Returns whether or not the workbook template has been published and can be viewed by other users."""
        raw_template = self._clients.template.get(self._clients.auth_header, self.rid)
        return raw_template.metadata.is_published

    def archive(self) -> None:
        """Archive this workbook template.
        Archived workbook templates are not deleted, but are hidden from the UI.
        """
        self._clients.template.update_metadata(
            self._clients.auth_header, scout_template_api.UpdateMetadataRequest(is_archived=True), self.rid
        )

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


def _create_workbook_template_with_content_and_layout(
    clients: WorkbookTemplate._Clients,
    title: str,
    layout: scout_layout_api.WorkbookLayout,
    content: scout_workbookcommon_api.WorkbookContent,
    workspace_rid: str,
    *,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    commit_message: str | None = None,
    is_published: bool = False,
) -> WorkbookTemplate:
    """Create a workbook template with specified content and layout.

    This is a helper method that constructs and creates a workbook template
    request with the provided parameters, including layout and content.  Method is considered experimental and may
    change in future releases. The template is created in the target workspace and is not discoverable by default.

    Args:
        clients: The clients to use for API calls.
        title: The title of the template.
        layout: The workbook layout to use.
        content: The workbook content to use.
        workspace_rid: The resource ID of the workspace to create the template in.
        description: The description of the template.
        labels: List of labels to apply to the template.
        properties: Dictionary of properties for the template.
        commit_message: The commit message for the template creation.
        is_published: If True, the template will show up as published.

    Returns:
        The newly created WorkbookTemplate.
    """
    request = scout_template_api.CreateTemplateRequest(
        title=title,
        description=description if description is not None else "",
        labels=list(labels) if labels is not None else [],
        properties=dict(properties) if properties is not None else {},
        is_published=is_published,
        layout=layout,
        content=content,
        message=commit_message if commit_message is not None else "",
        workspace=workspace_rid,
    )

    template = clients.template.create(clients.auth_header, request)
    return WorkbookTemplate._from_conjure(clients, template)
