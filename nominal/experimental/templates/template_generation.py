import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, TextIO

import yaml
from nominal_api import (
    api,
    scout_channelvariables_api,
    scout_chartdefinition_api,
    scout_comparisonrun_api,
    scout_layout_api,
    scout_rids_api,
    scout_template_api,
    scout_workbookcommon_api,
)

from nominal.core.client import NominalClient
from nominal.core.workbook_template import WorkbookTemplate

CHART_RID_BASE = "ri.scout.cerulean-staging.chart."


class SupportedPanels(Enum):
    """Current channels we are able to support in sdk implementation of programmatic
    template generation
    """

    TIMESERIES = 1


TemplateAxis = tuple[str, str]  # (axis title, axis side (0=R/1=L))
TemplateRow = dict[str, tuple[str, TemplateAxis]]  # {channel name: (color, TemplateAxis)}


@dataclass
class Comparisons:
    """Comparison run objects"""

    name: str
    color: str
    run_rids: list[str]  # TODO: maybe take something else than rids?


@dataclass
class Template_Panel:
    type: SupportedPanels
    row_data: list[TemplateRow]
    row_names: list[str]
    comparison_runs: list[Comparisons]


@dataclass
class Template_Tab:
    """Object used to represent a tab"""

    panels: list[Template_Panel]
    name: str


@dataclass
class Raw_Template:
    """Pythonic object we are using to represent template...
    subject to change
    """

    version: int
    title: str
    tabs: list[Template_Tab]
    labels: list[str]


class TemplateGenerator:
    def __init__(self, client: NominalClient, reference_refname: str):
        """Define a template generator object"""
        self.client = client
        self.refname = reference_refname
        self._channel_map: dict[str, str] = {}  # channel variables are global for a WB

    def _parse_yaml_to_raw_template(self, yaml_input: str | TextIO) -> Raw_Template:
        try:
            if isinstance(yaml_input, str):
                with open(yaml_input, "r") as file:
                    data = yaml.safe_load(file)
            else:
                data = yaml.safe_load(yaml_input)
        except Exception as e:
            raise IOError(f"Error opening file: {e}")

        # Parse tabs
        tabs = []
        for tab_name, tab_data in data["tabs"].items():
            panels = []

            # tab_data['panels'] should be a list of panel definitions
            for panel_data in tab_data["panels"]:
                row_data = []
                row_names = []
                comparison_runs = []

                # Get panel type or error
                panel_type = SupportedPanels.TIMESERIES if panel_data.get("type") == "TIMESERIES" else None
                if not panel_type:
                    raise RuntimeError("Illegal panel type! We currently only support timeseries")

                # Parse rows
                if "rows" in panel_data:
                    for row_name, row_channels in panel_data["rows"].items():
                        row_names.append(row_name)
                        template_row = {}

                        # Parse channels within the row
                        for channel_name, channel_info in row_channels.items():
                            color = channel_info[0]
                            axis_name = channel_info[1]
                            axis_side = channel_info[2]

                            template_axis = (axis_name, axis_side)
                            template_row[channel_name.replace("/", ".")] = (color, template_axis)

                        row_data.append(template_row)

                # Parse comparison runs if they exist
                if "comparison_runs" in panel_data:
                    for run_name, run_info in panel_data["comparison_runs"].items():
                        comparison = Comparisons(
                            name=run_name,
                            color=run_info[0],  # First element is color
                            run_rids=[run_info[1]],  # Second element is rid, wrap in list
                        )
                        comparison_runs.append(comparison)

                # Create the panel
                panel = Template_Panel(
                    type=panel_type, row_data=row_data, row_names=row_names, comparison_runs=comparison_runs
                )
                panels.append(panel)

            # Create the tab
            tab = Template_Tab(panels=panels, name=tab_name)
            tabs.append(tab)

        # Create and return the Raw_Template
        return Raw_Template(version=data["version"], title=data["name"], tabs=tabs, labels=data["labels"])

    def _create_comparison_run_group(
        self, name: str, color: str, run_rids: list[str]
    ) -> scout_comparisonrun_api.ComparisonRunGroup:
        return scout_comparisonrun_api.ComparisonRunGroup(
            name=name,
            runs=[self._create_comparison_run(rid) for rid in run_rids],
            uuid=str(uuid.uuid4()),
            color=color,
            offset=scout_comparisonrun_api.Offset(api.TimeUnit.SECONDS, 0),
            offset_anchor=scout_comparisonrun_api.OffsetAnchor(run=scout_comparisonrun_api.OffsetRunAnchor()),
        )

    def _create_comparison_run(self, run_rid: str) -> scout_comparisonrun_api.ComparisonRun:
        return scout_comparisonrun_api.ComparisonRun(enabled=True, run_rid=run_rid)

    # TODO: is there a conjure object for this?
    def _define_compute_spec_v1_as_JSON(self, channel_name: str, datasource_refname: str) -> dict[str, Any]:
        function_uuid = str(uuid.uuid4())
        return {
            "type": "fn",
            "fn": {
                "functionId": "numericSeries",
                "functionUuid": function_uuid,
                "args": {
                    "input": {
                        "type": "numericSeriesType",
                        "provider": {
                            "type": "channel",
                            "channel": {
                                "channelName": channel_name,
                                "dataSourceRefName": datasource_refname,
                                "assetRidVariableName": "assetRid",
                                "tagsToGroupBy": [],
                            },
                        },
                    }
                },
            },
        }

    def _define_channel_variable(
        self, letter: str, channel_name: str, compute_spec_v1: dict[str, Any]
    ) -> dict[str, scout_channelvariables_api.ChannelVariable]:
        return {
            letter: scout_channelvariables_api.ChannelVariable(
                variable_name=letter,
                display_name=channel_name,
                compute_spec=scout_channelvariables_api.ComputeSpec(v1=json.dumps(compute_spec_v1)),
            )
        }

    def _create_channel_axis(self, axis_id: str, axis_obj: TemplateAxis) -> scout_chartdefinition_api.ValueAxis:
        axis_name, side = axis_obj
        return scout_chartdefinition_api.ValueAxis(
            id=axis_id,
            title=axis_name,
            display_options=scout_chartdefinition_api.AxisDisplayOptions(
                show_title=True, axis_width=None, scale_type=None
            ),
            range=scout_chartdefinition_api.AxisRange(start=None, end=None),
            limit=scout_chartdefinition_api.AxisRange(start=None, end=None),
            position=scout_chartdefinition_api.AxisPosition.LEFT
            if int(side) == 0
            else scout_chartdefinition_api.AxisPosition.RIGHT,
            domain_type=scout_chartdefinition_api.AxisDomainType.NUMERIC,
        )

    def _create_plot(self, var_name: str, axis_id: str, color: str) -> scout_chartdefinition_api.TimeSeriesPlotV2:
        return scout_chartdefinition_api.TimeSeriesPlotV2(
            type=scout_chartdefinition_api.TimeSeriesPlotConfig(
                scout_chartdefinition_api.TimeSeriesNumericPlot(
                    color=color,
                    line_style=scout_chartdefinition_api.LineStyle(
                        v1=scout_chartdefinition_api.LineStyleV1.SOLID  # TODO: styly choice?
                    ),
                )
            ),
            variable_name=var_name,
            y_axis_id=axis_id,
            enabled=True,
        )

    def _create_row(
        self, plots: list[scout_chartdefinition_api.TimeSeriesPlotV2], row_name: str
    ) -> scout_chartdefinition_api.TimeSeriesRow:
        return scout_chartdefinition_api.TimeSeriesRow(
            plots=[], plots_v2=plots, row_flex_size=1, title=row_name, enabled=True
        )

    def _create_all_variables(self, channels: set[str]) -> dict[str, scout_channelvariables_api.ChannelVariable]:
        channel_variables = {}
        compute_specs = [self._define_compute_spec_v1_as_JSON(c, self.refname) for c in channels]
        for i, channel in enumerate(channels):
            var_name = chr(ord("a") + i)  # "a", "b", "c"...

            # create channel variable for row
            id, channel_var = list(self._define_channel_variable(var_name, channel, compute_specs[i]).items())[0]
            channel_variables[id] = channel_var
            self._channel_map[channel] = var_name

        return channel_variables

    def _create_single_panel(self, chart_rid: str) -> scout_layout_api.Panel:
        return scout_layout_api.Panel(
            chart=scout_layout_api.ChartPanel(
                v1=scout_layout_api.ChartPanelV1(
                    id=str(uuid.uuid4()),
                    chart_rid=scout_rids_api.VersionedVizId(rid=chart_rid, version=1),
                    hide_legend=False,
                )
            )
        )

    def _create_split_panel(
        self, panel_one: str | scout_layout_api.Panel, panel_two: str | scout_layout_api.Panel
    ) -> scout_layout_api.Panel:
        # TODO: orientation choice?
        vert, horz = scout_layout_api.SplitPanelOrientation.VERTICAL, scout_layout_api.SplitPanelOrientation.HORIZONTAL
        return scout_layout_api.Panel(
            split=scout_layout_api.SplitPanel(
                v1=scout_layout_api.SplitPanelV1(
                    id=str(uuid.uuid4()),
                    side_one=self._create_single_panel(panel_one) if isinstance(panel_one, str) else panel_one,
                    side_two=self._create_single_panel(panel_two) if isinstance(panel_two, str) else panel_two,
                    orientation=horz if all([isinstance(panel, str) for panel in (panel_one, panel_two)]) else vert,
                )
            )
        )

    def _create_timeseries_chart(
        self,
        rows: list[scout_chartdefinition_api.TimeSeriesRow],
        axes: list[scout_chartdefinition_api.ValueAxis],
        comparison_run_groups: list[Comparisons],
    ) -> scout_chartdefinition_api.VizDefinition:
        comparison_run_objects = [
            self._create_comparison_run_group(cmp.name, cmp.color, cmp.run_rids) for cmp in comparison_run_groups
        ]
        return scout_chartdefinition_api.VizDefinition(
            time_series=scout_chartdefinition_api.TimeSeriesChartDefinition(
                v1=scout_chartdefinition_api.TimeSeriesChartDefinitionV1(
                    title="Time series chart",
                    rows=rows,
                    value_axes=axes,
                    comparison_run_groups=comparison_run_objects,
                    events=None,
                    thresholds=[],
                )
            )
        )

    def _create_tab(self, panel_rids: list[str], title: str) -> scout_layout_api.SingleTab:
        if len(panel_rids) == 1:
            panel = self._create_single_panel(panel_rids[0])
        elif len(panel_rids) == 2:
            panel = self._create_split_panel(panel_one=panel_rids[0], panel_two=panel_rids[1])
        elif len(panel_rids) == 3:
            panel = self._create_split_panel(
                panel_one=self._create_split_panel(panel_one=panel_rids[0], panel_two=panel_rids[1]),
                panel_two=self._create_single_panel(panel_rids[2]),
            )
        elif len(panel_rids) == 4:
            panel = self._create_split_panel(
                panel_one=self._create_split_panel(panel_one=panel_rids[0], panel_two=panel_rids[1]),
                panel_two=self._create_split_panel(panel_one=panel_rids[2], panel_two=panel_rids[3]),
            )
        else:
            raise RuntimeError(f"{len(panel_rids)} panels provided. Max is 4")

        return scout_layout_api.SingleTab(v1=scout_layout_api.SingleTabV1(title=title, panel=panel))

    def _create_template_request(
        self,
        raw_template: Raw_Template,
    ) -> scout_template_api.CreateTemplateRequest:
        if raw_template.version != 0:
            raise RuntimeError("Sorry this function only supports template v0")

        """Creates template request object"""
        channel_name_list = [
            channel_name
            for tab in raw_template.tabs
            for panel in tab.panels
            for row in panel.row_data
            for channel_name in row.keys()
        ]

        channel_variables = self._create_all_variables(set(channel_name_list))
        # Create separate charts for each tab
        charts: dict[str, scout_chartdefinition_api.VizDefinition] = {}
        tabs: list[scout_layout_api.SingleTab] = []

        var_index = 0
        for tab_index, tab in enumerate(raw_template.tabs):
            panel_charts = []
            for panel_index, panel in enumerate(tab.panels):
                # Create chart for this panel
                chart_rid = f"{CHART_RID_BASE}{uuid.uuid4()}"

                # Create rows/axes for just this panel's channels
                panel_rows = []
                panel_axes = []

                for row_index, row in enumerate(panel.row_data):
                    row_plots = []

                    axis_mappings: dict[TemplateAxis, str] = {}

                    for channel_name, (color, axis_name) in row.items():
                        # plot identifier
                        var_name = self._channel_map[channel_name]
                        # make plot with appropriate color
                        if axis_name in axis_mappings:
                            axis_id = axis_mappings[axis_name]
                        else:
                            axis_id = str(uuid.uuid4())
                            axis_mappings[axis_name] = axis_id
                            panel_axes.append(self._create_channel_axis(axis_id, axis_name))

                        row_plots.append(self._create_plot(var_name, axis_id, color))
                        var_index += 1

                    row_name = panel.row_names[row_index]
                    panel_rows.append(self._create_row(row_plots, row_name))

                # Create chart with just this panel's rows/axes
                if panel.type == SupportedPanels.TIMESERIES:
                    charts[chart_rid] = self._create_timeseries_chart(panel_rows, panel_axes, panel.comparison_runs)
                    panel_charts.append(chart_rid)
                else:
                    # TODO: only supports timeseries for now
                    continue

            # Create tab pointing to this chart
            tabs.append(self._create_tab(panel_charts, tab.name))

        # Create workbook content object
        content = scout_workbookcommon_api.WorkbookContent(channel_variables=channel_variables, charts=charts)

        # Create workbook layout object
        layout = scout_layout_api.WorkbookLayout(
            v1=scout_layout_api.WorkbookLayoutV1(
                root_panel=scout_layout_api.Panel(
                    tabbed=scout_layout_api.TabbedPanel(
                        v1=scout_layout_api.TabbedPanelV1(id=str(uuid.uuid4()), tabs=tabs)
                    )
                )
            )
        )

        return scout_template_api.CreateTemplateRequest(
            content=content,
            title=raw_template.title,
            description="",
            labels=raw_template.labels,
            properties={},
            layout=layout,
            message="test",
            is_published=True,
        )

    def create_template_from_yaml(self, yaml_input: str | TextIO) -> WorkbookTemplate:
        """Main user facing function for creating template.
        TODO: Think of better object for user to pass. Would be better to use primitives
               and create object ourselves
        """
        template = self._parse_yaml_to_raw_template(yaml_input)
        request = self._create_template_request(template)

        conjure_template = self.client._clients.template.create(self.client._clients.auth_header, request)
        return WorkbookTemplate._from_conjure(self.client._clients, conjure_template)
