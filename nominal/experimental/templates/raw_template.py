import json
import uuid
from dataclasses import dataclass
from typing import Any, Union

from nominal_api import (
    scout_channelvariables_api,
    scout_chartdefinition_api,
    scout_layout_api,
    scout_rids_api,
    scout_template_api,
    scout_workbookcommon_api,
)

from nominal.experimental.templates.template_panel import (
    CartesianPanel,
    GeomapPanel,
    HistogramPanel,
    Panel,
    TimeseriesPanel,
)
from nominal.experimental.templates.template_utils import Comparisons

CHART_RID_BASE = "ri.scout.cerulean-staging.chart."

@dataclass
class TemplateTab:
    """Object used to represent a tab"""

    panels: list[Panel]
    name: str

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
        self, panel_one: Union[str, scout_layout_api.Panel], panel_two: Union[str, scout_layout_api.Panel]
    ) -> scout_layout_api.Panel:
        """Currently we define orientation choices. An extension could be to have more user definition"""
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

    def to_scout_SingleTab(self, panel_rids: list[str], title: str) -> scout_layout_api.SingleTab:
        """Currently supports a max of panels per tab. Can imagine how this can be extended as we
        support greater panel limts
        """
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
            raise RuntimeError(f"{len(panel_rids)} panels provided. Must be between 1 and 4")

        return scout_layout_api.SingleTab(v1=scout_layout_api.SingleTabV1(title=title, panel=panel))


@dataclass
class RawTemplate:
    """Object to represent template"""

    version: int  # yaml structure version
    title: str
    tabs: list[TemplateTab]
    labels: list[str]
    refname: str

    def __init__(self, data: dict[str, Any], refname: str):
        """Initialize template object from YAML input"""
        self.version, self.title = data["version"], data["title"]
        # Parse tabs
        tabs = []
        for tab_name, tab_data in data["tabs"].items():
            panels: list[Panel] = []

            if "panels" not in tab_data:
                raise ValueError(f"Could not find 'panels' for tab: {tab_name}. See docs.")

            # tab_data['panels'] should be a list of panel definitions
            for panel_data in tab_data["panels"]:
                comparison_runs = []

                # Parse comparison runs if they exist
                if "comparison_runs" in panel_data:
                    for run_name, run_info in panel_data["comparison_runs"].items():
                        comparison = Comparisons(
                            name=run_name,
                            color=run_info[0],  # First element is color
                            run_rids=[run_info[1]],  # Second element is rid, wrap in list
                        )
                        comparison_runs.append(comparison)

                # Get panel type or error
                panel_type = panel_data.get("type")
                panels.append(self._parse_yaml_panel(panel_type, panel_data, comparison_runs))

            # Create the tab
            tab = TemplateTab(panels=panels, name=tab_name)
            tabs.append(tab)

        self.tabs = tabs
        self.labels = data.get("labels", [])
        self.refname = refname

    def _parse_yaml_panel(
        self, panel_type: str, panel_data: dict[str, Any], comparison_runs: list[Comparisons]
    ) -> Panel:
        """Parses yaml representation of panel into object representation"""
        if panel_type == "TIMESERIES":
            return TimeseriesPanel(panel_data, comparison_runs)
        elif panel_type == "HISTOGRAM":
            return HistogramPanel(panel_data)
        elif panel_type == "SCATTER":
            return CartesianPanel(panel_data)
        elif panel_type == "GEOMAP":
            return GeomapPanel(panel_data)
        else:
            raise NotImplementedError(f"We do not yet support this panel type! {panel_type}")

    """Helper functions for channel variable defintions"""

    def _define_compute_spec_v1_as_JSON(self, channel_name: str, datasource_refname: str) -> dict[str, Any]:
        # TODO: is there a conjure object for this?
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

    def _create_all_variables(self) -> dict[str, scout_channelvariables_api.ChannelVariable]:
        """Defines all global channel variabls"""
        channel_list = [
            channel_name for tab in self.tabs for panel in tab.panels for channel_name in panel.get_channel_names()
        ]
        channel_map = {}
        channels = set(channel_list)
        channel_variables = {}
        compute_specs = [self._define_compute_spec_v1_as_JSON(c, self.refname) for c in channels]
        for i, channel in enumerate(channels):
            var_name = ""
            num = i+1
            # generate unique var name for all channels: 'a','b'...'z', 'aa', 'ab'...
            while num > 0:
                num -= 1
                remainder = num % 26
                var_name = chr(ord("a") + remainder) + var_name
                num = num // 26

            # create channel variable for row
            id, channel_var = list(self._define_channel_variable(var_name, channel, compute_specs[i]).items())[0]
            channel_variables[id] = channel_var
            channel_map[channel] = var_name

        return channel_variables, channel_map

    """Takes the current template object and turns it into a Template Request"""

    def create_request(self, commit_message: str) -> scout_template_api.CreateTemplateRequest:
        if self.version != 0:
            raise RuntimeError("Sorry this function only supports template v0")

        """Creates template request object"""
        # populate global channel variables
        channel_variables, channel_map = self._create_all_variables()
        # Create separate charts for each tab
        charts: dict[str, scout_chartdefinition_api.VizDefinition] = {}
        tabs: list[scout_layout_api.SingleTab] = []

        for tab_index, tab in enumerate(self.tabs):
            panel_charts = []
            for panel_index, panel in enumerate(tab.panels):
                # Create chart for this panel
                chart_rid = f"{CHART_RID_BASE}{uuid.uuid4()}"
                charts[chart_rid] = panel.to_viz_def(channel_map)
                panel_charts.append(chart_rid)
            # Create tab pointing to this chart
            tabs.append(tab.to_scout_SingleTab(panel_charts, tab.name))

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
            title=self.title,
            description="",
            labels=self.labels,
            properties={},
            layout=layout,
            message=commit_message,
            is_published=True,
        )
