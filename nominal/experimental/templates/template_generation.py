import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, TextIO, Union

import yaml
from nominal_api import (
    api,
    scout_channelvariables_api,
    scout_chartdefinition_api,
    scout_comparisonrun_api,
    scout_compute_api,
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
    SCATTER = 2
    HISTOGRAM = 3
    GEOMAP = 4


"""Helper classes for Histogram template"""


@dataclass
class CountStrategy:
    num_buckets: int


@dataclass
class WidthStrategy:
    bucket_width: float
    offset: float


"""Aliases for timeseries template objects"""
TemplateAxis = tuple[str, str]  # (axis title, axis side (0=R/1=L))
TemplateRow = dict[str, tuple[str, TemplateAxis]]  # {channel name: (color, TemplateAxis)}

"""Alias for scatter plot objects"""
TemplatePlot = TemplateRow


@dataclass
class Comparisons:
    """Comparison run objects"""

    name: str
    color: str
    run_rids: list[str]  # TODO: maybe take something else than rids?


@dataclass
class Panel:
    """Base class for different panels we support
    Model is based on the assumption that all panel types will
    eventually support comparison runs
    """

    type: SupportedPanels
    comparison_runs: list[Comparisons]


@dataclass
class Timeseries_Panel(Panel):
    row_data: list[TemplateRow]
    row_names: list[str]


@dataclass
class Cartesian_Panel(Panel):
    x_axis_data: tuple[str, str]  # <channel_name, axis_name>
    y_axis_data: TemplatePlot


@dataclass
class Histogram_Panel(Panel):
    channels_w_colors: list[tuple[str, str]]  # <channel_name, color>
    stacked: bool = False
    bucket_strat: Union[WidthStrategy, CountStrategy, None] = None


@dataclass
class Geomap_Panel(Panel):
    latlongs_w_color: list[tuple[str, tuple[str, str, str]]]  # <plot_name, (lat_channel, long_channel, color)>
    plot_type: str  # 'STREET' or 'SATELLITE'
    geopoints: list[tuple[float, float]]  # <lat_val, long_val>


@dataclass
class Template_Tab:
    """Object used to represent a tab"""

    panels: list[Panel]
    name: str


@dataclass
class Raw_Template:
    """Object to represent template"""

    version: int  # yaml structure version
    title: str
    tabs: list[Template_Tab]
    labels: list[str]


class TemplateGenerator:
    def __init__(self, client: NominalClient, reference_refname: str):
        """Define a template generator object
        Important to not that templates are specific to a refname so defining
        this accurately is important
        """
        self.client = client
        self.refname = reference_refname
        self._channel_map: dict[str, str] = {}  # channel variables are global for a WB

    def _get_panel_type(self, type: str) -> SupportedPanels:
        if type == "TIMESERIES":
            return SupportedPanels.TIMESERIES
        elif type == "SCATTER":
            return SupportedPanels.SCATTER
        elif type == "HISTOGRAM":
            return SupportedPanels.HISTOGRAM
        elif type == "GEOMAP":
            return SupportedPanels.GEOMAP
        else:
            raise NotImplementedError(f"We currently do not support {type}")

    def _parse_yaml_timeseries_panel(
        self, panel_data: dict[str, Any], comparison_runs: list[Comparisons]
    ) -> Timeseries_Panel:
        row_data = []
        row_names = []
        # Parse rows
        for row_name, row_channels in panel_data["rows"].items():
            row_names.append(row_name)
            template_row = {}

            try:
                # Parse channels within the row
                for channel_name, channel_info in row_channels.items():
                    color = channel_info[0]
                    axis_name = channel_info[1]
                    axis_side = channel_info[2]

                    template_axis = (axis_name, axis_side)
                    # TODO: question - do we universally replace / with . ?
                    template_row[channel_name.replace("/", ".")] = (color, template_axis)

                row_data.append(template_row)
            except Exception:
                raise ValueError(f"Bad structure of timeseries channel structure for row: {row_name}")

        return Timeseries_Panel(
            type=SupportedPanels.TIMESERIES, row_data=row_data, row_names=row_names, comparison_runs=comparison_runs
        )

    def _parse_yaml_scatter_panel(
        self, panel_data: dict[str, Any], comparison_runs: list[Comparisons]
    ) -> Cartesian_Panel:
        plots_data = panel_data["plots"]

        # Parse x_axis data
        try:
            x_axis_info = plots_data["x_axis"]
            x_channel_name = x_axis_info[0]
            x_axis_title = x_axis_info[1]
            x_axis_data = (x_channel_name.replace("/", "."), x_axis_title)
        except Exception:
            raise ValueError("Invalid x axis data for scatter panel! See docs.")

        # Parse y_axis data
        try:
            y_axis_info = plots_data["y_axis"]
            y_axis_data = {}

            for channel_name, channel_data in y_axis_info.items():
                color = channel_data[0]
                axis_title = channel_data[1]
                axis_side = channel_data[2]

                template_axis = (axis_title, axis_side)
                y_axis_data[channel_name.replace("/", ".")] = (color, template_axis)
        except Exception:
            raise ValueError("Invalid y axis data for scatter panel! See docs.")

        return Cartesian_Panel(
            type=SupportedPanels.SCATTER,
            x_axis_data=x_axis_data,
            y_axis_data=y_axis_data,
            comparison_runs=comparison_runs,
        )

    def _parse_yaml_bucket_strategy(
        self, data: Union[None, dict[str, Any]]
    ) -> Union[None, WidthStrategy, CountStrategy]:
        if not data:
            return None
        try:
            strat_type = data["type"]
        except Exception:
            raise ValueError("Histogram bucket strategy could not be determined. Ensure 'type' field is present")

        if strat_type == "COUNT":
            return CountStrategy(num_buckets=data["num_buckets"])
        elif strat_type == "WIDTH":
            return WidthStrategy(bucket_width=float(data["bucket_width"]), offset=float(data.get("offset", 0)))
        else:
            raise ValueError(f"Bucket strategy: {strat_type} NOT SUPPORTED")

    def _parse_yaml_histogram_panel(self, panel_data: dict[str, Any]) -> Histogram_Panel:
        try:
            channels_with_colors = [(pair[0], pair[1]) for pair in panel_data["channels"]]
        except Exception:
            raise ValueError("Bad channel structure for histogram panel! See docs.")
        bucket_strat = self._parse_yaml_bucket_strategy(panel_data.get("bucket_strategy"))
        return Histogram_Panel(
            type=SupportedPanels.HISTOGRAM,
            bucket_strat=bucket_strat,
            stacked=True if panel_data.get("stacked") == "true" else False,
            channels_w_colors=channels_with_colors,
            comparison_runs=[],
        )

    def _parse_yaml_geomap_panel(self, panel_data: dict[str, Any]) -> Geomap_Panel:
        latlongs_w_color: list[tuple[str, tuple[str, str, str]]] = []
        geopoints = []

        plot_type = panel_data.get("tile_type", "STREET")

        if "geopoints" in panel_data:
            for point in panel_data["geopoints"]:
                lat_val, long_val = point[0], point[1]
                geopoints.append((lat_val, long_val))

        plots_data = panel_data["plots"]

        try:
            for plot_name, plot_data in plots_data.items():
                lat_channel = plot_data[0]
                long_channel = plot_data[1]
                color = plot_data[2]

                latlongs_w_color.append((plot_name, (lat_channel, long_channel, color)))
        except Exception:
            raise ValueError("Bad channel structure for geomap panel! See docs")

        return Geomap_Panel(SupportedPanels.GEOMAP, [], latlongs_w_color, plot_type, geopoints)

    def _safe_open(self, yaml_input: Union[str, TextIO]) -> Any:
        """Open either a file object or a filepath"""
        try:
            if isinstance(yaml_input, str):
                with open(yaml_input, "r") as file:
                    data = yaml.safe_load(file)
            else:
                data = yaml.safe_load(yaml_input)
        except Exception as e:
            raise IOError(f"Error opening file: {e}")
        return data

    def _parse_yaml_to_raw_template(self, yaml_input: Union[str, TextIO]) -> Raw_Template:
        """Main parsing script for the YAML template"""
        data = self._safe_open(yaml_input)
        if "tabs" not in data:
            raise ValueError("Could not find 'tabs' in yaml. See docs for structure")

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
                panel_type = self._get_panel_type(panel_data.get("type"))

                if panel_type == SupportedPanels.TIMESERIES and "rows" in panel_data:
                    panels.append(self._parse_yaml_timeseries_panel(panel_data, comparison_runs))
                elif panel_type == SupportedPanels.SCATTER and "plots" in panel_data:
                    # TODO: currently cant do comparison runs for non timeseries panel (even in FE)
                    panels.append(self._parse_yaml_scatter_panel(panel_data, comparison_runs))
                elif panel_type == SupportedPanels.HISTOGRAM and "channels" in panel_data:
                    panels.append(self._parse_yaml_histogram_panel(panel_data))
                elif panel_type == SupportedPanels.GEOMAP and "plots" in panel_data:
                    panels.append(self._parse_yaml_geomap_panel(panel_data))
                else:
                    raise NotImplementedError(f"We do not yet support this panel type! {panel_type}")

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
        """Currently creates a generally ranged channel axis. An extension could be to have more
        user definition within the axis creation if necessary.
        """
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

    def _create_timeseries_plot(
        self, var_name: str, axis_id: str, color: str
    ) -> scout_chartdefinition_api.TimeSeriesPlotV2:
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

    def _create_cartesian_plot(
        self, var_name_x: str, var_name_y: str, axis_id_x: str, axis_id_y: str, color: str
    ) -> scout_chartdefinition_api.CartesianPlot:
        return scout_chartdefinition_api.CartesianPlot(
            color=color,
            x_axis_id=axis_id_x,
            x_variable_name=var_name_x,
            y_axis_id=axis_id_y,
            y_variable_name=var_name_y,
            enabled=True,
        )

    def _create_histogram_plot(self, color: str, var_name: str) -> scout_chartdefinition_api.HistogramPlot:
        return scout_chartdefinition_api.HistogramPlot(color=color, variable_name=var_name, enabled=True)

    def _create_histogram_bucket_strategy(
        self, bucket_strat: Union[WidthStrategy, CountStrategy]
    ) -> scout_compute_api.NumericHistogramBucketStrategy:
        if isinstance(bucket_strat, WidthStrategy):
            return scout_compute_api.NumericHistogramBucketStrategy(
                bucket_width_and_offset=scout_compute_api.NumericHistogramBucketWidthAndOffset(
                    width=scout_compute_api.DoubleConstant(literal=bucket_strat.bucket_width),
                    offset=scout_compute_api.DoubleConstant(literal=bucket_strat.offset),
                )
            )
        else:
            return scout_compute_api.NumericHistogramBucketStrategy(
                bucket_count=scout_compute_api.IntegerConstant(literal=bucket_strat.num_buckets)
            )

    def _create_timeseries_row(
        self, plots: list[scout_chartdefinition_api.TimeSeriesPlotV2], row_name: str
    ) -> scout_chartdefinition_api.TimeSeriesRow:
        return scout_chartdefinition_api.TimeSeriesRow(
            plots=[], plots_v2=plots, row_flex_size=1, title=row_name, enabled=True
        )

    def _get_channel_names_from_panel(self, panel: Panel) -> list[str]:
        """Extract channel names from a panel based on its type"""
        if panel.type == SupportedPanels.TIMESERIES and isinstance(panel, Timeseries_Panel):
            return [channel_name for row in panel.row_data for channel_name in row.keys()]
        elif panel.type == SupportedPanels.SCATTER and isinstance(panel, Cartesian_Panel):
            channels = []
            channels.append(panel.x_axis_data[0])
            channels.extend(panel.y_axis_data.keys())
            return channels
        elif panel.type == SupportedPanels.HISTOGRAM and isinstance(panel, Histogram_Panel):
            return [channel[0] for channel in panel.channels_w_colors]
        elif panel.type == SupportedPanels.GEOMAP and isinstance(panel, Geomap_Panel):
            return [val for plot in panel.latlongs_w_color for val in plot[1][0:1]]
        else:
            raise NotImplementedError(f"Channel extraction not implemented for panel type: {panel.type}")

    def _create_all_variables(self, template: Raw_Template) -> dict[str, scout_channelvariables_api.ChannelVariable]:
        """Defines all global channel variabls (currently just using letters starting from a)
        TODO: check if this will cause eventual bugs
        """
        channel_list = [
            channel_name
            for tab in template.tabs
            for panel in tab.panels
            for channel_name in self._get_channel_names_from_panel(panel)
        ]
        channels = set(channel_list)
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

    def _create_scatter_chart(
        self,
        plots: list[scout_chartdefinition_api.CartesianPlot],
        value_axes: list[scout_chartdefinition_api.ValueAxis],
        comparison_run_groups: list[Comparisons],
    ) -> scout_chartdefinition_api.VizDefinition:
        comparison_run_objects = [
            self._create_comparison_run_group(cmp.name, cmp.color, cmp.run_rids) for cmp in comparison_run_groups
        ]
        return scout_chartdefinition_api.VizDefinition(
            cartesian=scout_chartdefinition_api.CartesianChartDefinition(
                v1=scout_chartdefinition_api.CartesianChartDefinitionV1(
                    plots=plots,
                    value_axes=value_axes,
                    comparison_run_groups=comparison_run_objects,
                    title="Scatter plot chart",
                )
            )
        )

    def _create_histogram_chart(
        self,
        is_stacked: bool,
        plots: list[scout_chartdefinition_api.HistogramPlot],
        bucket_strategy: Union[None, WidthStrategy, CountStrategy],
    ) -> scout_chartdefinition_api.VizDefinition:
        return scout_chartdefinition_api.VizDefinition(
            histogram=scout_chartdefinition_api.HistogramChartDefinition(
                v1=scout_chartdefinition_api.HistogramChartDefinitionV1(
                    display_settings=scout_chartdefinition_api.HistogramDisplaySettings(
                        sort=scout_chartdefinition_api.HistogramSortOrder.VALUE_ASCENDING,
                        stacked=is_stacked,
                    ),
                    plots=plots,
                    numeric_bucket_strategy=(
                        None if bucket_strategy is None else self._create_histogram_bucket_strategy(bucket_strategy)
                    ),
                    title="Histogram chart",
                )
            )
        )

    def _create_geoplot(
        self, plot_name: str, lat_var_name: str, long_var_name: str, color: str
    ) -> scout_chartdefinition_api.GeoPlotFromLatLong:
        return scout_chartdefinition_api.GeoPlotFromLatLong(
            label=plot_name,
            latitude_variable_name=lat_var_name,
            longitude_variable_name=long_var_name,
            visualization_options=scout_chartdefinition_api.GeoPlotVisualizationOptions(
                color=color, line_style=scout_chartdefinition_api.GeoLineStyle(value="SOLID")
            ),
            enabled=True,
        )

    def _parse_timeseries_panel(self, panel: Timeseries_Panel) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for timeseries panels"""
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

                row_plots.append(self._create_timeseries_plot(var_name, axis_id, color))

            row_name = panel.row_names[row_index]
            panel_rows.append(self._create_timeseries_row(row_plots, row_name))
        return self._create_timeseries_chart(panel_rows, panel_axes, panel.comparison_runs)

    def _parse_cartesian_panel(self, panel: Cartesian_Panel) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for cartesian panels"""
        panel_plots: list[scout_chartdefinition_api.CartesianPlot] = []
        panel_axes: list[scout_chartdefinition_api.ValueAxis] = []

        # first, define x axis params (only 1 x axis for a plot)
        x_axis_channel, x_axis_name = panel.x_axis_data
        x_axis_var_name = self._channel_map[x_axis_channel]
        x_axis_id = str(uuid.uuid4())
        panel_axes.append(self._create_channel_axis(x_axis_id, (x_axis_name, "0")))

        axis_mappings: dict[TemplateAxis, str] = {}
        for channel_name, (color, axis_name) in panel.y_axis_data.items():
            # plot id
            y_axis_var_name = self._channel_map[channel_name]
            if axis_name in axis_mappings:
                y_axis_id = axis_mappings[axis_name]
            else:
                y_axis_id = str(uuid.uuid4())
                axis_mappings[axis_name] = y_axis_id
                panel_axes.append(self._create_channel_axis(y_axis_id, axis_name))

            panel_plots.append(
                self._create_cartesian_plot(
                    var_name_x=x_axis_var_name,
                    var_name_y=y_axis_var_name,
                    axis_id_x=x_axis_id,
                    axis_id_y=y_axis_id,
                    color=color,
                )
            )

        return self._create_scatter_chart(panel_plots, panel_axes, panel.comparison_runs)

    def _parse_histogram_panel(self, panel: Histogram_Panel) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for histogram panels"""
        panel_plots: list[scout_chartdefinition_api.HistogramPlot] = []
        for channel_name, color in panel.channels_w_colors:
            panel_plots.append(self._create_histogram_plot(color, self._channel_map[channel_name]))

        return self._create_histogram_chart(
            is_stacked=panel.stacked, plots=panel_plots, bucket_strategy=panel.bucket_strat
        )

    def _parse_geomap_panel(self, panel: Geomap_Panel) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for geomap panels"""
        panel_plots: list[scout_chartdefinition_api.GeoPlotFromLatLong] = []
        for plot_name, (lat, long, color) in panel.latlongs_w_color:
            panel_plots.append(self._create_geoplot(plot_name, self._channel_map[lat], self._channel_map[long], color))

        return scout_chartdefinition_api.VizDefinition(
            geo=scout_chartdefinition_api.GeoVizDefinition(
                v1=scout_chartdefinition_api.GeoVizDefinitionV1(
                    title="Geomap plot",
                    plots=panel_plots,
                    custom_features=[
                        scout_chartdefinition_api.GeoCustomFeature(
                            point=scout_chartdefinition_api.GeoPoint(
                                icon="circle-dot", latitude=lat, longitude=long, variables=[]
                            )
                        )
                        for lat, long in panel.geopoints
                    ],
                    base_tileset=scout_chartdefinition_api.GeoBaseTileset(
                        value=panel.plot_type if panel.plot_type in ("STREET", "SATELLITE") else "SATELLITE"
                    ),
                )
            )
        )

    def _create_tab(self, panel_rids: list[str], title: str) -> scout_layout_api.SingleTab:
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
            raise RuntimeError(f"{len(panel_rids)} panels provided. Max is 4")

        return scout_layout_api.SingleTab(v1=scout_layout_api.SingleTabV1(title=title, panel=panel))

    def _create_template_request(
        self,
        raw_template: Raw_Template,
    ) -> scout_template_api.CreateTemplateRequest:
        if raw_template.version != 0:
            raise RuntimeError("Sorry this function only supports template v0")

        """Creates template request object"""
        # populate global channel variables
        channel_variables = self._create_all_variables(raw_template)
        # Create separate charts for each tab
        charts: dict[str, scout_chartdefinition_api.VizDefinition] = {}
        tabs: list[scout_layout_api.SingleTab] = []

        for tab_index, tab in enumerate(raw_template.tabs):
            panel_charts = []
            for panel_index, panel in enumerate(tab.panels):
                # Create chart for this panel
                chart_rid = f"{CHART_RID_BASE}{uuid.uuid4()}"

                if panel.type == SupportedPanels.TIMESERIES and isinstance(panel, Timeseries_Panel):
                    charts[chart_rid] = self._parse_timeseries_panel(panel)
                elif panel.type == SupportedPanels.SCATTER and isinstance(panel, Cartesian_Panel):
                    charts[chart_rid] = self._parse_cartesian_panel(panel)
                elif panel.type == SupportedPanels.HISTOGRAM and isinstance(panel, Histogram_Panel):
                    charts[chart_rid] = self._parse_histogram_panel(panel)
                elif panel.type == SupportedPanels.GEOMAP and isinstance(panel, Geomap_Panel):
                    charts[chart_rid] = self._parse_geomap_panel(panel)
                else:
                    continue

                panel_charts.append(chart_rid)
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

    def create_template_from_yaml(self, yaml_input: Union[str, TextIO]) -> WorkbookTemplate:
        """Main user facing function for creating template.
        TODO: goal is to catch maximum errors to abstract away internals and have user focusing
        on template structure
        """
        try:
            template = self._parse_yaml_to_raw_template(yaml_input)
        except Exception:
            raise ValueError("Error parsing template! See our docs for structure.")

        try:
            request = self._create_template_request(template)
            conjure_template = self.client._clients.template.create(self.client._clients.auth_header, request)
            return WorkbookTemplate._from_conjure(self.client._clients, conjure_template)
        except Exception:
            raise ValueError("Could not create template. Please check values!")
