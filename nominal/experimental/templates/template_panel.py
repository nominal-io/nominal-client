import re
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from nominal_api import (
    api,
    scout_chartdefinition_api,
    scout_comparisonrun_api,
    scout_compute_api,
)

from nominal.experimental.templates.template_utils import Comparisons, TemplateAxis, TemplatePlot, TemplateRow


class Panel(ABC):
    """Base class for different panels we support
    Model is based on the assumption that all panel types will
    eventually support comparison runs
    """

    comparison_runs: list[Comparisons]

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

    def get_channel_names(self) -> list[str]:
        """Extract channel names from a panel based on its type"""
        if isinstance(self, TimeseriesPanel):
            return [channel_name for row in self.row_data for channel_name in row.keys()]
        elif isinstance(self, CartesianPanel):
            channels = []
            channels.append(self.x_axis_data[0])
            channels.extend(self.y_axis_data.keys())
            return channels
        elif isinstance(self, HistogramPanel):
            return [channel[0] for channel in self.channels_w_colors]
        elif isinstance(self, GeomapPanel):
            return [val for plot in self.latlongs_w_color for val in plot[1][0:1]]
        else:
            raise NotImplementedError(f"Channel extraction not implemented for panel type: {type(self)}")

    @staticmethod
    def _validate_hex_color(color: str) -> bool:
        """Validate hex color format"""
        if not isinstance(color, str):
            return False
        return bool(re.match(r"^#[0-9A-Fa-f]{6}$", color))

    @abstractmethod
    def to_viz_def(self, channel_map: dict[str, str]) -> scout_chartdefinition_api.VizDefinition:
        """Every panel has to have its own parse function which turns it from a Panel object to
        a scout api viz definition object
        """
        pass


class TimeseriesPanel(Panel):
    row_data: list[TemplateRow] = []
    row_names: list[str] = []

    def __init__(self, panel_data: dict[str, Any], comparison_runs: list[Comparisons] = []):
        """Initializes timeseries panel object from YAML config"""
        try:
            assert "rows" in panel_data and isinstance(panel_data["rows"], dict)
        except Exception:
            raise ValueError("Bad TIMESERIES row structure! See docs.")
        # Parse rows
        for row_name, row_channels in panel_data["rows"].items():
            self.row_names.append(row_name)
            template_row = {}

            try:
                # Parse channels within the row
                for channel_name, channel_info in row_channels.items():
                    color = channel_info[0]
                    axis_name = channel_info[1]
                    axis_side = channel_info[2]

                    if axis_side not in [0, 1]:
                        raise ValueError(f"SCATTER panel y_axis channel '{channel_name}' axis side must be 0 or 1")

                    template_axis = (axis_name, axis_side)
                    # TODO: question - do we universally replace / with . ?
                    template_row[channel_name.replace("/", ".")] = (color, template_axis)

                self.row_data.append(template_row)
            except Exception:
                raise ValueError(f"Bad structure of timeseries channel for row: {row_name}")
        self.comparison_runs = comparison_runs

    """Helper functions for turning to scout api object"""

    def _create_timeseries_plot(
        self, var_name: str, axis_id: str, color: str
    ) -> scout_chartdefinition_api.TimeSeriesPlotV2:
        if self._validate_hex_color(color):
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
        else:
            raise ValueError(f"TIMESERIES panel has invalid color '{color}'. Must be hex format like '#FF0000'")

    def _create_timeseries_row(
        self, plots: list[scout_chartdefinition_api.TimeSeriesPlotV2], row_name: str
    ) -> scout_chartdefinition_api.TimeSeriesRow:
        return scout_chartdefinition_api.TimeSeriesRow(
            plots=[], plots_v2=plots, row_flex_size=1, title=row_name, enabled=True
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

    def to_viz_def(self, channel_map: dict[str, str]) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for timeseries panels"""
        panel_rows = []
        panel_axes = []

        for row_index, row in enumerate(self.row_data):
            row_plots = []

            axis_mappings: dict[TemplateAxis, str] = {}

            for channel_name, (color, axis_name) in row.items():
                # plot identifier
                var_name = channel_map[channel_name]
                # make plot with appropriate color
                if axis_name in axis_mappings:
                    axis_id = axis_mappings[axis_name]
                else:
                    axis_id = str(uuid.uuid4())
                    axis_mappings[axis_name] = axis_id
                    panel_axes.append(self._create_channel_axis(axis_id, axis_name))

                row_plots.append(self._create_timeseries_plot(var_name, axis_id, color))

            row_name = self.row_names[row_index]
            panel_rows.append(self._create_timeseries_row(row_plots, row_name))
        return self._create_timeseries_chart(panel_rows, panel_axes, self.comparison_runs)


class CartesianPanel(Panel):
    x_axis_data: tuple[str, str]  # <channel_name, axis_name>
    y_axis_data: TemplatePlot = {}

    def __init__(self, panel_data: dict[str, Any], comparison_runs: list[Comparisons] = []):
        """Initializes cartesian panel object from YAML config"""
        try:
            assert "plots" in panel_data and isinstance(panel_data["plots"], dict)
        except Exception:
            raise ValueError("Bad SCATTER plot structure! See docs.")
        plots_data = panel_data["plots"]

        # Parse x_axis data
        try:
            x_axis_info = plots_data["x_axis"]
            x_channel_name = x_axis_info[0]
            x_axis_title = x_axis_info[1]
            self.x_axis_data = (x_channel_name.replace("/", "."), x_axis_title)
        except Exception:
            raise ValueError("Invalid x axis data for scatter panel! See docs.")

        # Parse y_axis data
        try:
            y_axis_info = plots_data["y_axis"]

            for channel_name, channel_data in y_axis_info.items():
                color = channel_data[0]
                axis_title = channel_data[1]
                axis_side = channel_data[2]

                if axis_side not in [0, 1]:
                    raise ValueError(f"SCATTER panel y_axis channel '{channel_name}' axis side must be 0 or 1")

                template_axis = (axis_title, axis_side)
                self.y_axis_data[channel_name.replace("/", ".")] = (color, template_axis)
        except Exception:
            raise ValueError("Invalid y axis data for scatter panel! See docs.")

        self.comparison_runs = comparison_runs

    """Helper functions for turning to scout api object"""

    def _create_cartesian_plot(
        self, var_name_x: str, var_name_y: str, axis_id_x: str, axis_id_y: str, color: str
    ) -> scout_chartdefinition_api.CartesianPlot:
        if self._validate_hex_color(color):
            return scout_chartdefinition_api.CartesianPlot(
                color=color,
                x_axis_id=axis_id_x,
                x_variable_name=var_name_x,
                y_axis_id=axis_id_y,
                y_variable_name=var_name_y,
                enabled=True,
            )
        else:
            raise ValueError(
                f"SCATTER panel y_axis channel has invalid color '{color}'.\
                                      Must be hex format like '#FF0000'"
            )

    def _create_cartesian_chart(
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

    def to_viz_def(self, channel_map: dict[str, str]) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for cartesian panels"""
        panel_plots: list[scout_chartdefinition_api.CartesianPlot] = []
        panel_axes: list[scout_chartdefinition_api.ValueAxis] = []

        # first, define x axis params (only 1 x axis for a plot)
        x_axis_channel, x_axis_name = self.x_axis_data
        x_axis_var_name = channel_map[x_axis_channel]
        x_axis_id = str(uuid.uuid4())
        panel_axes.append(self._create_channel_axis(x_axis_id, (x_axis_name, "0")))

        axis_mappings: dict[TemplateAxis, str] = {}
        for channel_name, (color, axis_name) in self.y_axis_data.items():
            # plot id
            y_axis_var_name = channel_map[channel_name]
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

        return self._create_cartesian_chart(panel_plots, panel_axes, self.comparison_runs)


class HistogramPanel(Panel):
    """Helper classes for Histogram template"""

    @dataclass
    class CountStrategy:
        """Define a COUNT stategy"""

        num_buckets: int

    @dataclass
    class WidthStrategy:
        """Define a WIDTH strategy"""

        bucket_width: float
        offset: float

    channels_w_colors: list[tuple[str, str]]  # <channel_name, color>
    stacked: bool = False
    bucket_strat: Union[WidthStrategy, CountStrategy, None] = None

    def __init__(self, panel_data: dict[str, Any], comparison_runs: list[Comparisons] = []):
        """Initializes histogram panel object from YAML config"""
        try:
            self.channels_w_colors = [(pair[0], pair[1]) for pair in panel_data["channels"]]
        except Exception:
            raise ValueError("Bad channel structure for histogram panel! See docs.")

        self.bucket_strat = self._get_bucket_strat(panel_data.get("bucket_strat"))
        self.stacked = True if panel_data.get("stacked") == "true" else False
        self.comparison_runs = comparison_runs

    def _get_bucket_strat(self, data: Union[None, dict[str, Any]]) -> Union[None, WidthStrategy, CountStrategy]:
        if not data:
            return None
        try:
            strat_type = data["type"]
        except Exception:
            raise ValueError("Histogram bucket strategy could not be determined. Ensure 'type' field is present")

        try:
            if strat_type == "COUNT":
                num_buckets = int(data["num_buckets"])
                assert num_buckets > 0
                return self.CountStrategy(num_buckets)
            elif strat_type == "WIDTH":
                bucket_width = float(data["bucket_width"])
                offset = float(data.get("offset", 0.0))
                assert bucket_width > 0
                return self.WidthStrategy(bucket_width, offset)
            else:
                raise ValueError(f"Bucket strategy: {strat_type} NOT SUPPORTED")
        except Exception:
            return None  # default bucket strat if not formated properly

    """Helper functions for turning to scout api object"""

    def _create_histogram_plot(self, color: str, var_name: str) -> scout_chartdefinition_api.HistogramPlot:
        if self._validate_hex_color(color):
            return scout_chartdefinition_api.HistogramPlot(color=color, variable_name=var_name, enabled=True)
        else:
            raise ValueError(
                f"HISTOGRAM panel channel has invalid color '{color}'.\
                                      Must be hex format like '#FF0000'"
            )

    def _create_histogram_bucket_strategy(
        self, bucket_strat: Union[WidthStrategy, CountStrategy]
    ) -> scout_compute_api.NumericHistogramBucketStrategy:
        if isinstance(bucket_strat, self.WidthStrategy):
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

    def to_viz_def(self, channel_map: dict[str, str]) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for histogram panels"""
        panel_plots: list[scout_chartdefinition_api.HistogramPlot] = []
        for channel_name, color in self.channels_w_colors:
            panel_plots.append(self._create_histogram_plot(color, channel_map[channel_name]))

        return self._create_histogram_chart(
            is_stacked=self.stacked, plots=panel_plots, bucket_strategy=self.bucket_strat
        )


class GeomapPanel(Panel):
    latlongs_w_color: list[tuple[str, tuple[str, str, str]]] = []  # <plot_name, (lat_channel, long_channel, color)>
    plot_type: str  # 'STREET' or 'SATELLITE'
    geopoints: list[tuple[float, float]] = []  # <lat_val, long_val>

    def __init__(self, panel_data: dict[str, Any], comparison_runs: list[Comparisons] = []):
        """Initializes geomap panel object from YAML config"""
        self.plot_type = panel_data.get("tile_type", "STREET")

        if "geopoints" in panel_data:
            for point in panel_data["geopoints"]:
                lat_val, long_val = point[0], point[1]
                self.geopoints.append((lat_val, long_val))

        try:
            assert "plots" in panel_data and isinstance(panel_data["plots"], dict)
        except Exception:
            raise ValueError("Bad GEOMAP plot structure! See docs.")

        plots_data = panel_data["plots"]

        try:
            for plot_name, plot_data in plots_data.items():
                lat_channel = plot_data[0]
                long_channel = plot_data[1]
                color = plot_data[2]

                self.latlongs_w_color.append((plot_name, (lat_channel, long_channel, color)))
        except Exception:
            raise ValueError("Bad channel structure for geomap panel! See docs")

        self.comparison_runs = comparison_runs

    """Helper function for turning to scout api object"""

    def _create_geoplot(
        self, plot_name: str, lat_var_name: str, long_var_name: str, color: str
    ) -> scout_chartdefinition_api.GeoPlotFromLatLong:
        if self._validate_hex_color(color):
            return scout_chartdefinition_api.GeoPlotFromLatLong(
                label=plot_name,
                latitude_variable_name=lat_var_name,
                longitude_variable_name=long_var_name,
                visualization_options=scout_chartdefinition_api.GeoPlotVisualizationOptions(
                    color=color, line_style=scout_chartdefinition_api.GeoLineStyle(value="SOLID")
                ),
                enabled=True,
            )
        else:
            raise ValueError(
                f"GEOMAP panel plot '{plot_name}' has invalid color '{color}'.\
                              Must be hex format like '#FF0000'"
            )

    def to_viz_def(self, channel_map: dict[str, str]) -> scout_chartdefinition_api.VizDefinition:
        """Parsing function for geomap panels"""
        panel_plots: list[scout_chartdefinition_api.GeoPlotFromLatLong] = []
        for plot_name, (lat, long, color) in self.latlongs_w_color:
            panel_plots.append(self._create_geoplot(plot_name, channel_map[lat], channel_map[long], color))

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
                        for lat, long in self.geopoints
                    ],
                    base_tileset=scout_chartdefinition_api.GeoBaseTileset(
                        value=self.plot_type if self.plot_type in ("STREET", "SATELLITE") else "SATELLITE"
                    ),
                )
            )
        )
