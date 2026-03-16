"""Helpers for building workbook content to push via WorkbookSession.

Simple usage — single data source::

    from nominal.experimental.multiplayer import (
        WorkbookSession,
        channel_variable, row, timeseries_panel, tab,
    )

    variables = {
        "speed":    channel_variable("speed",    channel_name="vehicle.speed",
                                    data_source_ref_name="my_logger",
                                    run_rid="ri.scout.main.run.xxx"),
        "altitude": channel_variable("altitude", channel_name="vehicle.altitude",
                                    data_source_ref_name="my_logger",
                                    run_rid="ri.scout.main.run.xxx"),
    }

    with WorkbookSession.create(workbook, access_token="<personal-access-token>") as session:
        session.set_workbook(
            variables,
            tabs=[
                tab("Flight Data", panels=[
                    timeseries_panel(rows=[
                        row(["speed"],    title="Speed"),
                        row(["altitude"], title="Altitude"),
                    ]),
                ]),
            ],
        )

Multi-source comparison with formula variables::

    from nominal.experimental.multiplayer import (
        WorkbookSession,
        channel_variable, formula_variable, row, timeseries_panel, tab,
        DEFAULT_COLORS,
    )

    vnames = variable_names()
    new_v, ref_v, err_v = next(vnames), next(vnames), next(vnames)

    variables = {
        new_v: channel_variable(new_v, channel_name="voltage",
                                data_source_ref_name="new_unit", run_rid="..."),
        ref_v: channel_variable(ref_v, channel_name="voltage",
                                data_source_ref_name="reference", run_rid="..."),
        err_v: formula_variable(err_v, expression=f"|{new_v}-{ref_v}|",
                                input_variable_names=[new_v, ref_v],
                                display_name="|Voltage Error|"),
    }

    with WorkbookSession.create(workbook, access_token="<personal-access-token>") as session:
        session.set_workbook(
            variables,
            tabs=[
                tab("Voltage", panels=[
                    timeseries_panel(rows=[
                        row([new_v, ref_v], title="Voltage",
                            colors=[DEFAULT_COLORS[0], DEFAULT_COLORS[1]]),
                    ]),
                    timeseries_panel(rows=[row([err_v], title="Error")]),
                ]),
            ],
        )

Manual layout with :meth:`~nominal.experimental.multiplayer.WorkbookSession.set_workbook_raw`
(for programmatic chart building where you control chart IDs explicitly)::

    from nominal.experimental.multiplayer import (
        WorkbookSession,
        channel_variable, timeseries_panel, row,
        new_id, chart_node, split_node, layout_tab,
    )

    speed_chart = new_id()
    error_chart = new_id()

    session.set_workbook_raw(
        variables,
        charts={
            speed_chart: timeseries_panel(rows=[row([new_v, ref_v], title="Speed")]),
            error_chart: timeseries_panel(rows=[row([err_v],         title="Error")]),
        },
        layout_tabs=[
            layout_tab("Speed",
                       split_node(chart_node(speed_chart), chart_node(error_chart))),
        ],
    )
"""

from __future__ import annotations

import string
import uuid
from typing import Any, Iterator

from pycrdt import Array, Map

# ── Public constants ──────────────────────────────────────────────────────────

DEFAULT_COLORS = [
    "#1c84ec",
    "#ec1c1c",
    "#10a1c6",
    "#c61046",
    "#5070f0",
    "#f08050",
    "#59bff1",
    "#f15973",
    "#da842e",
    "#20b663",
    "#a259f7",
    "#f7a259",
    "#59f7a2",
    "#f759d8",
    "#59d8f7",
    "#d8f759",
]
"""Default color palette cycling through perceptually distinct hues.

Used by :func:`row` when no explicit ``colors`` list is provided. Reference
individual colors by index when you need a consistent color for a specific
variable across panels or tabs::

    colors=[DEFAULT_COLORS[0], DEFAULT_COLORS[1]]
"""


# ── Public utilities ──────────────────────────────────────────────────────────


def new_id() -> str:
    """Generate a fresh UUID string for use as a chart, layout node, or variable ID.

    Use this when calling :meth:`WorkbookSession.set_workbook_raw` — you need a
    ``chart_rid`` to pass to both the ``charts`` dict and :func:`chart_node`.

    Example::

        chart_id = new_id()
        session.set_workbook_raw(
            variables,
            charts={chart_id: timeseries_panel(rows=[...])},
            layout_tabs=[layout_tab("Tab", chart_node(chart_id))],
        )
    """
    return str(uuid.uuid4())


def variable_names() -> Iterator[str]:
    """Yield an infinite sequence of short unique variable names: a, b, …, z, aa, ab, …

    Useful for programmatically assigning variable names when building
    workbooks over dynamic channel lists::

        vnames = variable_names()
        variables = {}
        for channel in selected_channels:
            name = next(vnames)
            variables[name] = channel_variable(name, channel_name=channel, ...)

    The generated names are intentionally short — they appear in formula
    expressions (e.g. ``|a-b|``) and the workbook's internal JSON.
    """
    letters = string.ascii_lowercase
    n = 1
    while True:
        indices = [0] * n
        while True:
            yield "".join(letters[i] for i in indices)
            pos = n - 1
            while pos >= 0:
                indices[pos] += 1
                if indices[pos] < len(letters):
                    break
                indices[pos] = 0
                pos -= 1
            else:
                break
        n += 1


def assign_colors(
    variable_names: list[str],
    palette: list[str] = DEFAULT_COLORS,
) -> dict[str, str]:
    """Assign a consistent hex color to each variable name.

    Returns a dict that can be passed as ``colors`` to :func:`row`. Because
    colors are keyed by name rather than position, a variable always gets the
    same color regardless of which row it appears in or how many variables
    share that row — making it easy to keep colors consistent across panels
    and tabs in a comparison workbook.

    Args:
        variable_names: Variables to assign colors to, in assignment order.
            Colors are assigned sequentially and cycle through ``palette`` if
            there are more names than colors.
        palette: Color list to cycle through. Defaults to :data:`DEFAULT_COLORS`.

    Returns:
        Dict of ``{variable_name: hex_color}``.

    Example::

        colors = assign_colors([new_v, ref_v, err_v])

        tab("Compare", panels=[
            timeseries_panel(rows=[
                # new_v → blue, ref_v → red — same colors in every row
                row([new_v, ref_v], title="Signal", colors=colors),
            ]),
            timeseries_panel(rows=[
                row([err_v], title="Error", colors=colors),
            ]),
        ])
    """
    return {name: palette[i % len(palette)] for i, name in enumerate(variable_names)}


# ── Public builder functions ──────────────────────────────────────────────────


def channel_variable(
    variable_name: str,
    *,
    channel_name: str,
    data_source_ref_name: str = "primary",
    asset_rid: str | None = None,
    run_rid: str | None = None,
    display_name: str | None = None,
) -> dict[str, Any]:
    """Create a numeric channel variable that references a data channel by name.

    Args:
        variable_name: Short identifier used to reference this variable in panel
            rows and formula expressions (e.g. ``"speed"``). Must be unique
            within the workbook. Use :func:`variable_names` to generate these
            programmatically.
        channel_name: The channel name in the data source (e.g. ``"vehicle.speed"``).
        data_source_ref_name: Data source reference name as configured in the
            workbook's data scope (e.g. ``"flax_logger"``). Defaults to ``"primary"``.
        asset_rid: RID of the asset to bind this channel to (e.g.
            ``"ri.scout.main.asset.xxx"``). Supply at least one of ``asset_rid``
            or ``run_rid`` for the channel to resolve data.
        run_rid: RID of the run to bind this channel to (e.g.
            ``"ri.scout.main.run.xxx"``).
        display_name: Optional human-readable label shown in the legend.
            Defaults to ``variable_name`` if omitted.

    Returns:
        A dict in the TypeScript ``ChannelVariable`` format ready for use in the
        ``variables`` dict passed to :meth:`WorkbookSession.set_workbook`.

    Example::

        variables = {
            "spd": channel_variable("spd", channel_name="vehicle.speed",
                                    data_source_ref_name="my_logger",
                                    run_rid="ri.scout.main.run.xxx",
                                    display_name="Speed"),
        }
    """
    channel_spec: dict[str, Any] = {
        "channelName": channel_name,
        "dataSourceRefName": data_source_ref_name,
        "tagsToGroupBy": [],
    }
    if asset_rid is not None:
        channel_spec["assetRidVariableName"] = asset_rid
    if run_rid is not None:
        channel_spec["runRidVariableName"] = run_rid

    result: dict[str, Any] = {
        "variableName": variable_name,
        "computeSpec": {
            "type": "fn",
            "fn": {
                "functionId": "numericSeries",
                "functionUuid": new_id(),
                "args": {
                    "input": {
                        "type": "numericSeriesType",
                        "provider": {
                            "type": "channel",
                            "channel": channel_spec,
                        },
                    },
                },
            },
        },
    }
    if display_name is not None:
        result["displayName"] = display_name
    return result


def formula_variable(
    variable_name: str,
    *,
    expression: str,
    input_variable_names: list[str],
    display_name: str | None = None,
) -> dict[str, Any]:
    """Create a derived numeric variable computed from an arithmetic expression.

    The expression is evaluated over other channel variables. Input variable
    names must match keys in the ``variables`` dict passed to
    :meth:`WorkbookSession.set_workbook`.

    Supported operators include ``+``, ``-``, ``*``, ``/``, ``|…|`` (absolute
    value), ``()``, and numeric literals. Variable names in the expression
    must match ``input_variable_names`` exactly.

    Args:
        variable_name: Short identifier for this derived variable. Must be
            unique within the workbook.
        expression: Arithmetic expression string, e.g. ``"|a-b|"`` or
            ``"(a+b)/2"``. References the names in ``input_variable_names``.
        input_variable_names: Variable names that the expression references,
            matching keys in the outer ``variables`` dict.
        display_name: Optional human-readable label shown in the legend.

    Returns:
        A dict in the TypeScript ``ChannelVariable`` format.

    Example — absolute error between two runs::

        variables = {
            "a": channel_variable("a", channel_name="speed", run_rid="...new..."),
            "b": channel_variable("b", channel_name="speed", run_rid="...ref..."),
            "e": formula_variable("e", expression="|a-b|",
                                  input_variable_names=["a", "b"],
                                  display_name="Speed Error"),
        }
    """
    result: dict[str, Any] = {
        "variableName": variable_name,
        "computeSpec": {
            "type": "fn",
            "fn": {
                "functionId": "numericSeriesFormula",
                "functionUuid": new_id(),
                "args": {
                    "formula": {
                        "type": "numericFormulaType",
                        "provider": {
                            "type": "plotFormula",
                            "plotFormula": {
                                "expression": expression,
                                "plotVariables": input_variable_names,
                            },
                        },
                    },
                },
            },
        },
    }
    if display_name is not None:
        result["displayName"] = display_name
    return result


def row(
    variable_names: list[str],
    *,
    title: str | None = None,
    colors: list[str] | dict[str, str] | None = None,
) -> dict[str, Any]:
    """Create a time series row containing one numeric plot per variable.

    Each row gets its own y-axis. Multiple variables in a row share that axis,
    so they should have compatible units and scale. To overlay signals from
    different data sources on the same axis, include all their variable names
    in one row. To keep them on independent axes, use separate rows.

    Args:
        variable_names: Ordered list of variable names (keys from the
            ``variables`` dict passed to :meth:`WorkbookSession.set_workbook`).
        title: Row title shown above the plots and used as the y-axis label.
        colors: Color assignment for plots. Three forms are accepted:

            - **Omitted** — cycles through :data:`DEFAULT_COLORS` by position.
            - **List** ``["#hex", …]`` — one color per variable, cycled by
              position. Use when all variables in this row are new.
            - **Dict** ``{variable_name: "#hex", …}`` — colors keyed by name,
              typically produced by :func:`assign_colors`. Variables missing
              from the dict fall back to :data:`DEFAULT_COLORS` by position.
              Use this form to keep colors consistent across multiple rows,
              panels, and tabs in a comparison workbook.

    Returns:
        A dict in the TypeScript ``TimeSeriesRow`` format. Internal
        ``_axis_id`` / ``_axis_title`` fields are consumed by
        :func:`timeseries_panel` and stripped before serialization.
    """
    axis_id = new_id()

    def _color(i: int, name: str) -> str:
        if isinstance(colors, dict):
            return colors.get(name, DEFAULT_COLORS[i % len(DEFAULT_COLORS)])
        pal = colors or DEFAULT_COLORS
        return pal[i % len(pal)]

    plots = [
        {
            "type": "numeric",
            "variableName": name,
            "enabled": True,
            "yAxisId": axis_id,
            "color": _color(i, name),
            "lineStyle": "SOLID",
        }
        for i, name in enumerate(variable_names)
    ]
    result: dict[str, Any] = {
        "uuid": new_id(),
        "enabled": True,
        "rowFlexSize": 1,
        "plots": plots,
        # Internal fields consumed by timeseries_panel; stripped before serialization.
        "_axis_id": axis_id,
        "_axis_title": title,
    }
    if title is not None:
        result["title"] = title
    return result


def timeseries_panel(
    rows: list[dict[str, Any]],
    *,
    title: str | None = None,
) -> dict[str, Any]:
    """Create a time series panel definition.

    Each row produced by :func:`row` gets its own independent y-axis.
    The panel uses ``"row-independent"`` coloring so plots in different rows
    cycle colors independently.

    Args:
        rows: List of row dicts produced by :func:`row`.
        title: Optional panel title shown in the chart header.

    Returns:
        A dict in the TypeScript ``TimeSeriesPanel`` format suitable for use
        as a value in the ``charts`` dict passed to
        :meth:`WorkbookSession.set_workbook_raw`, or as a panel in
        :func:`tab`.

    Example — two signals on independent axes::

        timeseries_panel(rows=[
            row(["speed"],    title="Speed (m/s)"),
            row(["altitude"], title="Altitude (m)"),
        ], title="Flight Overview")
    """
    value_axes = []
    clean_rows = []
    for r in rows:
        axis_id = r.get("_axis_id", new_id())
        axis_title = r.get("_axis_title") or ""
        value_axes.append(
            {
                "id": axis_id,
                "title": axis_title,
                "displayOptions": {"showTitle": bool(axis_title)},
                "range": {},
                "limit": {},
                "position": "LEFT",
                "domainType": "NUMERIC",
            }
        )
        clean_rows.append({k: v for k, v in r.items() if not k.startswith("_")})

    result: dict[str, Any] = {
        "type": "timeSeries",
        "comparisonRunGroups": [],
        "valueAxes": value_axes,
        "thresholds": [],
        "rows": clean_rows,
        "coloringMode": "row-independent",
        "stalenessConfiguration": {
            "shouldConnectGaps": True,
            "stalenessThreshold": {"precision": "seconds", "value": 1.0},
        },
    }
    if title is not None:
        result["title"] = title
    return result


def tab(title: str, panels: list[dict[str, Any]]) -> _Tab:
    """Create a workbook tab containing one or more panels stacked vertically.

    Panels are arranged top-to-bottom using a balanced binary split tree.
    Use :func:`timeseries_panel` to build each panel.

    For explicit control over the layout (e.g. asymmetric splits or side-by-side
    panels), use :func:`chart_node`, :func:`split_node`, :func:`layout_tab` and
    :meth:`WorkbookSession.set_workbook_raw` instead.

    Args:
        title: The tab title shown in the workbook tab bar.
        panels: Ordered list of panel dicts produced by :func:`timeseries_panel`.
            A single panel fills the tab. Two or more are split into equal halves.

    Returns:
        A :class:`_Tab` object consumed by :meth:`WorkbookSession.set_workbook`.

    Example::

        tab("Flight Data", panels=[
            timeseries_panel(rows=[row(["speed"])]),
            timeseries_panel(rows=[row(["altitude"])]),
        ])
    """
    return _Tab(title=title, panels=panels)


# ── Layout node builders (for set_workbook_raw) ───────────────────────────────


def chart_node(chart_rid: str, *, hide_legend: bool = False) -> dict[str, Any]:
    """Build a layout node that displays a panel from the ``charts`` dict.

    The ``chart_rid`` must be a key in the ``charts`` dict you pass to
    :meth:`WorkbookSession.set_workbook_raw`. Generate it with :func:`new_id`.

    Args:
        chart_rid: Key in the ``charts`` dict — the UUID identifying the panel
            content. Must be generated with :func:`new_id` *before* calling this
            function so you can use the same ID in both ``charts`` and the layout.
        hide_legend: Whether to hide the chart legend. Defaults to ``False``.

    Returns:
        A layout node dict for use in :func:`split_node` or :func:`layout_tab`.

    Example::

        cid = new_id()
        charts = {cid: timeseries_panel(rows=[...])}
        layout_tabs = [layout_tab("Tab", chart_node(cid))]
        session.set_workbook_raw(variables, charts=charts, layout_tabs=layout_tabs)
    """
    return {
        "type": "chart",
        "chart": {
            "id": new_id(),  # layout node ID — distinct from chart_rid
            "chartRid": {"rid": chart_rid, "version": 1},
            "hideLegend": hide_legend,
        },
    }


def split_node(
    top: dict[str, Any],
    bottom: dict[str, Any],
) -> dict[str, Any]:
    """Arrange two layout nodes top-and-bottom in a horizontal split.

    The divider is horizontal, so ``top`` appears above ``bottom``. Both
    halves receive equal space. Nest multiple :func:`split_node` calls to
    build deeper panel trees.

    Args:
        top: Layout node for the upper half — from :func:`chart_node` or
            another :func:`split_node`.
        bottom: Layout node for the lower half.

    Returns:
        A split layout node dict for use in :func:`layout_tab` or as an
        argument to another :func:`split_node`.

    Example — three panels stacked::

        layout_tab("Tab",
            split_node(chart_node(id1),
                       split_node(chart_node(id2), chart_node(id3))))
    """
    return {
        "type": "split",
        "split": {
            "id": new_id(),
            "orientation": "HORIZONTAL",
            "sideOne": top,
            "sideTwo": bottom,
        },
    }


def layout_tab(title: str, root_panel: dict[str, Any]) -> dict[str, Any]:
    """Build a WorkbookTab dict for use with :meth:`WorkbookSession.set_workbook_raw`.

    The tab ID is derived from the root panel's ID, matching the convention
    used by the Nominal frontend.

    Args:
        title: Tab title shown in the workbook tab bar.
        root_panel: Root layout node — either a :func:`chart_node` (single
            panel) or a :func:`split_node` tree (multiple panels).

    Returns:
        A TypeScript ``WorkbookTab`` dict for the ``layout_tabs`` list passed
        to :meth:`WorkbookSession.set_workbook_raw`.

    Example::

        top_id, bot_id = new_id(), new_id()
        session.set_workbook_raw(
            variables,
            charts={
                top_id: timeseries_panel(rows=[row(["a", "b"])]),
                bot_id: timeseries_panel(rows=[row(["err"])]),
            },
            layout_tabs=[
                layout_tab("Comparison",
                           split_node(chart_node(top_id), chart_node(bot_id))),
            ],
        )
    """
    return {
        "id": _get_panel_id(root_panel),
        "title": title,
        "layout": {"type": "grid", "rootPanel": root_panel},
    }


# ── Internal helpers ──────────────────────────────────────────────────────────


def _to_yjs(value: Any) -> Any:
    """Recursively convert Python lists to pycrdt Arrays.

    When replacing whole workbook subtrees we need concrete ``pycrdt`` container
    types at every level; relying on pycrdt's shallow dict auto-conversion drops
    nested arrays. This helper therefore materializes both maps and arrays.
    """
    if isinstance(value, list):
        return Array([_to_yjs(item) for item in value])
    elif isinstance(value, dict):
        return Map({k: _to_yjs(v) for k, v in value.items() if v is not None})
    return value


class _Tab:
    """Internal representation of a workbook tab with associated panel definitions.

    Produced by :func:`tab` and consumed by :meth:`WorkbookSession.set_workbook`.
    End users should not instantiate this class directly.
    """

    def __init__(self, title: str, panels: list[dict[str, Any]]) -> None:
        self.title = title
        self.panels = panels
        # chart_rids = keys used in content.charts (the "what chart is this")
        # layout_ids = IDs for the layout panel nodes (the "where is this panel")
        # These are separate because chart.id (layout node) ≠ chartRid.rid (content key).
        self._chart_rids = [new_id() for _ in panels]
        self._layout_ids = [new_id() for _ in panels]

    def to_layout_tab(self) -> dict[str, Any]:
        """Return the TypeScript ``WorkbookTab`` dict (layout side).

        Tab id = root panel id (matching the convention in real workbook state).
        """
        chart_nodes = [
            _chart_layout_node(lid, crid)
            for lid, crid in zip(self._layout_ids, self._chart_rids)
        ]
        root = _split_panels(chart_nodes)
        return {
            "id": _get_panel_id(root),
            "title": self.title,
            "layout": {"type": "grid", "rootPanel": root},
        }

    def to_charts(self) -> dict[str, dict[str, Any]]:
        """Return ``{chart_rid: panel_definition}`` entries for ``content.charts``."""
        return dict(zip(self._chart_rids, self.panels))


def _chart_layout_node(layout_id: str, chart_rid: str) -> dict[str, Any]:
    """Layout panel node referencing a chart in content.charts.

    Uses a pre-generated ``layout_id`` rather than calling ``new_id()`` so
    that ``_Tab`` can track the IDs it assigns.
    """
    return {
        "type": "chart",
        "chart": {
            "id": layout_id,
            "chartRid": {"rid": chart_rid, "version": 1},
            "hideLegend": False,
        },
    }


def _get_panel_id(panel: dict[str, Any]) -> str:
    """Extract the unique ID from a layout panel node."""
    if panel["type"] == "chart":
        panel_id = panel["chart"]["id"]
        if isinstance(panel_id, str):
            return panel_id
        raise ValueError(f"Chart panel id must be a string, received {panel_id!r}")
    if panel["type"] == "split":
        panel_id = panel["split"]["id"]
        if isinstance(panel_id, str):
            return panel_id
        raise ValueError(f"Split panel id must be a string, received {panel_id!r}")
    raise ValueError(f"Unknown panel type: {panel['type']!r}")


def _split_panels(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    """Recursively arrange layout nodes in a balanced binary vertical split tree."""
    if len(nodes) == 1:
        return nodes[0]
    mid = len(nodes) // 2
    split_id = new_id()
    return {
        "type": "split",
        "split": {
            "id": split_id,
            "orientation": "HORIZONTAL",
            "sideOne": _split_panels(nodes[:mid]),
            "sideTwo": _split_panels(nodes[mid:]),
        },
    }
