"""Rich notebook display for SDK resources.

Jupyter, VS Code and Colab call `_repr_html_` to display an object at the end of a cell. The resources that
implement it decorate it with `card` and build their HTML from the helpers here, and `set_display_options`
configures how it looks.

A card never fails and never waits long: the sections it loads from Nominal run in parallel under one time limit,
`fetch_timeout`, and a section that misses it says so. If building the card fails anyway, the notebook shows the
plain ``repr`` instead.
"""

from __future__ import annotations

import functools
import html
import logging
import re
import secrets
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime, timezone, tzinfo
from importlib import resources
from itertools import islice
from typing import Any, Literal, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC

DisplayStyle = Literal["html", "text"]
TimeFormat = Literal["iso", "readable"]
TimePrecision = Literal["s", "ms", "us", "ns"]
_FRACTION_DIGITS: dict[str, int] = {"s": 0, "ms": 3, "us": 6, "ns": 9}
T = TypeVar("T")
logger = logging.getLogger(__name__)


def _resolve_timezone(name: str) -> tzinfo | None:
    """Return the tzinfo for an option value, where `None` means the kernel's local time zone."""
    if name == "local":
        return None
    if name == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"unknown time zone {name!r}; use 'UTC', 'local' or an IANA name") from e


@dataclass(frozen=True)
class DisplayOptions:
    """How Nominal resources display in notebooks.

    Change these with `set_display_options`, and read the current values with `get_display_options`.
    """

    style: DisplayStyle = "html"
    """``"html"`` shows resources as formatted cards. ``"text"`` shows their plain ``repr``."""
    timezone: str = "UTC"
    """Time zone for timestamps: ``"UTC"``, ``"local"`` for the notebook kernel's time zone, or an IANA name such as
    ``"Europe/Berlin"``."""
    time_format: TimeFormat = "iso"
    """``"iso"`` shows timestamps as ISO 8601, such as ``2026-11-02T16:33:03Z``. ``"readable"`` uses a space and
    the time zone's name, such as ``2026-11-02 16:33:03 UTC``."""
    time_precision: TimePrecision = "s"
    """The finest unit timestamps show: ``"s"``, ``"ms"``, ``"us"`` or ``"ns"``. Finer digits are cut off, not
    rounded, and trailing zeros are left out, so ``"ns"`` shows each timestamp exactly."""
    show_link: bool = True
    """Whether cards link resources to their pages in the Nominal app.

    Building a link resolves the client's workspace. The first card for a client that hasn't resolved it yet makes
    one request; later cards reuse the result.
    """
    fetch_details: bool = True
    """Whether cards load related resources from Nominal, such as an asset's datasets and runs, a dataset's
    channels and a numeric channel's statistics.

    Each card then makes a few requests. With this off, cards show only the fields the object already holds.
    """
    collapse_threshold: int = 0
    """Sections with more items than this start collapsed, and a diagram with more nodes than this does too. With
    the default, 0, every section with items starts collapsed, so a card shows its fields and the titles of its
    sections. A section that holds only a note, such as a load that failed, stays open so the note shows."""
    max_items: int = 50
    """The most items a section lists, and loads where it can. The section says how many more were left out."""
    fetch_timeout: float = 5.0
    """Seconds a card waits for what it loads from Nominal, in total. Sections still loading then say so, and
    links that need the workspace show as plain text."""
    show_graph: bool = True
    """Whether cards draw a diagram of the resource and the resources around it, such as an asset's datasets and
    runs. Each node links to its page in the Nominal app.

    The diagram uses only what the card already loads, so it makes no requests of its own. Cards draw none with
    ``fetch_details=False``.
    """
    graph_max_nodes: int = 20
    """The most nodes a diagram draws. Past it, each group of related resources shows its first few and a node
    counting the rest, which links to the card's section listing them. The card's own node and one node of each
    group always show, so a cap below one more than the number of groups is exceeded."""

    def __post_init__(self) -> None:
        if self.style not in ("html", "text"):
            raise ValueError(f"style must be 'html' or 'text', not {self.style!r}")
        if self.time_format not in ("iso", "readable"):
            raise ValueError(f"time_format must be 'iso' or 'readable', not {self.time_format!r}")
        if self.time_precision not in _FRACTION_DIGITS:
            raise ValueError(f"time_precision must be 's', 'ms', 'us' or 'ns', not {self.time_precision!r}")
        if self.collapse_threshold < 0:
            raise ValueError(f"collapse_threshold must be at least 0, not {self.collapse_threshold}")
        if self.max_items < 1:
            raise ValueError(f"max_items must be at least 1, not {self.max_items}")
        if self.fetch_timeout <= 0:
            raise ValueError(f"fetch_timeout must be more than 0, not {self.fetch_timeout}")
        if self.graph_max_nodes < 1:
            raise ValueError(f"graph_max_nodes must be at least 1, not {self.graph_max_nodes}")
        _resolve_timezone(self.timezone)


_options = DisplayOptions()


def get_display_options() -> DisplayOptions:
    """Return the current notebook display options."""
    return _options


def set_display_options(
    *,
    style: DisplayStyle | None = None,
    timezone: str | None = None,
    time_format: TimeFormat | None = None,
    time_precision: TimePrecision | None = None,
    show_link: bool | None = None,
    fetch_details: bool | None = None,
    collapse_threshold: int | None = None,
    max_items: int | None = None,
    fetch_timeout: float | None = None,
    show_graph: bool | None = None,
    graph_max_nodes: int | None = None,
) -> AbstractContextManager[DisplayOptions]:
    """Change how Nominal resources display in notebooks.

    Options left out keep their current values. Called on its own, the change lasts for the rest of the Python
    process. Used in a ``with`` block, it lasts until the block ends.

    Args:
        style: ``"html"`` shows resources as formatted cards. ``"text"`` shows their plain ``repr``.
        timezone: Time zone for timestamps: ``"UTC"``, ``"local"``, or an IANA name such as ``"Europe/Berlin"``.
        time_format: ``"iso"`` for ISO 8601, or ``"readable"`` for a space and the time zone's name.
        time_precision: The finest unit timestamps show: ``"s"``, ``"ms"``, ``"us"`` or ``"ns"``.
        show_link: Whether cards link resources to their pages in the Nominal app.
        fetch_details: Whether cards load related resources and channel statistics from Nominal.
        collapse_threshold: Sections with more items than this start collapsed.
        max_items: The most items a section lists.
        fetch_timeout: Seconds a card waits for what it loads from Nominal, in total.
        show_graph: Whether cards draw a diagram of the resource and the resources around it.
        graph_max_nodes: The most nodes a diagram draws. The card's own node and one node of each group of
            related resources always show, so a cap below one more than the number of groups is exceeded.

    Returns:
        A context manager that restores the previous options when its ``with`` block ends.

    Raises:
        ValueError: If an option has an invalid value, such as an unknown time zone.

    Example:
        ```python
        from nominal.core import set_display_options

        set_display_options(timezone="America/Los_Angeles", time_precision="ms")

        with set_display_options(fetch_details=False):
            run  # shows only the run's own fields
        ```
    """
    global _options
    changes: dict[str, Any] = dict(
        style=style,
        timezone=timezone,
        time_format=time_format,
        time_precision=time_precision,
        show_link=show_link,
        fetch_details=fetch_details,
        collapse_threshold=collapse_threshold,
        max_items=max_items,
        fetch_timeout=fetch_timeout,
        show_graph=show_graph,
        graph_max_nodes=graph_max_nodes,
    )
    previous = _options
    # Applied now, not on entering a `with` block, so a plain call changes the options too.
    _options = replace(_options, **{name: value for name, value in changes.items() if value is not None})
    return _restoring(previous)


@contextmanager
def _restoring(previous: DisplayOptions) -> Iterator[DisplayOptions]:
    global _options
    try:
        yield _options
    finally:
        _options = previous


# When the current card's loads must finish, by `time.monotonic()`. Unset outside a card and in loading threads.
_deadline: ContextVar[float | None] = ContextVar("nominal_display_deadline", default=None)
_executor: ThreadPoolExecutor | None = None


def _pool() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="nominal-display")
    return _executor


def card(build: Callable[[T], str | None]) -> Callable[[T], str | None]:
    """Decorate a resource's `_repr_html_`, so its card loads within `fetch_timeout` and never fails to display.

    The loads the card makes share one deadline. If building the card raises, it returns `None`, and the notebook
    shows the plain ``repr`` instead of a traceback.
    """

    @functools.wraps(build)
    def wrapper(resource: T) -> str | None:
        if _options.style == "text":
            return None
        token = _deadline.set(time.monotonic() + _options.fetch_timeout)
        try:
            return build(resource)
        except Exception:  # noqa: BLE001 - a display falls back to the plain repr rather than fail
            logger.debug("couldn't build the notebook card for %s", type(resource).__name__, exc_info=True)
            return None
        finally:
            _deadline.reset(token)

    return wrapper


def wait(future: Future[T]) -> T:
    """The future's result, waiting until the card's deadline, or for `fetch_timeout` outside a card."""
    deadline = _deadline.get()
    remaining = _options.fetch_timeout if deadline is None else deadline - time.monotonic()
    return future.result(timeout=max(remaining, 0))


def _within_deadline(load: Callable[[], T]) -> T:
    """Run `load` in a loading thread, so the card stops waiting for it at its deadline.

    Outside a card, as inside a loading thread, `load` runs directly: the card already waits for that thread.

    Raises:
        concurrent.futures.TimeoutError: If `load` hasn't finished by the deadline. It keeps running in the
            background, and its result is dropped.
    """
    if _deadline.get() is None:
        return load()
    return wait(_pool().submit(load))


@dataclass(frozen=True)
class Section:
    """A collapsible part of a card, such as its properties or a dataset's channels."""

    title: str
    rows: Sequence[Sequence[str]] = ()
    """Cell HTML for each row, already escaped."""
    columns: Sequence[str] = ()
    """Column headings. Without them, the first cell of each row is a row heading."""
    has_more: bool = False
    """Whether more rows exist than were loaded."""
    body: str = ""
    """HTML shown before the rows, such as a chart."""
    note: str = ""
    """Text shown after the rows, such as why there are none."""
    counted: bool = True
    """Whether the title shows how many rows there are."""
    collapsed: bool = False
    """Whether the section starts collapsed however few rows it has."""


def escape(value: object) -> str:
    return html.escape(str(value))


def code(value: object) -> str:
    return f"<code>{escape(value)}</code>"


def timestamp(value: IntegralNanosecondsUTC) -> str:
    """Format a timestamp in the configured time zone, format and precision."""
    seconds, nanos = divmod(value, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, timezone.utc).astimezone(_resolve_timezone(_options.timezone))
    fraction = f"{nanos:09d}"[: _FRACTION_DIGITS[_options.time_precision]]
    # Leave out trailing zeros in groups of three, so whole milliseconds don't show as nanoseconds.
    while fraction.endswith("000"):
        fraction = fraction[:-3]
    clock = moment.strftime("%H:%M:%S") + (f".{fraction}" if fraction else "")
    if _options.time_format == "readable":
        text = f"{moment:%Y-%m-%d} {clock} {moment.tzname()}"
    else:
        offset = moment.strftime("%z")
        zone = "Z" if offset == "+0000" else f"{offset[:3]}:{offset[3:]}"
        text = f"{moment:%Y-%m-%d}T{clock}{zone}"
    # The full value stays available on hover and to scripts reading the page.
    full = f"{datetime.fromtimestamp(seconds, timezone.utc):%Y-%m-%dT%H:%M:%S}.{nanos:09d}Z"
    return f'<time datetime="{full}" title="{full}">{escape(text)}</time>'


def duration(value: IntegralNanosecondsDuration) -> str:
    """Format a duration for reading, such as ``1 h 2 min 3.5 s``."""
    sign = "-" if value < 0 else ""
    minutes, nanos = divmod(abs(value), 60_000_000_000)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = [f"{amount} {unit}" for amount, unit in ((days, "d"), (hours, "h"), (minutes, "min")) if amount]
    if nanos or not parts:
        # Integer arithmetic, so a fraction such as 59.999999999 s doesn't round up to a whole minute.
        seconds, fraction = divmod(nanos, 1_000_000_000)
        parts.append(f"{seconds}{f'.{fraction:09d}'.rstrip('0') if fraction else ''} s")
    return escape(sign + " ".join(parts))


def number(value: float, unit: str | None = None) -> str:
    return escape(f"{value:.6g}" + (f" {unit}" if unit else ""))


def link(url: str, text: str) -> str:
    """An external link, or plain text when the URL isn't http(s), so a stored value can't run script."""
    if not _is_web_url(url):
        return escape(text)
    return f'<a href="{escape(url)}" target="_blank" rel="noopener noreferrer">{escape(text)}</a>'


def resource_link(text: str, url: Callable[[], str]) -> str:
    """Link text to a resource's page in the Nominal app, when links are on and the workspace resolves."""
    resolved = _resolved_url(url)
    return escape(text) if resolved is None else link(resolved, text)


def _is_web_url(url: str) -> bool:
    return url.startswith(("https://", "http://"))


def _resolved_url(url: Callable[[], str]) -> str | None:
    """A resource's page in the Nominal app, or `None` when links are off or the page can't be linked.

    Resolving the workspace can make a request, so the text style, which builds no card, never resolves one, and
    a card stops waiting for it at its deadline.
    """
    if _options.style == "text" or not _options.show_link:
        return None
    try:
        resolved = _within_deadline(url)
    except Exception:  # noqa: BLE001 - an unresolved link, including one past the deadline, shows as plain text
        return None
    return resolved if _is_web_url(resolved) else None


def copy_button(value: str) -> str:
    """A button that copies `value`. Notebooks that strip scripts leave it inert; the value stays selectable."""
    return (
        f'<button type="button" class="nominal-repr-copy" title="Copy" aria-label="Copy {escape(value)}" '
        f'data-copy="{escape(value)}" '
        "onclick=\"navigator.clipboard.writeText(this.dataset.copy).then(() => { this.textContent = '✓'; })\""
        ">⧉</button>"
    )


def identifier(value: str, url: Callable[[], str] | None = None) -> str:
    """An ID such as a RID: selectable in one click, with a copy button, and linked when it has a page."""
    text = escape(value) if url is None else resource_link(value, url)
    return f'<span class="nominal-repr-id-group"><code class="nominal-repr-id">{text}</code>{copy_button(value)}</span>'


def named(name: str, rid: str, url: Callable[[], str] | None = None) -> str:
    """A related resource's name, linked to its page, with a button that copies its RID."""
    text = escape(name) if url is None else resource_link(name, url)
    return f'<span class="nominal-repr-id-group">{text}{copy_button(rid)}</span>'


def labels(values: Sequence[str]) -> str:
    shown = [f'<span class="nominal-repr-label">{escape(value)}</span>' for value in values[: _options.max_items]]
    if len(values) > _options.max_items:
        shown.append(f'<span class="nominal-repr-more">… and {len(values) - _options.max_items} more</span>')
    return " ".join(shown)


def properties_section(properties: Mapping[str, object], title: str = "Properties") -> Section:
    return Section(title, [[escape(key), escape(value)] for key, value in sorted(properties.items())])


def first(items: Iterable[T]) -> tuple[list[T], bool]:
    """Take up to `max_items` items, and whether there were more, without loading the rest."""
    taken = list(islice(items, _options.max_items + 1))
    return taken[: _options.max_items], len(taken) > _options.max_items


@dataclass(frozen=True)
class Loading:
    """A section loading from Nominal in the background. `render` waits for it until the card's deadline."""

    title: str
    build: Callable[[], Section]
    """Waits for what the section loads and builds it."""

    def result(self) -> Section:
        """The loaded section, or one whose note says why it didn't load."""
        try:
            return self.build()
        except FutureTimeoutError:
            return Section(self.title, note=f"Didn't load within {_options.fetch_timeout:g} s.")
        except Exception as e:  # noqa: BLE001 - a display shows what it can; the error is reported in the card
            message = str(e).splitlines()[0] if str(e) else ""
            return Section(self.title, note=f"Couldn't load: {type(e).__name__} {message}"[:300])


def start(load: Callable[[], T]) -> Future[T] | None:
    """Start loading from Nominal in the background, or return `None` when loading is off or the text style builds
    no card.

    Start a load once when both a section and the card's diagram use it, and `wait` for it in each.
    """
    if _options.style == "text" or not _options.fetch_details:
        return None
    return _pool().submit(load)


def fetched(title: str, load: Callable[[], Section]) -> Loading | None:
    """Start loading a section from Nominal, or return `None` when loading is off or the text style builds no card.

    Sections load in parallel. A failure or a load past the card's deadline becomes a note in the section, so the
    rest of the card still displays.
    """
    future = start(load)
    return None if future is None else Loading(title, lambda: wait(future))


def fetched_from(title: str, future: Future[T] | None, build: Callable[[T], Section]) -> Loading | None:
    """A section built from a load already started with `start`, or `None` when it wasn't started."""
    return None if future is None else Loading(title, lambda: build(wait(future)))


def sparkline(points: Sequence[tuple[float, float, float] | None]) -> str:
    """A small chart of bucketed data: each point is (min, max, mean), or `None` for a bucket without data."""
    present = [point for point in points if point is not None]
    if not present:
        return ""
    width, height, pad = 360, 64, 2
    low = min(point[0] for point in present)
    high = max(point[1] for point in present)
    span = (high - low) or 1.0

    def x(index: int) -> float:
        return pad + index * (width - 2 * pad) / max(len(points) - 1, 1)

    def y(value: float) -> float:
        return pad + (high - value) * (height - 2 * pad) / span

    indexed = [(index, point) for index, point in enumerate(points) if point is not None]
    band = [f"{x(i):.1f},{y(p[1]):.1f}" for i, p in indexed] + [
        f"{x(i):.1f},{y(p[0]):.1f}" for i, p in reversed(indexed)
    ]
    mean = [f"{x(i):.1f},{y(p[2]):.1f}" for i, p in indexed]
    return (
        f'<svg class="nominal-repr-chart" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'role="img" aria-label="Channel values over time: range and mean">'
        f'<polygon points="{" ".join(band)}" fill="currentColor" fill-opacity="0.15" stroke="none"/>'
        f'<polyline points="{" ".join(mean)}" fill="none" stroke="currentColor" stroke-width="1.5"/>'
        "</svg>"
    )


@dataclass(frozen=True)
class Node:
    """A resource in a card's diagram."""

    kind: str
    """The resource type, such as ``"Asset"``, which sets the node's shape and colors."""
    name: str
    rid: str
    url: Callable[[], str] | None = None
    """Returns the resource's page in the Nominal app."""
    edge: str = ""
    """The label on the edge joining it to its group's node, such as a data scope's ref name."""


@dataclass(frozen=True)
class Group:
    """Related resources of one kind, each joined to the same node, such as an asset's runs."""

    section: str
    """The title of the card's section that lists all of them. A node counting the ones not drawn links to it."""
    nodes: Sequence[Node]
    to: Node
    """The node each of them joins: the card's own node, or a node of an earlier group."""
    below: bool = True
    """Whether they sit below `to` and point up to it, as datasets feed an asset. Otherwise `to` points up to them."""
    has_more: bool = False
    """Whether more exist than were loaded."""
    dashed: bool = False
    """Whether their edges are dashed, as for a workbook that points at one run at a time."""


@dataclass(frozen=True)
class Graph:
    """A card's resource and the resources around it, drawn from the bottom up as data flows."""

    node: Node
    groups: Sequence[Group]


# Fill, stroke and shape for each kind of node, matching the diagrams in the Nominal docs. The fills are light, with
# dark text, so the nodes read the same on light and dark themes.
_NODE_STYLES: dict[str, tuple[str, str, str]] = {
    "Asset": ("#dcfce7", "#15803d", "rect"),
    "Run": ("#fee2e2", "#b91c1c", "rect"),
    "Dataset": ("#e0e7ff", "#4338ca", "cylinder"),
    "Workbook": ("#e0f2fe", "#0369a1", "rect"),
    "Event": ("#fef3c7", "#b45309", "rounded"),
    "Checklist": ("#ccfbf1", "#0f766e", "rounded"),
    "Data review": ("#fae8ff", "#a21caf", "rounded"),
    "File": ("#f1f5f9", "#475569", "rounded"),
}
_MORE_STYLE = ("#f9fafb", "#6b7280", "rounded")
_TEXT_COLOR = "#1f2937"
_MAX_LABEL = 28
_CHAR_WIDTH = 6.6
_LABEL_CHAR_WIDTH = 6.2
_ROW_GAP = 64
_COLUMN_GAP = 16


@dataclass
class _Box:
    """A node as drawn, positioned by `_layout`."""

    lines: list[str]
    tooltip: str
    style: tuple[str, str, str]
    href: str | None
    is_self: bool
    width: float
    height: float
    x: float = 0.0
    y: float = 0.0


@dataclass(frozen=True)
class _Edge:
    source: _Box
    target: _Box
    label: str
    dashed: bool


def _truncate(text: str) -> str:
    return text if len(text) <= _MAX_LABEL else text[: _MAX_LABEL - 1] + "…"


def _box(
    lines: list[str], tooltip: str, style: tuple[str, str, str], href: str | None, edge: str, is_self: bool
) -> _Box:
    # Wide enough for its own text and for the label on the edge leaving it, so labels of side-by-side nodes don't
    # overlap.
    width = max(max(len(line) for line in lines) * _CHAR_WIDTH, len(edge) * _LABEL_CHAR_WIDTH + 8) + 24
    height = 16 * len(lines) + 16 + (12 if style[2] == "cylinder" else 0)
    return _Box(lines, tooltip, style, href, is_self, max(width, 80), height)


def _node_box(node: Node, is_self: bool = False) -> _Box:
    style = _NODE_STYLES.get(node.kind, _MORE_STYLE)
    if style[2] == "cylinder":
        lines = [node.kind, _truncate(node.name)]
    else:
        lines = [_truncate(f"{node.kind}: {node.name}")]
    href = None if node.url is None else _resolved_url(node.url)
    return _box(lines, f"{node.kind}: {node.name}", style, href, node.edge, is_self)


def _shown_counts(groups: Sequence[Group]) -> list[int]:
    """How many nodes of each group to draw, within `graph_max_nodes` with the card's own node.

    Each group gets an even share of what's left after smaller groups take what they need, and a group that's cut
    gives one of its share to the node counting the rest. With a share of one, a group draws only that counting node.
    Every group with resources draws at least one node, so the cap is exceeded only when it's below one more than the
    number of groups.
    """
    budget = _options.graph_max_nodes - 1
    shown = [0] * len(groups)
    order = sorted((i for i in range(len(groups)) if groups[i].nodes), key=lambda i: len(groups[i].nodes))
    for position, index in enumerate(order):
        group = groups[index]
        share = max(budget // (len(order) - position), 1)
        needed = len(group.nodes) + (1 if group.has_more else 0)
        if needed <= share:
            shown[index] = len(group.nodes)
            budget -= needed
        else:
            shown[index] = share - 1
            budget -= share
    return shown


def _layout(graph: Graph, card_id: str) -> tuple[list[list[_Box]], list[_Edge]]:
    """Place the graph's nodes in rows, bottom to top, and join them with edges."""
    own = _node_box(graph.node, is_self=True)
    boxes: dict[str, _Box] = {graph.node.rid: own}
    layers: dict[str, int] = {graph.node.rid: 0}
    rows: dict[int, list[_Box]] = {0: [own]}
    edges: list[_Edge] = []
    for group, count in zip(graph.groups, _shown_counts(graph.groups), strict=True):
        if not group.nodes or group.to.rid not in boxes:
            continue
        anchor = boxes[group.to.rid]
        layer = layers[group.to.rid] + (-1 if group.below else 1)
        members = [(_node_box(node), node.edge) for node in group.nodes[:count]]
        for node, (box, _) in zip(group.nodes, members):
            boxes.setdefault(node.rid, box)
            layers.setdefault(node.rid, layer)
        left_out = len(group.nodes) - count
        if left_out or group.has_more:
            members.append((_more_box(group, left_out, f"#{card_id}-{_slug(group.section)}"), ""))
        for box, label in members:
            rows.setdefault(layer, []).append(box)
            source, target = (box, anchor) if group.below else (anchor, box)
            edges.append(_Edge(source, target, label, group.dashed))
    return [rows[layer] for layer in sorted(rows, reverse=True)], edges


def _more_box(group: Group, left_out: int, href: str) -> _Box:
    kind = group.nodes[0].kind.lower()
    if group.has_more:
        # At least one more exists past the ones loaded.
        text = f"+{left_out + 1} or more {group.section.lower()}"
    else:
        text = f"+{left_out} more {kind if left_out == 1 else group.section.lower()}"
    return _box([text], f"{text}: see {group.section} below", _MORE_STYLE, href, "", False)


def _slug(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def diagram(graph: Graph, card_id: str) -> tuple[str, int]:
    """Draw the graph as inline SVG, and return it with the number of nodes drawn.

    The SVG needs no script, so it displays and links the same in untrusted and saved notebooks and on static
    renders. Every user value in it is escaped.

    Nodes link through HTML links laid over the SVG rather than SVG ``<a>`` elements: JupyterLab sets ``target``
    on every link in an output, which fails on SVG links, whose ``target`` can't be set.
    """
    rows, edges = _layout(graph, card_id)
    pad = 8
    width = max(sum(box.width for box in row) + _COLUMN_GAP * (len(row) - 1) for row in rows) + 2 * pad
    y = float(pad)
    for row in rows:
        row_height = max(box.height for box in row)
        x = (width - sum(box.width for box in row) - _COLUMN_GAP * (len(row) - 1)) / 2
        for box in row:
            box.x, box.y = x, y + (row_height - box.height) / 2
            x += box.width + _COLUMN_GAP
        y += row_height + _ROW_GAP
    height = y - _ROW_GAP + pad

    parts = [_draw_edges(edges), *(_draw_box(box) for row in rows for box in row)]
    count = sum(len(row) for row in rows)
    links = "".join(_link_over(box, box.href) for row in rows for box in row if box.href is not None)
    html = (
        f'<div class="nominal-repr-graph"><div class="nominal-repr-graph-canvas" '
        f'style="width: {width:.0f}px; height: {height:.0f}px">'
        f'<svg viewBox="0 0 {width:.0f} {height:.0f}" width="{width:.0f}" height="{height:.0f}" role="img" '
        f'aria-label="{escape(_alt_text(graph))}" font-size="12">{"".join(parts)}</svg>{links}</div></div>'
    )
    return html, count


def _alt_text(graph: Graph) -> str:
    related = ", ".join(
        f"{len(group.nodes)}{'+' if group.has_more else ''} {group.section.lower()}"
        for group in graph.groups
        if group.nodes
    )
    return f"{graph.node.kind} {graph.node.name} and its related resources: {related}"


def _draw_edges(edges: Sequence[_Edge]) -> str:
    """Draw edges as curves from the top of their source to the bottom of their target, with arrowheads.

    The edges meeting a node spread across its side in the order of their other ends, so they don't cross there.
    """
    ends: dict[tuple[int, str], list[_Edge]] = {}
    for edge in edges:
        ends.setdefault((id(edge.source), "out"), []).append(edge)
        ends.setdefault((id(edge.target), "in"), []).append(edge)

    def spread(box: _Box, side: str, edge: _Edge) -> float:
        group = sorted(ends[(id(box), side)], key=lambda e: (e.target if side == "out" else e.source).x)
        return box.x + box.width * (group.index(edge) + 1) / (len(group) + 1)

    lines, labels = [], []
    for edge in edges:
        sx, sy = spread(edge.source, "out", edge), edge.source.y
        tx, ty = spread(edge.target, "in", edge), edge.target.y + edge.target.height
        mid = (sy + ty + 8) / 2
        dash = ' stroke-dasharray="5 4"' if edge.dashed else ""
        lines.append(
            f'<path d="M{sx:.1f},{sy:.1f} C{sx:.1f},{mid:.1f} {tx:.1f},{mid:.1f} {tx:.1f},{ty + 8:.1f}" '
            f'fill="none" stroke="currentColor" stroke-opacity="0.6" stroke-width="1.5"{dash}/>'
            f'<polygon points="{tx:.1f},{ty:.1f} {tx - 4:.1f},{ty + 8:.1f} {tx + 4:.1f},{ty + 8:.1f}" '
            'fill="currentColor" fill-opacity="0.6"/>'
        )
        if edge.label:
            # Near the source, where the edges meeting one node are still apart.
            t = 0.3
            lx = sx + (tx - sx) * (3 * t**2 - 2 * t**3)
            ly = sy + (ty + 8 - sy) * t
            label_width = len(edge.label) * _LABEL_CHAR_WIDTH + 8
            labels.append(
                f'<rect x="{lx - label_width / 2:.1f}" y="{ly - 9:.1f}" width="{label_width:.1f}" height="18" rx="3" '
                'fill="#f3f4f6"/>'
                f'<text x="{lx:.1f}" y="{ly + 4:.1f}" text-anchor="middle" font-size="11" fill="{_TEXT_COLOR}">'
                f"{escape(edge.label)}</text>"
            )
    return "".join(lines + labels)


def _draw_box(box: _Box) -> str:
    fill, stroke, shape = box.style
    x, y, w, h = box.x, box.y, box.width, box.height
    stroke_width = 3 if box.is_self else 1.5
    paint = f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"'
    if shape == "cylinder":
        rx, ry = w / 2, 6
        body = (
            f'<path d="M{x:.1f},{y + ry:.1f} a{rx:.1f},{ry} 0 0 1 {w:.1f},0 v{h - 2 * ry:.1f} '
            f'a{rx:.1f},{ry} 0 0 1 {-w:.1f},0 Z" {paint}/>'
            f'<path d="M{x:.1f},{y + ry:.1f} a{rx:.1f},{ry} 0 0 0 {w:.1f},0" fill="none" stroke="{stroke}" '
            f'stroke-width="{stroke_width}"/>'
        )
        top = y + 2 * ry
    else:
        corner = 10 if shape == "rounded" else 3
        dash = ' stroke-dasharray="4 3"' if box.style == _MORE_STYLE else ""
        body = f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{corner}" {paint}{dash}/>'
        top = y
    text_top = top + (y + h - top - 16 * len(box.lines)) / 2
    weight = ' font-weight="600"' if box.is_self else ""
    text = "".join(
        f'<text x="{x + w / 2:.1f}" y="{text_top + 16 * index + 12:.1f}" text-anchor="middle" '
        f'fill="{_TEXT_COLOR}"{weight}>{escape(line)}</text>'
        for index, line in enumerate(box.lines)
    )
    return f"<g><title>{escape(box.tooltip)}</title>{body}{text}</g>"


def _link_over(box: _Box, href: str) -> str:
    """A transparent link to `href` covering a node, with the node's full name as its tooltip."""
    # Links within the card, to a section, open in place; links to the Nominal app open a new tab.
    target = "" if href.startswith("#") else ' target="_blank" rel="noopener noreferrer"'
    return (
        f'<a class="nominal-repr-graph-link" href="{escape(href)}"{target} title="{escape(box.tooltip)}" '
        f'aria-label="{escape(box.tooltip)}" '
        f'style="left: {box.x:.1f}px; top: {box.y:.1f}px; width: {box.width:.1f}px; height: {box.height:.1f}px"></a>'
    )


def render(
    *,
    kind: str,
    title: str,
    fallback: str,
    fields: Sequence[tuple[str, str | None]],
    properties: Mapping[str, object] | None = None,
    sections: Sequence[Section | Loading | None] = (),
    archived: bool = False,
    url: Callable[[], str] | None = None,
    graph: Callable[[], Graph | None] | None = None,
) -> str | None:
    """Build a resource's card, or return `None` so the notebook shows its plain ``repr`` instead.

    Args:
        kind: The resource type, such as ``"Run"``.
        title: The resource's display name, unescaped.
        fallback: Plain text shown where the notebook strips styles, such as on GitHub.
        fields: Label and value HTML for the card's table. Values must already be escaped. Fields whose value is
            `None` are left out.
        properties: The resource's properties, listed first after the table.
        sections: Collapsible parts after the properties and the diagram. Empty sections and `None` are left
            out. Sections still loading are waited for until the card's deadline.
        archived: Whether to mark the resource as archived.
        url: Returns the resource's page in the Nominal app.
        graph: Returns the resource and the resources around it, drawn as a diagram after the properties, or
            `None` when it has none. It runs with the card's deadline, so it can wait for loads the sections
            share. If it fails, the card shows a note in the diagram's place.
    """
    if _options.style == "text":
        return None

    header = [f'<span class="nominal-repr-kind">{escape(kind)}</span>', f"<strong>{escape(title)}</strong>"]
    if archived:
        header.append('<span class="nominal-repr-archived">archived</span>')
    if url is not None and (page := _resolved_url(url)) is not None:
        header.append(f'<span class="nominal-repr-link">{link(page, "Open in Nominal ↗")}</span>')

    rows = "".join(f"<tr><th>{escape(label)}</th><td>{value}</td></tr>" for label, value in fields if value is not None)
    body = [f'<div class="nominal-repr-header">{" ".join(header)}</div>', f"<table>{rows}</table>"]
    # Section IDs are unique to the card, so a diagram's links to its sections reach this card's.
    card_id = f"nominal-repr-{secrets.token_hex(4)}"
    if graph is not None and _options.show_graph and _options.fetch_details:
        sections = [Loading("Relationships", lambda: _graph_section(graph(), card_id)), *sections]
    if properties is not None:
        sections = [properties_section(properties), *sections]
    loaded = [section.result() if isinstance(section, Loading) else section for section in sections]
    body.extend(
        _render_section(section, card_id)
        for section in loaded
        if section is not None and (section.rows or section.body or section.note)
    )
    return (
        f"<style>{_css()}</style>"
        f'<pre class="nominal-repr-fallback">{escape(fallback)}</pre>'
        f'<div class="nominal-repr" style="display: none">{"".join(body)}</div>'
    )


def _graph_section(graph: Graph | None, card_id: str) -> Section:
    if graph is None or not any(group.nodes for group in graph.groups):
        return Section("Relationships")
    svg, count = diagram(graph, card_id)
    return Section("Relationships", body=svg, counted=False, collapsed=count > _options.collapse_threshold)


def _render_section(section: Section, card_id: str) -> str:
    rows = section.rows[: _options.max_items]
    left_out = len(section.rows) - len(rows)
    content = section.body
    if rows:
        head = "".join(f"<th>{escape(column)}</th>" for column in section.columns)
        lines = [f"<tr>{head}</tr>"] if head else []
        for row in rows:
            cells = [f"<td>{cell}</td>" for cell in row]
            if not section.columns and len(row) > 1:
                cells[0] = f"<th>{row[0]}</th>"
            lines.append(f"<tr>{''.join(cells)}</tr>")
        content += f"<table>{''.join(lines)}</table>"
    if left_out:
        content += f'<p class="nominal-repr-more">… and {left_out} more</p>'
    elif section.has_more:
        content += f'<p class="nominal-repr-more">Showing the first {len(rows)}.</p>'
    if section.note:
        content += f'<p class="nominal-repr-more">{escape(section.note)}</p>'

    count = f"{len(section.rows)}{'+' if section.has_more else ''}"
    summary = escape(section.title) + (f" ({count})" if section.rows and section.counted else "")
    is_open = "" if section.collapsed or len(section.rows) > _options.collapse_threshold else " open"
    anchor = f"{card_id}-{_slug(section.title)}"
    return f'<details id="{anchor}"{is_open}><summary>{summary}</summary>{content}</details>'


@functools.cache
def _css() -> str:
    """The cards' styles, from `_display.css` next to this module, without its comments, which every card repeats."""
    text = resources.files(__package__).joinpath("_display.css").read_text(encoding="utf-8")
    return re.sub(r"/\*.*?\*/\s*", "", text, flags=re.DOTALL)
