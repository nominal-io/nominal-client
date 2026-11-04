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
    collapse_threshold: int = 10
    """Sections with more items than this, such as properties, start collapsed."""
    max_items: int = 50
    """The most items a section lists, and loads where it can. The section says how many more were left out."""
    fetch_timeout: float = 5.0
    """Seconds a card waits for what it loads from Nominal, in total. Sections still loading then say so, and
    links that need the workspace show as plain text."""

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


def _wait(future: Future[T]) -> T:
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
    return _wait(_pool().submit(load))


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
    future: Future[Section]

    def result(self) -> Section:
        """The loaded section, or one whose note says why it didn't load."""
        try:
            return _wait(self.future)
        except FutureTimeoutError:
            return Section(self.title, note=f"Didn't load within {_options.fetch_timeout:g} s.")
        except Exception as e:  # noqa: BLE001 - a display shows what it can; the error is reported in the card
            message = str(e).splitlines()[0] if str(e) else ""
            return Section(self.title, note=f"Couldn't load: {type(e).__name__} {message}"[:300])


def fetched(title: str, load: Callable[[], Section]) -> Loading | None:
    """Start loading a section from Nominal, or return `None` when loading is off or the text style builds no card.

    Sections load in parallel. A failure or a load past the card's deadline becomes a note in the section, so the
    rest of the card still displays.
    """
    if _options.style == "text" or not _options.fetch_details:
        return None
    return Loading(title, _pool().submit(load))


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


def render(
    *,
    kind: str,
    title: str,
    fallback: str,
    fields: Sequence[tuple[str, str | None]],
    sections: Sequence[Section | Loading | None] = (),
    archived: bool = False,
    url: Callable[[], str] | None = None,
) -> str | None:
    """Build a resource's card, or return `None` so the notebook shows its plain ``repr`` instead.

    Args:
        kind: The resource type, such as ``"Run"``.
        title: The resource's display name, unescaped.
        fallback: Plain text shown where the notebook strips styles, such as on GitHub.
        fields: Label and value HTML for the card's table. Values must already be escaped. Fields whose value is
            `None` are left out.
        sections: Collapsible parts after the table. Empty sections and `None` are left out. Sections still
            loading are waited for until the card's deadline.
        archived: Whether to mark the resource as archived.
        url: Returns the resource's page in the Nominal app.
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
    loaded = [section.result() if isinstance(section, Loading) else section for section in sections]
    body.extend(
        _render_section(section)
        for section in loaded
        if section is not None and (section.rows or section.body or section.note)
    )
    return (
        f"<style>{_css()}</style>"
        f'<pre class="nominal-repr-fallback">{escape(fallback)}</pre>'
        f'<div class="nominal-repr" style="display: none">{"".join(body)}</div>'
    )


def _render_section(section: Section) -> str:
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
    is_open = "" if len(section.rows) > _options.collapse_threshold else " open"
    return f"<details{is_open}><summary>{summary}</summary>{content}</details>"


@functools.cache
def _css() -> str:
    """The cards' styles, from `_display.css` next to this module, without its comments, which every card repeats."""
    text = resources.files(__package__).joinpath("_display.css").read_text(encoding="utf-8")
    return re.sub(r"/\*.*?\*/\s*", "", text, flags=re.DOTALL)
