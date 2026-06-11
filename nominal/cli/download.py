"""Terminal UX: Click entry-point launches a Textual TUI wizard to browse
assets → datasets → select channels → pick time bounds → download to disk.
"""

from __future__ import annotations

import datetime
import logging
import pathlib
import warnings
from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

import click
import pandas as pd
import polars as pl
from rich.box import HORIZONTALS
from rich.panel import Panel
from rich.syntax import Syntax
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ProgressBar,
    RadioButton,
    RadioSet,
    RichLog,
    Static,
)

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import Asset, Channel, Dataset, Event, NominalClient, Run
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# UI helpers (kept from original)
# --------------------------------------------------------------------------------------


def _render_properties(props: Optional[Mapping[str, str]]) -> str:
    if not props:
        return "-"
    # show a compact key=value list
    items = [f"'{k}'='{v}'" for k, v in list(props.items())[:6]]
    suffix = " ..." if props and len(props) > 6 else ""
    return ", ".join(items) + suffix


def _render_labels(labels: Optional[Sequence[str]]) -> str:
    if not labels:
        return "-"
    # show a compact key=value list
    items = [f"'{label}'" for label in labels[:6]]
    suffix = " ..." if labels and len(labels) > 6 else ""
    return ", ".join(items) + suffix


def _parse_utc_ts(ts: str) -> datetime.datetime | None:
    dt = pd.to_datetime(ts, utc=True)
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def _normalize_duration_text(s: str) -> str:
    s = s.strip()
    if not s:
        return s
    s = s.replace("and", " ")
    s = s.replace("mins", "minutes").replace("secs", "seconds")
    return " ".join(s.split())


# --------------------------------------------------------------------------------------
# Downloaders (standalone classes, no abstract base)
# --------------------------------------------------------------------------------------


class CsvDownloader:
    @staticmethod
    def write_dataframe(df: pl.DataFrame, part_idx: int, directory: pathlib.Path, prefix: str) -> pathlib.Path:
        out_path = directory / f"{prefix}-part_{part_idx}.csv"
        df.write_csv(out_path)
        return out_path

    @staticmethod
    def instructions_code(directory: pathlib.Path, prefix: str) -> list[tuple[str, str, str]]:
        old_code = f'''
files = dir(fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.csv" ...
));

nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat(c{{{{:}}}}), ...
        cellfun( ...
            @readtable, ...
            fullfile({{{{files.folder}}}}, {{{{files.name}}}}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);'''

        new_code = f'''
data_dir = fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.csv" ...
);
ds = tabularTextDatastore(data_dir);
nominal_data = sortrows(readall(ds), "timestamp");'''

        return [
            ("Load downloaded CSV data into MATLAB 2019b+", "matlab", old_code.strip()),
            ("Load downloaded CSV data into MATLAB 2023b+", "matlab", new_code.strip()),
        ]


class ParquetDownloader:
    @staticmethod
    def write_dataframe(df: pl.DataFrame, part_idx: int, directory: pathlib.Path, prefix: str) -> pathlib.Path:
        out_path = directory / f"{prefix}-part_{part_idx}.parquet"
        df.write_parquet(out_path, compression="snappy")
        return out_path

    @staticmethod
    def instructions_code(directory: pathlib.Path, prefix: str) -> list[tuple[str, str, str]]:
        old_code = f'''
files=dir(fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.parquet" ...
));
nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat (c{{{{:}}}}), ...
        cellfun( ...
            @parquetread, ...
            fullfile({{{{files.folder}}}}, {{{{files.name}}}}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);'''

        new_code = f'''
data_dir = fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.parquet" ...
);
nominal_data = sortrows(readall(parquetDatastore(data_dir)), "timestamp");'''

        return [
            ("Load downloaded parquet data into MATLAB 2019b+", "matlab", old_code.strip()),
            ("Load downloaded parquet data into MATLAB 2023b+", "matlab", new_code.strip()),
        ]


# --------------------------------------------------------------------------------------
# Shared state
# --------------------------------------------------------------------------------------


@dataclass
class DownloadState:
    asset: Asset | None = None
    dataset: Dataset | None = None
    refname: str | None = None
    channels: list[Channel] = field(default_factory=list)
    output_dir: pathlib.Path | None = None
    start: datetime.datetime | None = None
    end: datetime.datetime | None = None
    scope_tags: Mapping[str, str] | None = None


# --------------------------------------------------------------------------------------
# Logging handler that routes to RichLog widget
# --------------------------------------------------------------------------------------


class _TextualLogHandler(logging.Handler):
    def __init__(self, rich_log: RichLog) -> None:
        super().__init__()
        self._rich_log = rich_log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._rich_log.write(msg)
        except Exception:
            self.handleError(record)


# --------------------------------------------------------------------------------------
# Screens
# --------------------------------------------------------------------------------------


class AssetSelectionScreen(Screen):
    BINDINGS = [("escape", "app.quit", "Quit")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Loading assets...", id="loading-label")
        dt = DataTable(id="asset-table", cursor_type="row")
        dt.display = False
        yield dt
        yield Footer()

    def on_mount(self) -> None:
        self._load_assets()

    @work(thread=True)
    def _load_assets(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        client = app.client
        assets = client.search_assets()
        if not assets:
            self.app.call_from_thread(self._show_no_assets)
            return

        raw_assets = client._clients.assets.get_assets(client._clients.auth_header, [a.rid for a in assets])
        sorted_assets = sorted(assets, key=lambda a: pd.to_datetime(raw_assets[a.rid].updated_at), reverse=True)
        self.app.call_from_thread(self._populate_table, sorted_assets)

    def _show_no_assets(self) -> None:
        self.query_one("#loading-label", Label).update("No assets available!")

    def _populate_table(self, assets: list[Asset]) -> None:
        self._assets = assets
        loading = self.query_one("#loading-label", Label)
        loading.display = False

        table = self.query_one("#asset-table", DataTable)
        table.add_columns("Name", "Description", "Labels", "Properties")
        for asset in assets:
            table.add_row(
                asset.name,
                asset.description or "-",
                _render_labels(asset.labels),
                _render_properties(asset.properties),
            )
        table.display = True
        table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if not hasattr(self, "_assets"):
            return
        asset = self._assets[event.cursor_row]
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.asset = asset
        app.push_screen(DatasetSelectionScreen())


class DatasetSelectionScreen(Screen):
    BINDINGS = [("escape", "go_back", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Loading datasets...", id="loading-label")
        dt = DataTable(id="dataset-table", cursor_type="row")
        dt.display = False
        yield dt
        yield Footer()

    def on_mount(self) -> None:
        self._load_datasets()

    def action_go_back(self) -> None:
        self.app.pop_screen()

    @work(thread=True)
    def _load_datasets(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        asset = app.state.asset
        assert asset is not None
        datasets_by_ref = {refname: dataset for refname, dataset in asset.list_datasets()}
        if not datasets_by_ref:
            self.app.call_from_thread(self._show_no_datasets)
            return

        if len(datasets_by_ref) == 1:
            refname = list(datasets_by_ref.keys())[0]
            dataset = datasets_by_ref[refname]
            self.app.call_from_thread(self._auto_advance, refname, dataset)
            return

        self.app.call_from_thread(self._populate_table, datasets_by_ref)

    def _show_no_datasets(self) -> None:
        self.query_one("#loading-label", Label).update("No datasets for this asset!")

    def _auto_advance(self, refname: str, dataset: Dataset) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.refname = refname
        app.state.dataset = dataset
        self._resolve_scope_tags_and_advance()

    def _populate_table(self, datasets_by_ref: dict[str, Dataset]) -> None:
        self._dataset_pairs: list[tuple[str, Dataset]] = list(datasets_by_ref.items())
        loading = self.query_one("#loading-label", Label)
        loading.display = False

        table = self.query_one("#dataset-table", DataTable)
        table.add_columns("Name", "Refname", "Description", "Labels", "Properties")
        for refname, dataset in self._dataset_pairs:
            table.add_row(
                dataset.name,
                refname,
                dataset.description or "-",
                _render_labels(dataset.labels),
                _render_properties(dataset.properties),
            )
        table.display = True
        table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if not hasattr(self, "_dataset_pairs"):
            return
        refname, dataset = self._dataset_pairs[event.cursor_row]
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.refname = refname
        app.state.dataset = dataset
        self._resolve_scope_tags_and_advance()

    def _resolve_scope_tags_and_advance(self) -> None:
        self._resolve_tags()

    @work(thread=True)
    def _resolve_tags(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        client = app.client
        asset = app.state.asset
        refname = app.state.refname
        assert asset is not None and refname is not None

        raw_asset = client._clients.assets.get_assets(client._clients.auth_header, [asset.rid])[asset.rid]
        scope_tags = None
        for raw_datascope in raw_asset.data_scopes:
            if raw_datascope.data_scope_name == refname:
                scope_tags = raw_datascope.series_tags
                break
        app.state.scope_tags = scope_tags
        self.app.call_from_thread(self._advance_to_channels)

    def _advance_to_channels(self) -> None:
        self.app.push_screen(ChannelSelectionScreen())


class ChannelSelectionScreen(Screen):
    BINDINGS = [
        ("escape", "go_back", "Back"),
        Binding("ctrl+t", "toggle_all", "Toggle All/None", priority=True),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("", id="channel-info")
        with Horizontal(id="search-bar"):
            yield Input(placeholder="Channel substrings (comma-separated) or * for all", id="search-input")
            yield Button("Search", id="search-btn", variant="primary")
        yield Label("Loading channels...", id="loading-label")
        dt = DataTable(id="channel-table", cursor_type="row")
        dt.display = False
        yield dt
        yield Label("", id="selection-count")
        with Horizontal(id="channel-buttons"):
            yield Button("Select All", id="select-all-btn")
            yield Button("Clear", id="clear-btn")
            yield Button("Confirm", id="confirm-btn", variant="success")
        yield Footer()

    def on_mount(self) -> None:
        self._all_channels: list[Channel] = []
        self._displayed_channels: list[Channel] = []
        self._selected_indices: set[int] = set()
        self._fetch_all_channels()

    def action_go_back(self) -> None:
        self.app.pop_screen()

    @work(thread=True)
    def _fetch_all_channels(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        dataset = app.state.dataset
        assert dataset is not None
        channels = list(dataset.search_channels())
        self.app.call_from_thread(self._channels_loaded, channels)

    def _channels_loaded(self, channels: list[Channel]) -> None:
        self._all_channels = channels
        loading = self.query_one("#loading-label", Label)
        loading.update(f"{len(channels)} channels available. Enter search query above.")
        info = self.query_one("#channel-info", Label)
        app: DownloadApp = self.app  # type: ignore[assignment]
        info.update(f"Dataset: {app.state.dataset.name}")  # type: ignore[union-attr]
        self.query_one("#search-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "search-btn":
            self._do_search()
        elif event.button.id == "select-all-btn":
            self._select_all()
        elif event.button.id == "clear-btn":
            self._clear_selection()
        elif event.button.id == "confirm-btn":
            self._confirm()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self._do_search()

    def _do_search(self) -> None:
        query = self.query_one("#search-input", Input).value.strip()
        if not query:
            return

        if query == "*":
            self._displayed_channels = list(self._all_channels)
        else:
            subqueries = [s.strip() for s in query.split(",") if s.strip()]
            self._search_channels(subqueries)
            return

        self._selected_indices = set(range(len(self._displayed_channels)))
        self._refresh_table()

    @work(thread=True)
    def _search_channels(self, subqueries: list[str]) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        dataset = app.state.dataset
        assert dataset is not None
        results = [
            channel for subquery in subqueries for channel in dataset.search_channels(exact_match=[subquery])
        ]
        self.app.call_from_thread(self._search_done, results)

    def _search_done(self, channels: list[Channel]) -> None:
        self._displayed_channels = channels
        self._selected_indices = set(range(len(channels)))
        self._refresh_table()

    def _refresh_table(self) -> None:
        table = self.query_one("#channel-table", DataTable)
        table.clear(columns=True)
        table.add_columns("Sel", "Name", "Description", "Data Type", "Unit")
        for idx, ch in enumerate(self._displayed_channels):
            marker = "X" if idx in self._selected_indices else " "
            table.add_row(
                marker,
                ch.name,
                ch.description or "-",
                ch.data_type.value if ch.data_type else "-",
                ch.unit or "-",
            )
        table.display = True
        table.focus()
        self._update_count()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        idx = event.cursor_row
        if idx in self._selected_indices:
            self._selected_indices.discard(idx)
        else:
            self._selected_indices.add(idx)
        # Update the selection marker in the table
        table = self.query_one("#channel-table", DataTable)
        row_key = table.ordered_rows[idx].key
        col_key = table.ordered_columns[0].key
        marker = "X" if idx in self._selected_indices else " "
        table.update_cell(row_key, col_key, marker)
        self._update_count()

    def action_toggle_all(self) -> None:
        if self._selected_indices == set(range(len(self._displayed_channels))):
            self._clear_selection()
        else:
            self._select_all()

    def _select_all(self) -> None:
        self._selected_indices = set(range(len(self._displayed_channels)))
        self._update_markers()

    def _clear_selection(self) -> None:
        self._selected_indices.clear()
        self._update_markers()

    def _update_markers(self) -> None:
        """Update selection markers in-place without rebuilding the table."""
        table = self.query_one("#channel-table", DataTable)
        col_key = table.ordered_columns[0].key
        for idx in range(len(self._displayed_channels)):
            row_key = table.ordered_rows[idx].key
            marker = "X" if idx in self._selected_indices else " "
            table.update_cell(row_key, col_key, marker)
        self._update_count()

    def _update_count(self) -> None:
        count = len(self._selected_indices)
        label = self.query_one("#selection-count", Label)
        warning = " (Warning: >500 channels may be slow!)" if count > 500 else ""
        label.update(f"{count} channel(s) selected{warning}")

    def _confirm(self) -> None:
        selected = [self._displayed_channels[i] for i in sorted(self._selected_indices)]
        if not selected:
            self.query_one("#selection-count", Label).update("No channels selected! Select at least one.")
            return
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.channels = selected
        app.push_screen(OutputDirScreen())


class OutputDirScreen(Screen):
    BINDINGS = [("escape", "go_back", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Enter output directory:")
        yield Input(value="./out", id="dir-input")
        yield Label("", id="dir-feedback")
        yield Button("Continue", id="continue-btn", variant="primary")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#dir-input", Input).focus()

    def action_go_back(self) -> None:
        self.app.pop_screen()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "continue-btn":
            self._continue()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "dir-input":
            self._continue()

    def _continue(self) -> None:
        raw = self.query_one("#dir-input", Input).value.strip()
        if not raw:
            self.query_one("#dir-feedback", Label).update("Please enter a directory path.")
            return
        out_dir = pathlib.Path(raw).resolve()
        if out_dir.exists() and not out_dir.is_dir():
            self.query_one("#dir-feedback", Label).update("Path exists but is not a directory!")
            return
        if not out_dir.exists():
            out_dir.mkdir(parents=True, exist_ok=True)
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.output_dir = out_dir
        app.push_screen(TimeBoundsScreen())


class TimeBoundsScreen(Screen):
    BINDINGS = [("escape", "go_back", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Choose time bounds source:")
        with RadioSet(id="bounds-mode"):
            yield RadioButton("Event (by RID)", value=True, id="radio-event")
            yield RadioButton("Run (by RID)", id="radio-run")
            yield RadioButton("Custom timestamps", id="radio-custom")
        with Vertical(id="input-area"):
            yield Input(placeholder="Enter RID or timestamp...", id="input-1")
            yield Input(placeholder="End timestamp (UTC) e.g. 2025-09-03T10:15:00Z", id="input-2")
            yield Input(placeholder="Duration e.g. 5m, 2 hours, 90s", id="input-duration")
        yield Label("", id="bounds-preview")
        yield Label("", id="bounds-error")
        yield Button("Confirm", id="confirm-btn", variant="primary")
        yield Footer()

    def on_mount(self) -> None:
        self._mode = "event"
        self._update_inputs()
        self.query_one("#input-1", Input).focus()

    def action_go_back(self) -> None:
        self.app.pop_screen()

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        pressed_id = event.pressed.id
        if pressed_id == "radio-event":
            self._mode = "event"
        elif pressed_id == "radio-run":
            self._mode = "run"
        else:
            self._mode = "custom"
        self._update_inputs()

    def _update_inputs(self) -> None:
        inp1 = self.query_one("#input-1", Input)
        inp2 = self.query_one("#input-2", Input)
        inp_dur = self.query_one("#input-duration", Input)

        if self._mode == "event":
            inp1.placeholder = "Event RID (copy from Nominal)"
            inp1.display = True
            inp2.display = False
            inp_dur.placeholder = "Duration (if event has none) e.g. 5m, 2h30m"
            inp_dur.display = True
        elif self._mode == "run":
            inp1.placeholder = "Run RID (copy from Nominal)"
            inp1.display = True
            inp2.placeholder = "End timestamp (if run has no end)"
            inp2.display = True
            inp_dur.display = False
        else:
            inp1.placeholder = "Start timestamp (UTC) e.g. 2025-09-03T10:15:00Z"
            inp1.display = True
            inp2.placeholder = "End timestamp (UTC) e.g. 2025-09-03T10:15:00Z"
            inp2.display = True
            inp_dur.display = False
        inp1.focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm-btn":
            self._resolve_bounds()

    def _resolve_bounds(self) -> None:
        self.query_one("#bounds-error", Label).update("")
        if self._mode == "event":
            self._resolve_event()
        elif self._mode == "run":
            self._resolve_run()
        else:
            self._resolve_custom()

    def _resolve_custom(self) -> None:
        raw_start = self.query_one("#input-1", Input).value.strip()
        raw_end = self.query_one("#input-2", Input).value.strip()
        start = _parse_utc_ts(raw_start) if raw_start else None
        end = _parse_utc_ts(raw_end) if raw_end else None
        if start is None:
            self.query_one("#bounds-error", Label).update("Invalid start timestamp.")
            return
        if end is None:
            self.query_one("#bounds-error", Label).update("Invalid end timestamp.")
            return
        if start >= end:
            self.query_one("#bounds-error", Label).update("End must be after start.")
            return
        self._set_bounds(start, end)

    @work(thread=True)
    def _resolve_event(self) -> None:
        rid = self.app.call_from_thread(lambda: self.query_one("#input-1", Input).value.strip())
        if not rid:
            self.app.call_from_thread(lambda: self.query_one("#bounds-error", Label).update("Enter an event RID."))
            return
        app: DownloadApp = self.app  # type: ignore[assignment]
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                event: Event = app.client.get_event(rid)
        except Exception as ex:
            self.app.call_from_thread(lambda: self.query_one("#bounds-error", Label).update(f"Failed: {ex}"))
            return

        event_duration = datetime.timedelta(seconds=event.duration / 1e9)
        if event_duration.total_seconds() == 0:
            raw_dur = self.app.call_from_thread(lambda: self.query_one("#input-duration", Input).value.strip())
            if not raw_dur:
                self.app.call_from_thread(
                    lambda: self.query_one("#bounds-error", Label).update(
                        "Event has no duration. Enter duration below."
                    )
                )
                return
            normalized = _normalize_duration_text(raw_dur)
            td = pd.to_timedelta(normalized, errors="coerce")
            if pd.isna(td):
                self.app.call_from_thread(
                    lambda: self.query_one("#bounds-error", Label).update("Invalid duration format.")
                )
                return
            event_duration = datetime.timedelta(seconds=float(td.total_seconds()))

        start = datetime.datetime.fromtimestamp(event.start / 1e9, tz=datetime.timezone.utc)
        self.app.call_from_thread(self._set_bounds, start, start + event_duration)

    @work(thread=True)
    def _resolve_run(self) -> None:
        rid = self.app.call_from_thread(lambda: self.query_one("#input-1", Input).value.strip())
        if not rid:
            self.app.call_from_thread(lambda: self.query_one("#bounds-error", Label).update("Enter a run RID."))
            return
        app: DownloadApp = self.app  # type: ignore[assignment]
        try:
            run: Run = app.client.get_run(rid)
        except Exception as ex:
            self.app.call_from_thread(lambda: self.query_one("#bounds-error", Label).update(f"Failed: {ex}"))
            return

        start = datetime.datetime.fromtimestamp(run.start / 1e9, tz=datetime.timezone.utc)
        if run.end is not None:
            end = datetime.datetime.fromtimestamp(run.end / 1e9, tz=datetime.timezone.utc)
        else:
            raw_end = self.app.call_from_thread(lambda: self.query_one("#input-2", Input).value.strip())
            if not raw_end:
                self.app.call_from_thread(
                    lambda: self.query_one("#bounds-error", Label).update(
                        "Run has no end. Enter end timestamp above."
                    )
                )
                return
            end = _parse_utc_ts(raw_end)
            if end is None:
                self.app.call_from_thread(
                    lambda: self.query_one("#bounds-error", Label).update("Invalid end timestamp.")
                )
                return

        self.app.call_from_thread(self._set_bounds, start, end)

    def _set_bounds(self, start: datetime.datetime, end: datetime.datetime) -> None:
        seconds = (end - start).total_seconds()
        preview = self.query_one("#bounds-preview", Label)
        preview.update(f"Bounds: {start.isoformat()} to {end.isoformat()} ({seconds:.1f}s)")
        app: DownloadApp = self.app  # type: ignore[assignment]
        app.state.start = start
        app.state.end = end
        app.push_screen(DownloadScreen())


class DownloadScreen(Screen):
    BINDINGS = [("escape", "go_back", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Download Summary", id="summary-title")
        yield Static(id="summary")
        yield Button("Start Download", id="start-btn", variant="success")
        yield ProgressBar(id="progress", total=100, show_eta=False)
        yield RichLog(id="log", highlight=True, markup=True)
        with VerticalScroll(id="instructions-area"):
            yield Static(id="instructions")
        yield Footer()

    def on_mount(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        s = app.state
        assert s.asset and s.dataset and s.channels and s.output_dir and s.start and s.end
        seconds = (s.end - s.start).total_seconds()
        summary_text = (
            f"Asset: {s.asset.name}\n"
            f"Dataset: {s.dataset.name} (ref: {s.refname})\n"
            f"Channels: {len(s.channels)}\n"
            f"Output: {s.output_dir}\n"
            f"Time: {s.start.isoformat()} to {s.end.isoformat()} ({seconds:.1f}s)\n"
            f"Format: {app.download_format}"
        )
        self.query_one("#summary", Static).update(summary_text)

    def action_go_back(self) -> None:
        self.app.pop_screen()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "start-btn":
            event.button.disabled = True
            self._run_download()

    @work(thread=True)
    def _run_download(self) -> None:
        app: DownloadApp = self.app  # type: ignore[assignment]
        s = app.state
        assert s.dataset and s.channels and s.output_dir and s.start and s.end

        rich_log = self.query_one("#log", RichLog)
        handler = _TextualLogHandler(rich_log)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logging.getLogger().addHandler(handler)

        try:
            self.app.call_from_thread(lambda: rich_log.write("Starting download..."))

            downloader = CsvDownloader if app.download_format == "csv" else ParquetDownloader
            dataset_prefix = s.dataset.rid.split(".")[-1]
            exporter = PolarsExportHandler(
                app.client,
                points_per_dataframe=app.points_per_file,
                channels_per_request=app.channels_per_request,
            )

            for idx, df in enumerate(
                exporter.export(
                    s.channels,
                    int(s.start.timestamp() * 1e9),
                    int(s.end.timestamp() * 1e9),
                    s.scope_tags,
                    join_batches=True,
                )
            ):
                out_path = downloader.write_dataframe(df, idx, s.output_dir, dataset_prefix)
                self.app.call_from_thread(lambda p=out_path, i=idx: rich_log.write(f"Wrote part {i}: {p}"))

            self.app.call_from_thread(lambda: rich_log.write("[bold green]Download complete![/bold green]"))

            # Show MATLAB instructions
            snippets = downloader.instructions_code(s.output_dir, dataset_prefix)
            self.app.call_from_thread(self._show_instructions, snippets)
        except Exception as ex:
            self.app.call_from_thread(lambda: rich_log.write(f"[bold red]Error: {ex}[/bold red]"))
        finally:
            logging.getLogger().removeHandler(handler)

    def _show_instructions(self, snippets: list[tuple[str, str, str]]) -> None:
        instructions = self.query_one("#instructions", Static)
        renderables = []
        for title, language, code in snippets:
            panel = Panel(
                Syntax(code, language, theme="monokai", padding=1),
                title=title,
                padding=1,
                box=HORIZONTALS,
            )
            renderables.append(panel)
        from rich.console import Group

        instructions.update(Group(*renderables))


# --------------------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------------------


class DownloadApp(App):
    CSS = """
    #search-bar {
        height: 3;
        margin: 1 0;
    }
    #search-bar Input {
        width: 1fr;
    }
    #search-bar Button {
        width: auto;
    }
    #channel-buttons {
        height: 3;
        margin: 1 0;
    }
    #channel-buttons Button {
        width: auto;
        margin: 0 1;
    }
    DataTable {
        height: 1fr;
    }
    #selection-count {
        margin: 1 0;
        color: $accent;
    }
    #summary {
        margin: 1 0;
        padding: 1;
        background: $surface;
    }
    #summary-title {
        text-style: bold;
    }
    #progress {
        margin: 1 0;
    }
    #log {
        height: 1fr;
        min-height: 8;
        margin: 1 0;
    }
    #instructions-area {
        height: auto;
        max-height: 50%;
    }
    #bounds-mode {
        margin: 1 0;
    }
    #input-area {
        height: auto;
        margin: 1 0;
    }
    #input-area Input {
        margin: 0 0 1 0;
    }
    #bounds-preview {
        margin: 1 0;
        color: $accent;
    }
    #bounds-error {
        color: $error;
    }
    #dir-feedback {
        color: $error;
    }
    #loading-label {
        margin: 1 0;
        color: $text-muted;
    }
    #channel-info {
        margin: 1 0;
    }
    """

    TITLE = "Nominal Download"
    BINDINGS = [Binding("ctrl+q", "quit", "Quit", priority=True)]

    def __init__(
        self,
        client: NominalClient,
        channels_per_request: int,
        points_per_file: int,
        download_format: str,
        log_level: int,
    ) -> None:
        super().__init__()
        self.client = client
        self.channels_per_request = channels_per_request
        self.points_per_file = points_per_file
        self.download_format = download_format
        self.log_level = log_level
        self.state = DownloadState()

    def on_mount(self) -> None:
        self.push_screen(AssetSelectionScreen())


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


@click.command("download")
@click.option(
    "--channels-per-request",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Number of channels to request at a time. "
    "For requesting a small number of channels, setting this value low helps parallelize downloads.",
)
@click.option(
    "--points-per-file",
    type=click.IntRange(min=1),
    default=25_000_000,
    show_default=True,
    help="Number of points to put in each written file.",
)
@click.option("--format", type=click.Choice(["csv", "parquet"]), default="parquet", show_default=True)
@client_options
@global_options
@click.pass_context
def download_cmd(
    ctx: click.Context, client: NominalClient, channels_per_request: int, points_per_file: int, format: str
) -> None:
    """Browse assets, pick a dataset, filter channels by exact name, and download."""
    app = DownloadApp(
        client=client,
        channels_per_request=channels_per_request,
        points_per_file=points_per_file,
        download_format=format,
        log_level=ctx.obj["log_level"],
    )
    app.run()


if __name__ == "__main__":
    download_cmd()
