"""Terminal UX: Click + Rich CLI to browse assets → datasets → select channels by
exact-match queries → download to disk for users to open in matlab.
"""

from __future__ import annotations

import abc
import datetime
import logging
import pathlib
import warnings
from typing import Any, List, Mapping, Optional, Sequence

import click
import pandas as pd
import polars as pl
from rich.box import ASCII, HORIZONTALS
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Column, Table

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import Asset, Channel, Dataset, Event, NominalClient, Run
from nominal.experimental.logging.rich_log_handler import configure_rich_logging
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# UI helpers
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


class DataDownloader(abc.ABC):
    def __init__(self, client: NominalClient, console: Console):
        """Base class for downloading data to disks from the Nominal API

        Args:
            client: Nominal client for communicating with the API
            console: Rich console for printing to
        """
        self._client = client
        self._console = console

    def _render_table(self, table: Table, max_unpaged: int = 20) -> None:
        if table.row_count > max_unpaged:
            with self._console.pager():
                self._console.print(table)
        else:
            self._console.print(table)

    def _select_asset(self) -> Asset | None:
        with self._console.status("Loading assets…", spinner="dots"):
            assets = self._client.search_assets()
            if not assets:
                self._console.print("No assets available!", style=Style(color="red"))
                return None

        # Sort by last updated timestamps
        raw_assets = self._client._clients.assets.get_assets(
            self._client._clients.auth_header, [asset.rid for asset in assets]
        )
        sorted_assets = sorted(assets, key=lambda asset: pd.to_datetime(raw_assets[asset.rid].updated_at), reverse=True)
        table = Table(
            Column("#", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
            Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
            Column("Description", style=Style(color="cyan"), ratio=5, overflow="fold"),
            Column("Labels", style=Style(color="green"), ratio=3, overflow="fold"),
            Column("Properties", style=Style(color="magenta"), ratio=4, overflow="fold"),
            title=f"Available Assets ({len(sorted_assets)})",
            expand=True,
            box=ASCII,
        )
        for idx, asset in enumerate(sorted_assets):
            table.add_row(
                str(idx),
                asset.name,
                asset.description or "-",
                _render_labels(asset.labels),
                _render_properties(asset.properties),
            )

        self._render_table(table)

        while True:
            idx = IntPrompt.ask("Select an asset #")
            if 0 <= idx < len(sorted_assets):
                asset = sorted_assets[idx]
                self._console.print(f"Selected asset: {asset.name} ({asset.rid})", style=Style(color="magenta"))
                return asset
            else:
                self._console.print(
                    f"Please enter a number between 0 and {len(sorted_assets) - 1}.", style=Style(color="red")
                )

            if Confirm.ask("See asset table again?", default=False, show_default=True):
                self._render_table(table)

    def _select_dataset(self, asset: Asset) -> tuple[str | None, Dataset | None]:
        with self._console.status("Loading datasets...", spinner="dots"):
            datasets_by_ref = {refname: dataset for refname, dataset in asset.list_datasets()}
            if not datasets_by_ref:
                self._console.print("No datasets for this asset.", style=Style(color="red"))
                return None, None

        if len(datasets_by_ref) == 1:
            refname = list(datasets_by_ref.keys())[0]
            dataset = datasets_by_ref[refname]
            self._console.print(f"[cyan]Defaulting to dataset {dataset.name} ([magenta]{dataset.rid}[cyan])[/cyan]")
            return refname, dataset

        table = Table(
            Column("#", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
            Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
            Column("Refname", style=Style(italic=True, dim=True), ratio=2, overflow="fold"),
            Column("Description", style=Style(color="cyan"), ratio=5, overflow="fold"),
            Column("Labels", style=Style(color="green"), ratio=3, overflow="fold"),
            Column("Properties", style=Style(color="magenta"), ratio=4, overflow="fold"),
            title=f"Available Datasets ({len(datasets_by_ref)})",
            expand=True,
            box=ASCII,
        )
        dataset_pairs = []
        for idx, refname in enumerate(datasets_by_ref):
            dataset = datasets_by_ref[refname]
            dataset_pairs.append((refname, dataset))
            table.add_row(
                str(idx),
                dataset.name,
                refname,
                dataset.description or "-",
                _render_labels(dataset.labels),
                _render_properties(dataset.properties),
            )

        self._render_table(table)

        while True:
            idx = IntPrompt.ask("Select a dataset #")
            if 0 <= idx < len(dataset_pairs):
                refname, dataset = dataset_pairs[idx]
                self._console.print(f"Selected dataset: '{dataset.name}' ({dataset.rid})", style=Style(color="magenta"))
                return refname, dataset
            else:
                self._console.print(
                    f"Please enter a number between 0 and {len(dataset_pairs) - 1}.", style=Style(color="red")
                )

            if Confirm.ask("See dataset table again?", default=False, show_default=True):
                self._render_table(table)

    def _select_channels(self, dataset: Dataset) -> list[Channel]:
        with self._console.status("Fetching channels…", spinner="dots"):
            all_channels = list(dataset.search_channels())
            self._console.print(
                f"[dim]There are {len(all_channels)} total channels in dataset[/dim] [bold]{dataset.name}[/bold]."
            )
            self._console.print(
                "See the dataset page to see available channels",
                style=Style(link=dataset.nominal_url, color="blue"),
            )

        while True:
            substrings = Prompt.ask("Enter exact channel substrings, separated by a comma [* for all]").strip()
            subqueries = [subquery.strip() for subquery in substrings.split(",")]
            new_channels = []
            if not subqueries:
                self._console.print(
                    "No patterns provided! Try again...",
                    style=Style(bold=True, color="yellow"),
                )
                continue
            elif "*" in subqueries:
                self._console.print(
                    "'*' provided-- selecting all channels!",
                    style=Style(color="magenta"),
                )
                new_channels = all_channels
            else:
                with self._console.status("Searching channels…", spinner="dots"):
                    new_channels = [
                        channel
                        for subquery in subqueries
                        for channel in dataset.search_channels(exact_match=[subquery])
                    ]

            if not new_channels:
                self._console.print(
                    "No channels found matching query...",
                    style=Style(bold=True, color="red"),
                )
                continue

            if Confirm.ask(
                f"{len(new_channels)} channel(s) selected! View channels?", default=False, show_default=True
            ):
                self._display_channels(new_channels)

            if Confirm.ask("Would you like to edit the list of channels?", default=False, show_default=True):
                edited_lines = click.edit(text="\n".join(sorted([ch.name for ch in new_channels])))
                if edited_lines is None:
                    self._console.print("No channels selected... restarting", style=Style(color="yellow"))
                    continue

                edited_channels = set([line for line in edited_lines.splitlines() if line])
                new_channels = [channel for channel in new_channels if channel.name in edited_channels]

            if len(new_channels) > 500:
                self._console.print(
                    f"{len(new_channels)} channels selected! Too many (>500) channels results in slow exports!",
                    style=Style(bold=True, color="red"),
                )

            if Confirm.ask(f"Are you sure you want to proceed with {len(new_channels)} channel(s)?"):
                return new_channels

    def _display_channels(self, channels: List[Channel]) -> None:
        if not channels:
            self._console.print("No channels...", style=Style(color="yellow"))
            return

        table = Table(
            Column("Name", style=Style(color="white", bold=True), ratio=4, overflow="fold"),
            Column("Description", style=Style(color="cyan"), ratio=3, overflow="fold"),
            Column("Data Type", style=Style(color="green"), ratio=2, overflow="fold"),
            Column("Unit", style=Style(color="magenta"), ratio=2, overflow="fold"),
            title=f"Channels ({len(channels)})",
            expand=True,
            box=ASCII,
        )
        for ch in sorted(channels, key=lambda ch: ch.name):
            table.add_row(ch.name, ch.description or "-", ch.data_type.value if ch.data_type else "-", ch.unit or "-")

        self._render_table(table)

    def _select_bounds(self) -> tuple[datetime.datetime, datetime.datetime]:
        time_option = Prompt.ask(
            "Choose an option for providing time bounds for download",
            choices=["event", "run", "custom"],
            show_choices=True,
        ).strip()
        if time_option == "event":
            start, end = self._select_bounds_for_event()
        elif time_option == "run":
            start, end = self._select_bounds_for_run()
        else:
            start, end = self._select_custom_bounds()

        start, end = self._edit_window_loop(start, end)
        return start, end

    def _select_bounds_for_event(self) -> tuple[datetime.datetime, datetime.datetime]:
        # Request event from user
        event = self._ask_event_by_rid()

        # If event has no duration, ask for a duration
        event_duration = datetime.timedelta(seconds=event.duration / 1e9)
        if event_duration.total_seconds() == 0:
            event_duration = self._ask_duration("Selected event has no duration! Enter duration")

        start = datetime.datetime.fromtimestamp(event.start / 1e9, tz=datetime.timezone.utc)
        return start, start + event_duration

    def _ask_event_by_rid(self) -> Event:
        while True:
            # Get event rid
            event_rid = Prompt.ask("Enter event rid (copy + paste from nominal)").strip()

            try:
                # Silence warnings about picosecond resolution
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    return self._client.get_event(event_rid)
            except Exception as ex:
                self._console.print(f"Failed to get event... try again!\n{ex}", style=Style(color="red"))
                continue

    def _ask_run_by_rid(self) -> Run:
        while True:
            # Get run rid
            run_rid = Prompt.ask("Enter run rid (copy + paste from nominal)").strip()

            try:
                return self._client.get_run(run_rid)
            except Exception as ex:
                self._console.print(f"Failed to get run... try again!\n{ex}", style=Style(color="red"))
                continue

    def _select_bounds_for_run(self) -> tuple[datetime.datetime, datetime.datetime]:
        # Request run from user
        run = self._ask_run_by_rid()

        start_ns = run.start
        start_timestamp = datetime.datetime.fromtimestamp(start_ns / 1e9, tz=datetime.timezone.utc)

        # If run has no end time, ask for a duration
        end_ns = run.end
        end_timestamp = None
        if end_ns is None:
            end_timestamp = self._ask_utc_timestamp(
                "Run has no end! Provide end timestamp (UTC)", default=datetime.datetime.now().isoformat()
            )
        else:
            end_timestamp = datetime.datetime.fromtimestamp(end_ns / 1e9, tz=datetime.timezone.utc)

        return start_timestamp, end_timestamp

    def _select_custom_bounds(self) -> tuple[datetime.datetime, datetime.datetime]:
        start = self._ask_utc_timestamp("Enter start timestamp (UTC), e.g. 2025-09-03T10:15:00Z")
        end = self._ask_utc_timestamp("Enter end timestamp (UTC)")
        return start, end

    def _edit_window_loop(
        self, start: datetime.datetime, end: datetime.datetime
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """Show current start/end (UTC), ask if user wants to edit.
        If yes: prompt for new start then end; blank keeps current.
        Loop until user is satisfied and end > start.
        """
        while True:
            seconds = (end - start).total_seconds()
            self._console.print(
                f"Bounds preview ({seconds} seconds):\nStart (UTC): {start.isoformat()}\nEnd   (UTC): {end.isoformat()}"
            )

            if start > end:
                self._console.print("End must be after start. Please adjust!", style=Style(color="red"))
            elif not Confirm.ask("Edit timestamps?", default=False, show_default=True):
                return start, end

            # Start edit (blank keeps)
            start = self._ask_utc_timestamp("Enter new START (UTC)", default=start.isoformat())
            end = self._ask_utc_timestamp("Enter new END (UTC)", default=end.isoformat())

            if start > end:
                continue
            elif not Confirm.ask(
                f"Parsed bounds as: [{start.isoformat()}, {end.isoformat()}]. Edit bounds?",
                default=False,
                show_default=True,
            ):
                return start, end

    def _ask_utc_timestamp(self, prompt: str, default: Any = ...) -> datetime.datetime:
        while True:
            raw_ts = Prompt.ask(prompt, default=default, show_default=True)
            parsed_ts = _parse_utc_ts(raw_ts)
            if parsed_ts:
                return parsed_ts

            self._console.print(
                "Invalid timestamp. Try values like '2025-09-03T10:15:00Z' or '2025-09-03 10:15:00' (UTC).",
                style=Style(color="red"),
            )

    def _ask_duration(self, prompt: str, default: Any = ...) -> datetime.timedelta:
        while True:
            raw_duration = Prompt.ask(prompt, default=default, show_default=True)
            normalized_duration = _normalize_duration_text(raw_duration)
            td = pd.to_timedelta(normalized_duration, errors="coerce")
            if not pd.isna(td):
                return datetime.timedelta(seconds=float(td.total_seconds()))

            self._console.print(
                "Invalid duration. Try '5m', '2 hours 30 minutes', '90s', or '00:05:00'.",
                style=Style(color="red"),
            )

    def download(self, channels_per_request: int, points_per_file: int) -> None:
        # Select asset to download from
        asset = self._select_asset()
        if not asset:
            logger.error("No asset selected! Exiting...")
            return

        # Select dataset to download from
        refname, dataset = self._select_dataset(asset)
        if dataset is None or refname is None:
            logger.error("No dataset selected! Exiting...")
            return

        # get tags from dataset & asset combo
        scope_tags = None
        raw_asset = self._client._clients.assets.get_assets(self._client._clients.auth_header, [asset.rid])[asset.rid]
        for raw_datascope in raw_asset.data_scopes:
            if raw_datascope.data_scope_name == refname:
                scope_tags = raw_datascope.series_tags
                break
        if scope_tags is None:
            logger.error("Failed to retrieve datascope details for refname %s", refname)
            return

        # Select channels (exact-name matching with iterative queries) to download
        channels = self._select_channels(dataset)
        if not channels:
            logger.error("No channels selected! Exiting...")
            return

        # Select output directory to download data into
        out_dir: pathlib.Path = click.prompt(
            "Enter download directory:",
            type=click.Path(file_okay=False, dir_okay=True, resolve_path=True, path_type=pathlib.Path),
            default="./out",
            show_default=True,
        )
        if not out_dir.exists():
            self._console.print(f"Creating output directory '{out_dir}'")
            out_dir.mkdir(parents=True, exist_ok=True)

        # Select time bounds to download data from
        start, end = self._select_bounds()

        # 6) Make sure user is fully aware of what they are about to download
        seconds_to_dl = (end - start).total_seconds()
        if not Confirm.ask(
            f"About to download {seconds_to_dl} seconds of data from {len(channels)} channels. Are you sure?"
        ):
            self._console.print("OK! Exiting...")
            return

        # 7) Download
        dataset_prefix = dataset.rid.split(".")[-1]
        exporter = PolarsExportHandler(
            self._client, points_per_dataframe=points_per_file, channels_per_request=channels_per_request
        )
        with self._console.status("Downloading...", spinner="bouncingBar"):
            for idx, df in enumerate(
                exporter.export(
                    channels, int(start.timestamp() * 1e9), int(end.timestamp() * 1e9), scope_tags, join_batches=True
                )
            ):
                out_path = self.write_dataframe(df, idx, out_dir, dataset_prefix)
                logger.debug("Wrote dataframe %d to %s", idx, out_path)

        self.print_instructions(out_dir, dataset_prefix)

    @abc.abstractmethod
    def write_dataframe(
        self, df: pl.DataFrame, part_idx: int, directory: pathlib.Path, prefix: str
    ) -> pathlib.Path: ...

    @abc.abstractmethod
    def print_instructions(self, directory: pathlib.Path, prefix: str) -> None: ...

    def _print_code(self, code: str, title: str, language: str) -> None:
        panel = Panel(
            Syntax(code.strip(), language, theme="monokai", padding=1), title=title, padding=1, box=HORIZONTALS
        )
        self._console.print(panel)


class CsvDownloader(DataDownloader):
    def write_dataframe(self, df: pl.DataFrame, part_idx: int, directory: pathlib.Path, prefix: str) -> pathlib.Path:
        out_path = directory / f"{prefix}-part_{part_idx}.csv"
        df.write_csv(out_path)
        return out_path

    def print_instructions(self, directory: pathlib.Path, prefix: str) -> None:
        # MATLAB 2023b+: use tabularTextDatastore + readall
        new_code = f'''
data_dir = fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.csv" ...
);
ds = tabularTextDatastore(data_dir);
nominal_data = sortrows(readall(ds), "timestamp");'''

        # MATLAB 2019b+: dir + readtable + vertcat
        old_code = f'''
files = dir(fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.csv" ...
));

nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat(c{{:}}), ...
        cellfun( ...
            @readtable, ...
            fullfile({{files.folder}}, {{files.name}}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);'''

        self._print_code(old_code, "Load downloaded CSV data into MATLAB 2019b+", "matlab")
        self._print_code(new_code, "Load downloaded CSV data into MATLAB 2023b+", "matlab")


class ParquetDownloader(DataDownloader):
    def write_dataframe(self, df: pl.DataFrame, part_idx: int, directory: pathlib.Path, prefix: str) -> pathlib.Path:
        out_path = directory / f"{prefix}-part_{part_idx}.parquet"
        df.write_parquet(out_path, compression="snappy")
        return out_path

    def print_instructions(self, directory: pathlib.Path, prefix: str) -> None:
        new_code = f'''
data_dir = fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.parquet" ...
);
nominal_data = sortrows(readall(parquetDatastore(data_dir)), "timestamp");'''

        old_code = f'''
files=dir(fullfile( ...
    "{str(directory)}", ...
    "{prefix}-part_*.parquet" ...
));
nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat (c{{:}}), ...
        cellfun( ...
            @parquetread, ...
            fullfile({{files.folder}}, {{files.name}}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);'''

        self._print_code(old_code, "Load downloaded parquet data into MATLAB 2019b+", "matlab")
        self._print_code(new_code, "Load downloaded parquet data into MATLAB 2023b+", "matlab")


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
    # Keeping logging and associated console alive for duration of the CLI
    console = Console()
    _listener = configure_rich_logging(console, ctx.obj["log_level"])

    match format:
        case "parquet":
            ParquetDownloader(client, console).download(channels_per_request, points_per_file)
        case "csv":
            CsvDownloader(client, console).download(channels_per_request, points_per_file)
        case _:
            raise ValueError(f"Unknown format: {format}")


if __name__ == "__main__":
    download_cmd()
