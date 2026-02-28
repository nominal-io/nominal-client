from __future__ import annotations

import shutil
from pathlib import Path

import click

import nominal
from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.core.flow import FlowBuilder, FlowBuilderError, StepInfo

NOMINAL_LOGO = [
    "            \u2588\u2588\u2588",
    "           \u2588\u2588\u2588\u2588\u2588",
    "         \u2588\u2588\u2588\u2588 \u2588\u2588\u2588\u2588",
    "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588    \u2588\u2588\u2588\u2588\u2588",
    "         \u2588\u2588\u2588\u2588 \u2588\u2588\u2588\u2588",
    "           \u2588\u2588\u2588\u2588\u2588",
    "            \u2588\u2588\u2588",
]

TIMESTAMP_TYPES = [
    "iso_8601",
    "epoch_days",
    "epoch_hours",
    "epoch_minutes",
    "epoch_seconds",
    "epoch_milliseconds",
    "epoch_microseconds",
    "epoch_nanoseconds",
]


def _separator() -> None:
    """Print a horizontal line across the terminal width."""
    term_width = shutil.get_terminal_size((80, 24)).columns
    click.echo("\u2500" * term_width)


def _prompt_with_separator(prompt_text: str, **kwargs):
    """Show a prompt with separator lines above and below."""
    _separator()
    result = click.prompt(prompt_text, **kwargs)
    _separator()
    return result


def _abbreviate_path(path: Path) -> str:
    home = Path.home()
    try:
        return "~/" + str(path.relative_to(home))
    except ValueError:
        return str(path)


def _render_header(
    version: str,
    display_name: str,
    cwd: str,
    workspace_name: str,
) -> str:
    term_width = shutil.get_terminal_size((80, 24)).columns
    inner_width = max(term_width - 2, 58)

    title = f"Nominal CLI v{version}"
    title_styled = click.style(title, fg="cyan", bold=True)
    top_border = (
        "\u256d\u2500\u2500\u2500 "
        + title_styled
        + " "
        + "\u2500" * (inner_width - len(title) - 5)
        + "\u256e"
    )
    bottom_border = "\u2570" + "\u2500" * inner_width + "\u256f"

    def pad(text: str, visible_len: int | None = None) -> str:
        vlen = visible_len if visible_len is not None else len(text)
        padding = inner_width - vlen - 1
        if padding < 0:
            # Truncate text to fit within the box
            overflow = -padding
            text = text[: len(text) - overflow - 3] + "..."
            padding = 0
        return "\u2502 " + text + " " * max(padding, 0) + "\u2502"

    blank = pad("")

    welcome = f"Welcome back {display_name}!"
    welcome_styled = click.style(welcome, bold=True)

    logo_styled = [click.style(line, fg="cyan") for line in NOMINAL_LOGO]
    logo_visible_lens = [len(line) for line in NOMINAL_LOGO]

    info_text = f"Nominal \u00b7 {workspace_name}"
    info_styled = click.style(info_text, dim=True)

    lines = [
        top_border,
        blank,
        pad(welcome_styled, len(welcome)),
        blank,
    ]

    mid = len(NOMINAL_LOGO) // 2
    for i, (styled, vlen) in enumerate(zip(logo_styled, logo_visible_lens)):
        if i == mid:
            cwd_suffix = f"   {cwd}"
            combined = styled + cwd_suffix
            combined_vlen = vlen + len(cwd_suffix)
            lines.append(pad(combined, combined_vlen))
        else:
            lines.append(pad(styled, vlen))

    lines.extend([
        blank,
        pad(info_styled, len(info_text)),
        bottom_border,
    ])

    return "\n".join(lines)


def _handle_select_step(builder: FlowBuilder, info: StepInfo) -> None:
    for i, option in enumerate(info.options, start=1):
        click.echo(f"  {i}. {option.title}")
    click.echo()

    choice = _prompt_with_separator(
        "Choose an option",
        type=click.IntRange(1, len(info.options)),
    )
    selected_title = info.options[choice - 1].title
    click.secho(f"  -> {selected_title}", fg="green")
    builder.select(selected_title)


def _handle_form_step(builder: FlowBuilder, info: StepInfo) -> None:
    values: dict[str, str] = {}
    for field_info in info.fields:
        label = field_info.label
        required_marker = click.style(" *", fg="red") if field_info.is_required else ""
        prompt_text = f"  {label}{required_marker}"

        default = field_info.placeholder if field_info.placeholder else None

        if field_info.is_required:
            value = click.prompt(prompt_text, default=default)
        else:
            value = click.prompt(prompt_text, default=default or "", show_default=False)
            if value == "":
                continue

        values[label] = value

    builder.fill(values)


def _walk_flow_interactively(builder: FlowBuilder) -> None:
    step_num = 0
    while True:
        info = builder.current_step_info()
        step_num += 1

        click.echo()
        click.secho(f"Step {step_num}: {info.title}", fg="cyan", bold=True)
        if info.description:
            click.echo(f"  {info.description}")
        click.echo()

        if info.step_type == "select":
            _handle_select_step(builder, info)
        elif info.step_type == "form":
            _separator()
            _handle_form_step(builder, info)
            _separator()
        elif info.step_type == "upload":
            break
        else:
            raise click.ClickException(f"Unknown step type: {info.step_type}")


def _display_result(result) -> None:
    click.echo()
    click.secho("Flow completed successfully!", fg="green", bold=True)
    click.echo()
    click.echo(f"  Dataset File: {result.dataset_file.id}")
    if result.run is not None:
        click.echo(f"  Run RID:      {result.run.rid}")
        click.echo(f"  Run Name:     {result.run.name}")


@click.group(name="flow")
def flow_cmd() -> None:
    """Ingest flow commands."""
    pass


@flow_cmd.command(name="run")
@client_options
@global_options
def run_cmd(client: NominalClient) -> None:
    """Interactively run an ingest flow."""

    # Fetch user and workspace for the header
    try:
        user = client.get_user()
        workspace = client.get_workspace()
    except Exception:
        user = None
        workspace = None

    version = nominal.__version__
    display_name = user.display_name if user else "user"
    cwd = _abbreviate_path(Path.cwd())
    ws_name = (workspace.display_name or workspace.id) if workspace else "unknown"

    header = _render_header(version, display_name, cwd, ws_name)
    click.echo(header)

    # Prompt for ingest flow RID
    rid = _prompt_with_separator("Enter the ingest flow RID")

    # Initialize the FlowBuilder
    try:
        builder = FlowBuilder(rid, client=client)
    except Exception as e:
        raise click.ClickException(f"Failed to load ingest flow: {e}")

    # Walk through the flow interactively
    try:
        _walk_flow_interactively(builder)
    except FlowBuilderError as e:
        raise click.ClickException(str(e))

    # Handle the upload step
    info = builder.current_step_info()
    click.echo()
    click.secho(f"Upload: {info.title}", fg="cyan", bold=True)
    if info.description:
        click.echo(f"  {info.description}")
    click.echo()

    _separator()
    file_path = click.prompt(
        "  File path",
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    )
    timestamp_column = click.prompt("  Timestamp column name")
    timestamp_type = click.prompt(
        "  Timestamp type",
        type=click.Choice(TIMESTAMP_TYPES),
    )

    default_run_name = Path(file_path).stem
    run_name = click.prompt("  Run name", default=default_run_name)
    _separator()

    click.echo()
    click.secho("Uploading...", fg="yellow")

    try:
        flow = builder.build()
        result = flow.add_tabular_data(
            file_path,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            run_name=run_name,
        )
    except FlowBuilderError as e:
        raise click.ClickException(str(e))
    except FileNotFoundError as e:
        raise click.ClickException(f"File not found: {e}")

    _display_result(result)
