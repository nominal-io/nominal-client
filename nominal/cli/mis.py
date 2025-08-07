import csv
import logging
from pathlib import Path
from typing import Tuple, Union

import click
import tabulate
import pandas as pd

from nominal.core import NominalClient

logger = logging.getLogger(__name__)


def read_mis_csv(mis_path: Path) -> dict[str, Tuple[str, str]]:
    """Read the MIS CSV and return a dictionary of channel names and their descriptions and units."""
    df = pd.read_csv(mis_path)
    return df


def read_mis_excel(mis_path: Path, sheet: str) -> dict[str, Tuple[str, str]]:
    """Read the MIS Excel file and return a dictionary of channel names and their descriptions and units."""
    try:
        df = pd.read_excel(mis_path, sheet_name=sheet)
    except ValueError as e:
        raise click.UsageError(f"Error reading Excel file: {e}")
    return df


def update_channels(mis_data: dict[str, Tuple[str, str]], dataset_rid: str, profile: str) -> None:
    """Update channels using dictionary lookup instead of nested loops."""
    client = NominalClient.from_profile(profile)
    dataset = client.get_dataset(dataset_rid)
    channel_list = dataset.get_channels()
    channel_map = {channel.name: channel for channel in channel_list}

    for channel_name, (description, unit) in mis_data.items():
        channel = channel_map.get(channel_name)
        if channel:
            channel.update(description=description, unit=unit)
        else:
            logger.warning(f"Channel {channel_name} not found in dataset {dataset_rid}")


@click.group(
    help="""\
This CLI processes an MIS and turns it into unit assignments and channel descriptions on a dataset.
MIS must be a CSV with the following columns: Channel, Description, UCUM Unit

Example:

Channel, Description, UCUM Unit \n
RPM, Engine RPM, rpm \n
ECT1, Engine Coolant Temperature Main, Cel \n
"""
)
def mis_cmd() -> None:
    """MIS processing and validation commands."""
    pass


@mis_cmd.command(
    name="process", help="Processes an MIS file and updates channel descriptions and units."
)
@click.argument("mis_path", type=click.Path(exists=True))
@click.option("--dataset-rid", type=str, required=True)
@click.option("--profile", type=str, required=True)
@click.option(
    "--sheet",
    type=str,
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
def process(mis_path: Path, dataset_rid: str, profile: str, sheet: str) -> None:
    """Processes an MIS file and updates channel descriptions and units."""
    click.echo(f"Validating MIS file: {mis_path}")

    is_excel = mis_path.endswith((".xlsx", ".xls"))
    if is_excel and not sheet:
        raise click.UsageError("You must provide --sheet when using an Excel file.")

    # Read unique units from the MIS file
    if is_excel:
        mis_data = read_mis_excel(Path(mis_path), sheet)
    else:
        mis_data = read_mis_csv(Path(mis_path))
    formatted_mis_data = {
        row["Channel"]: (row["Description"], row["UCUM Unit"]) for _, row in mis_data.iterrows()
    }
    update_channels(formatted_mis_data, dataset_rid, profile)


@mis_cmd.command(
    name="validate", help="Validate units in an MIS file against available units in Nominal."
)
@click.argument("mis_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", type=str, required=True, help="The profile to use for authentication.")
@click.option(
    "--sheet",
    type=str,
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
@click.pass_context
def check_units(ctx: click.Context, mis_path: str, profile: str, sheet: str) -> None:
    """Validates the units in an MIS file against the available units in Nominal."""
    click.echo(f"Validating MIS file: {mis_path}")

    is_excel = mis_path.endswith((".xlsx", ".xls"))
    if is_excel and not sheet:
        raise click.UsageError("You must provide --sheet when using an Excel file.")

    # Read unique units from the MIS file
    if is_excel:
        mis_data = read_mis_excel(Path(mis_path), sheet)
    else:
        mis_data = read_mis_csv(Path(mis_path))

    mis_units = set(mis_data.iloc[:, 2].unique())

    # Get available units from Nominal
    client = NominalClient.from_profile(profile)
    try:
        nominal_units_list = client.get_all_units()
        nominal_units = {unit.symbol for unit in nominal_units_list}
        click.echo(
            f"Found {len(nominal_units)} available units in Nominal for profile '{profile}'."
        )
    except Exception as e:
        click.secho(f"Error fetching units from Nominal: {e}", fg="red", err=True)
        return

    # Find invalid units
    invalid_units = mis_units - nominal_units

    # Report results
    if not invalid_units:
        click.secho("✓ All units in the MIS file are valid.", fg="green")
    else:
        logger.warning(f"Found {len(invalid_units)} invalid units in the MIS file:")
        for unit in sorted(list(invalid_units)):
            click.echo(f"  - {unit}")
        click.secho(
            "The listed units will still show in Nominal but will not work with the "
            "'Unit Conversion' transform. You can use the 'list-units' command to see all available units.",
            fg="red",
        )
        # Exit with a non-zero code to indicate failure, useful for scripting
        ctx.exit(1)


@mis_cmd.command(name="list-units", help="List all available units in Nominal.")
@click.option("--profile", type=str, required=True, help="The profile to use for authentication.")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(dir_okay=False, writable=True),
    help="Path to write the units to as a CSV file.",
)
@click.option(
    "--excel",
    "excel_path",
    type=str,
    required=False,
    help="Path to write the units to as an Excel file.",
)
def list_units(profile: str, csv_path: Union[str, None], excel_path: Union[str, None]) -> None:
    """List all available units in Nominal."""
    client = NominalClient.from_profile(profile)
    units = client.get_all_units()
    sorted_units = sorted(units, key=lambda u: u.symbol)

    units_data = [{"UCUM": unit.symbol, "Unit Name": unit.name} for unit in sorted_units]
    df = pd.DataFrame(units_data, columns=["UCUM", "Unit Name"])

    if csv_path:
        # Convert Unit objects to DataFrame
        df.to_csv(csv_path, index=False)
        click.echo(f"Unit list successfully written to {csv_path}")
    elif excel_path:
        df.to_excel(excel_path, index=False)
        click.echo(f"Unit list successfully written to {excel_path}")
    else:
        click.echo(tabulate.tabulate(sorted_units, headers=["UCUM", "Unit Name"], tablefmt="grid"))
