import logging
from pathlib import Path
from typing import Tuple, Union

import click
import pandas as pd

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import NominalClient

logger = logging.getLogger(__name__)


def read_mis_csv(mis_path: Path) -> pd.DataFrame:
    """Read the MIS CSV and return a dictionary of channel names and their descriptions and units."""
    df = pd.read_csv(mis_path)
    return df


def read_mis_excel(mis_path: Path, sheet: str) -> pd.DataFrame:
    """Read the MIS Excel file and return a dictionary of channel names and their descriptions and units."""
    try:
        df = pd.read_excel(mis_path, sheet_name=sheet)
    except ValueError as e:
        raise click.UsageError(f"Error reading Excel file: {e}")
    return df


def update_channels(
    mis_data: dict[str, Tuple[str, str]], dataset_rid: str, client: NominalClient
) -> None:
    """Update channels using dictionary lookup instead of nested loops."""
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
    help="""
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
@click.argument(
    "mis_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option("--dataset-rid", type=str, required=True)
@click.option(
    "--sheet",
    type=str,
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
@client_options
@global_options
def process(mis_path: Path, dataset_rid: str, sheet: str, client: NominalClient) -> None:
    """Processes an MIS file and updates channel descriptions and units."""
    click.echo(f"Validating MIS file: {mis_path}")

    is_excel = str(mis_path).endswith((".xlsx", ".xls"))
    if is_excel and not sheet:
        raise click.UsageError("You must provide --sheet when using an Excel file.")

    # Read unique units from the MIS file
    if is_excel:
        mis_data = read_mis_excel(mis_path, sheet)
    else:
        mis_data = read_mis_csv(mis_path)
    formatted_mis_data = {
        row["Channel"]: (row["Description"], row["UCUM Unit"]) for _, row in mis_data.iterrows()
    }
    update_channels(formatted_mis_data, dataset_rid, client)


@mis_cmd.command(
    name="validate", help="Validate units in an MIS file against available units in Nominal."
)
@click.argument(
    "mis_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--sheet",
    type=str,
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
@click.pass_context
@client_options
@global_options
def check_units(ctx: click.Context, mis_path: Path, sheet: str, client: NominalClient) -> None:
    """Validates the units in an MIS file against the available units in Nominal."""
    click.echo(f"Validating MIS file: {mis_path}")

    is_excel = str(mis_path).endswith((".xlsx", ".xls"))
    if is_excel and not sheet:
        raise click.UsageError("You must provide --sheet when using an Excel file.")

    # Read unique units from the MIS file
    if is_excel:
        mis_data = read_mis_excel(mis_path, sheet)
    else:
        mis_data = read_mis_csv(mis_path)

    mis_units = set(mis_data.iloc[:, 2].unique())

    # Get available units from Nominal
    try:
        nominal_units_list = client.get_all_units()
        nominal_units = {unit.symbol for unit in nominal_units_list}
        click.echo(
            f"Found {len(nominal_units)} available units in Nominal for profile '{client.get_user()}'."
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
@click.option(
    "--csv-name",
    type=click.Path(dir_okay=False, writable=True),
    help="Output CSV file path (defaults to stdout).",
)
@click.option(
    "--excel-name",
    type=click.Path(dir_okay=False, writable=True),
    help="Output Excel file path.",
)
@client_options
@global_options
def list_units(csv_name: str, excel_name: Union[str, None], client: NominalClient) -> None:
    """List all available units in Nominal."""
    units = client.get_all_units()
    sorted_units = sorted(units, key=lambda u: u.symbol)

    # Convert Unit objects to DataFrame
    units_data = [{"UCUM": unit.symbol, "Unit Name": unit.name} for unit in sorted_units]
    df = pd.DataFrame(units_data)

    if csv_name:
        # Convert Unit objects to DataFrame
        df.to_csv(csv_name, index=False)
        click.echo(f"Unit list successfully written to {csv_name}")
    elif excel_name:
        df.to_excel(excel_name, index=False)
        click.echo(f"Unit list successfully written to {excel_name}")
    else:
        # print to stdout for piping and grep
        click.echo(df.to_csv(index=False))
