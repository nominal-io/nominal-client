from __future__ import annotations

import logging
import typing
from pathlib import Path

import click
import pandas as pd
import tabulate

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import NominalClient

logger = logging.getLogger(__name__)


def read_mis(mis_path: Path, sheet: str | None) -> pd.DataFrame:
    """Read the MIS file and return a dictionary of channel names and their descriptions and units."""
    is_excel = str.lower(mis_path.suffix) in (".xlsx", ".xls")

    # For Excel files, automatically use the single sheet if only one exists
    if is_excel and sheet is None:
        excel_file = pd.ExcelFile(mis_path)
        sheet_names = excel_file.sheet_names
        if len(sheet_names) > 1:
            raise ValueError(
                f"Excel file has multiple sheets ({', '.join(sheet_names)}). "
                "Please specify which sheet to use with the --sheet option."
            )
        sheet = sheet_names[0]
        logger.info(f"Using sheet: {sheet}")

    try:
        if is_excel:
            df = pd.read_excel(mis_path, sheet_name=sheet)
        else:
            df = pd.read_csv(mis_path)
    except Exception as e:
        logger.error("Error parsing MIS file: %s. Only accepts CSV and Excel files.", mis_path, exc_info=e)
        raise ValueError(f"Error parsing MIS file: {mis_path}") from e

    # standardize and validate column names
    df.columns = df.columns.str.lower()
    selected_columns = ["channel", "description", "ucum unit"]
    if not all(col in df.columns for col in selected_columns):
        raise ValueError("MIS file must have columns: Channel, Description, UCUM Unit")

    return df[selected_columns]


def update_channels(mis_data: pd.DataFrame, dataset_rid: str, client: NominalClient) -> None:
    """Update channels using dictionary lookup instead of nested loops."""
    dataset = client.get_dataset(dataset_rid)
    channel_list = dataset.get_channels()
    channel_map = {channel.name: channel for channel in channel_list}

    for _, channel_name, description, unit in mis_data.itertuples():
        channel = channel_map.get(channel_name)
        if channel:
            channel.update(description=description, unit=unit)
        else:
            logger.warning(f"Channel {channel_name} not found in dataset {dataset_rid}")


def validate_units(df: pd.DataFrame, client: NominalClient) -> set[str] | None:
    """Validate units in an MIS file against available units in Nominal."""
    mis_units = set(df.loc[:, "ucum unit"].unique())

    # Get available units from Nominal
    nominal_units_list = client.get_all_units()
    nominal_units = {unit.symbol for unit in nominal_units_list}

    # Find display only units
    display_only_units = mis_units - nominal_units

    return display_only_units


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


@mis_cmd.command(name="process", help="Processes an MIS file and updates channel descriptions and units.")
@click.argument(
    "mis_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option("--dataset-rid", required=True, help="The RID of the dataset to update.")
@click.option(
    "--sheet",
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
@client_options
@global_options
def process(mis_path: Path, dataset_rid: str, sheet: str | None, client: NominalClient) -> None:
    """Processes an MIS file and updates channel descriptions and units."""
    logger.info("Validating MIS file: %s", mis_path)
    mis_data = read_mis(mis_path, sheet)
    logger.info("Updating channels: %s", mis_data)
    update_channels(mis_data, dataset_rid, client)
    logger.info("Channels updated.")


@mis_cmd.command(name="validate", help="Validate units in an MIS file against available units in Nominal.")
@click.argument(
    "mis_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--sheet",
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel.",
)
@client_options
@global_options
def check_units(mis_path: Path, sheet: str | None, client: NominalClient) -> None:
    """Validates the units in an MIS file against the available units in Nominal."""
    logger.info("Validating MIS file: %s", mis_path)
    mis_data = read_mis(mis_path, sheet)
    display_only_units = validate_units(mis_data, client)

    # Report results
    if not display_only_units:
        logger.info("All units in the MIS file are valid.")
    else:
        logger.warning("Found %s display only units in the MIS file:", len(display_only_units))
        for unit in sorted(list(display_only_units)):
            logger.warning("  - %s", unit)
        logger.warning(
            "The listed units will still show in Nominal but will not work with the "
            "'Unit Conversion' transform. You can use the 'list-units' command to see all available units.",
        )


@mis_cmd.command(name="list-units", help="List all available units in Nominal.")
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, resolve_path=True, path_type=Path),
    help="If provided, write the output to a file.",
)
@click.option(
    "-f",
    "--format",
    type=typing.Literal(
        [
            "table",
            "csv",
        ],
        case_sensitive=True,
    ),
    default="table",
    show_default=True,
    help="The format to represent the data as",
)
@client_options
@global_options
def list_units(output: Path | None, format: str, client: NominalClient) -> None:
    """List all available units in Nominal."""
    units = client.get_all_units()
    sorted_units = sorted(units, key=lambda u: u.symbol)

    # Convert Unit objects to DataFrame
    units_data = [{"UCUM": unit.symbol, "Unit Name": unit.name} for unit in sorted_units]
    df = pd.DataFrame(units_data)

    output_str = _data_to_string(df, format)

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(output_str)
        click.secho(f"Unit list successfully written to {output}", fg="cyan")
    else:
        click.echo(output_str)


def _data_to_string(data: pd.DataFrame, format: str) -> str:
    if format == "csv":
        return data.to_csv(index=False)
    elif format == "table":
        return tabulate.tabulate(data.values.tolist(), headers=data.columns.tolist())
    else:
        raise ValueError(f"Expected format to be one of csv, table, received {format}")
