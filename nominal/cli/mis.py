from __future__ import annotations

import logging
from pathlib import Path

import click
import pandas as pd
import tabulate

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import NominalClient

logger = logging.getLogger(__name__)


def read_mis(mis_path: Path, sheet: str | None) -> pd.DataFrame:
    """Read the MIS file (CSV or Excel) and return a dataframe containing its contents

    Args:
        mis_path: Path to the '.xlsx', '.xls', or '.csv' file to read
        sheet: Name of the excel sheet to read containing the MIS data
    """
    is_excel = str.lower(mis_path.suffix) in (".xlsx", ".xls")
    is_csv = str.lower(mis_path.suffix) == ".csv"
    if is_excel:
        excel_file = pd.ExcelFile(mis_path)
        if sheet is None:
            if len(excel_file.sheet_names) > 1:
                sheet_names_str = [str(name) for name in excel_file.sheet_names]
                raise ValueError(
                    f"Excel file has multiple sheets ({', '.join(sheet_names_str)}). "
                    "Please specify which sheet to use with the --sheet option."
                )
            sheet = str(excel_file.sheet_names[0])
            logger.info("Using sheet: %s", sheet)
        df = excel_file.parse(sheet)
    elif is_csv:
        df = pd.read_csv(mis_path)
    else:
        raise ValueError(f"Error parsing MIS file: {mis_path}, only accepts CSV and Excel files.")
    logger.info("Read MIS file: %s", mis_path)

    # standardize and validate column names
    df.columns = df.columns.str.lower()
    required_columns = ["channel"]
    optional_columns = ["description", "ucum unit"]

    if any([col not in df.columns for col in required_columns]):
        raise ValueError(f"MIS file must have columns: {required_columns}")

    for col in optional_columns:
        if col not in df.columns:
            df[col] = None

    return df[[*required_columns, *optional_columns]]


def update_channels(
    mis_data: pd.DataFrame, dataset_rid: str, override_channel_info: bool, client: NominalClient
) -> None:
    """The main function for updating channels in a dataset.

    Args:
        mis_data: The dataframe containing the MIS data
        dataset_rid: The RID of the dataset to update
        override_channel_info: will override if present
        client: The Nominal client
    """
    dataset = client.get_dataset(dataset_rid)
    channel_list = dataset.get_channels()
    channel_map = {channel.name: channel for channel in channel_list}

    for _, channel_name, description, unit in mis_data.itertuples():
        channel = channel_map.get(channel_name)
        if channel:
            if (channel.description is not None and description is not None and channel.description != description) or (
                channel.unit is not None and channel.unit != unit
            ):
                if override_channel_info:
                    logger.warning(
                        "Channel %s description %s and units %s will be overridden by input: %s, %s",
                        channel.name,
                        channel.description,
                        channel.unit,
                        description,
                        unit,
                    )
                    channel.update(description=description, unit=unit)
                else:
                    logger.warning(
                        "Channel %s description %s and units %s does not match input: %s, %s. Skipping update.",
                        channel.name,
                        channel.description,
                        channel.unit,
                        description,
                        unit,
                    )
            else:
                # TODO (sean): We should use the bulk API for this.
                logger.info("Updated channel %s with description: %s and unit: %s", channel.name, description, unit)
                channel.update(description=description, unit=unit)
        else:
            logger.warning("Channel %s not found in dataset %s", channel_name, dataset_rid)


def get_display_only_units(df: pd.DataFrame, client: NominalClient) -> set[str] | None:
    """Validate units in an MIS file against available units in Nominal.

    Args:
        df: The dataframe containing the MIS data
        client: The Nominal client
    Returns:
        A set of units that are not valid
    """
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
    help="The sheet to use in the Excel file if parsing direct from Excel. Only needed if there are multiple sheets.",
)
@click.option(
    "--override-channel-info",
    is_flag=True,
    help="If channel is already present and has different description/units information, overwrite.",
)
@client_options
@global_options
def process(
    mis_path: Path, dataset_rid: str, sheet: str | None, override_channel_info: bool, client: NominalClient
) -> None:
    """Processes an MIS file and updates channel descriptions and units."""
    logger.info("Validating MIS file: %s", mis_path)
    mis_data = read_mis(mis_path, sheet)
    logger.info("Updating channels: %s", mis_data)
    update_channels(mis_data, dataset_rid, override_channel_info, client)
    logger.info("Channels updated.")


@mis_cmd.command(name="validate", help="Validate units in an MIS file against available units in Nominal.")
@click.argument(
    "mis_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--sheet",
    required=False,
    help="The sheet to use in the Excel file if parsing direct from Excel. Only needed if there are multiple sheets.",
)
@client_options
@global_options
def check_units(mis_path: Path, sheet: str | None, client: NominalClient) -> None:
    """Validates the units in an MIS file against the available units in Nominal."""
    logger.info("Validating MIS file: %s", mis_path)
    mis_data = read_mis(mis_path, sheet)
    display_only_units = get_display_only_units(mis_data, client)

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
    type=click.Choice(
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
