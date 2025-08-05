import csv
from pathlib import Path
from typing import Tuple, Union

import click

from nominal.core import NominalClient


def process_mis_csv(mis_path: Path) -> dict[str, Tuple[str, str]]:
    """Read the MIS CSV and return a dictionary of channel names and their descriptions and units."""
    processed_data = {}
    with open(mis_path, "r", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # Skip header
        except StopIteration:
            return {}  # Handle empty file

        for row in reader:
            # Ensure row has enough columns before processing
            if len(row) >= 3:
                # Strip whitespace from all fields to prevent validation issues and handle empty lines
                channel_name = row[0].strip()
                description = row[1].strip()
                unit = row[2].strip()
                if channel_name:
                    processed_data[channel_name] = (description, unit)
    return processed_data


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


@click.group(
    help="""\
This CLI processes an MIS and turns it into unit assignments and channel descriptions on a dataset.

MIS must be in the format of a CSV with the following columns:

\b
- Channel Name
- Channel Description
- UCUM Unit

Example:

Channel Name,Channel Description,UCUM Unit
RPM, Engine RPM, rpm
ECT1, Engine Coolant Temperature Main, Cel
"""
)
def mis_cmd() -> None:
    """MIS processing and validation commands."""
    pass


@mis_cmd.command(name="process", help="Processes an MIS file and updates channel descriptions and units.")
@click.argument("mis_path", type=click.Path(exists=True))
@click.option("--dataset-rid", type=str, required=True)
@click.option("--profile", type=str, required=True)
def process(mis_path: Path, dataset_rid: str, profile: str) -> None:
    """Processes an MIS file and updates channel descriptions and units."""
    mis_data = process_mis_csv(mis_path)
    update_channels(mis_data, dataset_rid, profile)


@mis_cmd.command(name="validate", help="Validate units in an MIS file against available units in Nominal.")
@click.argument("mis_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", type=str, required=True, help="The profile to use for authentication.")
def check_units(mis_path: str, profile: str) -> None:
    """Validates the units in an MIS file against the available units in Nominal."""
    click.echo(f"Validating MIS file: {mis_path}")

    # Read unique units from the MIS file
    mis_data = process_mis_csv(Path(mis_path))
    mis_units = {unit for _, (_, unit) in mis_data.items() if unit}

    # Get available units from Nominal
    client = NominalClient.from_profile(profile)
    try:
        nominal_units_list = client.get_all_units()
        nominal_units = {unit.symbol for unit in nominal_units_list}
        click.echo(f"Found {len(nominal_units)} available units in Nominal for profile '{profile}'.")
    except Exception as e:
        click.echo(click.style(f"Error fetching units from Nominal: {e}", fg="red"), err=True)
        return

    # Find invalid units
    invalid_units = mis_units - nominal_units

    # Report results
    if not invalid_units:
        click.echo(click.style("✓ All units in the MIS file are valid.", fg="green"))
    else:
        click.echo(click.style(f"\nFound {len(invalid_units)} invalid units in the MIS file:", fg="yellow"))
        for unit in sorted(list(invalid_units)):
            click.echo(f"  - {unit}")
        click.echo(
            click.style(
                "The listed units will still show in Nominal but will not work with the "
                "'Unit Conversion' transform. You can use the 'list-units' command to see all available units.",
                fg="red",
            )
        )
        # Exit with a non-zero code to indicate failure, useful for scripting
        raise click.exceptions.Exit(1)


@mis_cmd.command(name="list-units", help="List all available units in Nominal.")
@click.option("--profile", type=str, required=True, help="The profile to use for authentication.")
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(dir_okay=False, writable=True),
    help="Path to write the units to as a CSV file.",
)
def list_units(profile: str, csv_path: Union[str, None]) -> None:
    """List all available units in Nominal."""
    client = NominalClient.from_profile(profile)
    units = client.get_all_units()
    sorted_units = sorted(units, key=lambda u: u.symbol)

    if csv_path:
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Name"])
            for unit in sorted_units:
                writer.writerow([unit.symbol, unit.name])
        click.echo(f"Unit list successfully written to {csv_path}")
    else:
        click.echo(f"{'Symbol':<20} {'Name'}")
        click.echo("-" * 40)
        for unit in sorted_units:
            click.echo(f"{unit.symbol:<20} {unit.name}")
