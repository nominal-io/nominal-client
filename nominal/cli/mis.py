"""This CLI processes an MIS and turns it into unit assignments and chennel descriptions on a dataset
MIS must be in the format of a CSV with the following columns:
- Channel Name
- Channel Description
- UCUM Unit
"""

import csv
from pathlib import Path
from typing import Tuple

import click

from nominal.core import NominalClient


def process_mis_csv(mis_path: Path) -> dict[str, Tuple[str, str]]:
    """Read the MIS CSV and return a dictionary of channel names and their descriptions and units.

    Args:
        mis_path: The path to the MIS CSV file containing channel information

    Returns:
        dict[str, Tuple[str, str]]: Dictionary mapping channel names to tuples of (description, unit)
    """
    with open(mis_path, "r") as f:
        reader = csv.reader(f)
        next(reader)
        return {row[0]: (row[1], row[2]) for row in reader}


class Channel_Updater:
    def __init__(self, dataset_rid: str, profile: str):
        self.dataset_rid = dataset_rid
        self.client = NominalClient.from_profile(profile)
        self.dataset = self.client.get_dataset(dataset_rid)
        self.channel_list = self.dataset.get_channels()
        self.channel_map = {channel.name: channel for channel in self.channel_list}

    def update_channels(self, mis_data: dict):
        """Update channels using dictionary lookup instead of nested loops."""
        for channel_name, (description, unit) in mis_data.items():
            channel = self.channel_map.get(channel_name)
            if channel:
                channel.update(description=description, unit=unit)


@click.group()
def mis_cmd():
    pass


@mis_cmd.command()
@click.argument("mis_path", type=click.Path(exists=True))
@click.option("--dataset-rid", type=str, required=True)
@click.option("--profile", type=str, required=True)
def process(mis_path: Path, dataset_rid: str, profile: str):
    mis_data = process_mis_csv(mis_path)
    channel_updater = Channel_Updater(dataset_rid, profile)
    channel_updater.update_channels(mis_data)
