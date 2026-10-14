import logging
import math
import random
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

import click
from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import Asset, NominalClient

logger = logging.getLogger(__name__)


def get_or_create_asset_or_guide_cleanup(
    client: NominalClient,
    properties: Mapping[str, str],
    *,
    name: str,
    description: str | None = None,
    labels: Sequence[str] = (),
) -> Asset:
    """Look up an asset by properties, creating one if none exists.

    Asset properties should uniquely identify the physical thing the asset
    represents. If multiple assets share these properties (for example,
    someone manually created a duplicate), log a red clean-up guide via
    the CLI's ClickLogHandler and exit.
    """
    try:
        return client.get_or_create_asset_by_properties(
            properties, name=name, description=description, labels=labels
        )
    except ValueError:
        conflicts = client.search_assets(properties=properties)
        conflict_lines = "\n".join(
            f"  - {a.name!r}: {a.nominal_url}" for a in conflicts
        )
        logger.error(
            f"Multiple assets in this workspace share properties {dict(properties)!r}:\n"
            f"{conflict_lines}\n"
            "\n"
            "Asset properties are key-value tags on an asset (for example, serial_num\n"
            "or car_number). They should uniquely identify the physical thing an asset\n"
            "represents, so a property search returns exactly one asset. Duplicates\n"
            "here mean an earlier tutorial session or another user already created an asset with\n"
            "these properties.\n"
            "\n"
            "Open each URL above, archive the ones you do not want to keep, then\n"
            "re-run this script. You can also archive from Python:\n"
            "\n"
            "    client.get_asset('<rid>').archive()"
        )
        sys.exit(1)


@click.command()
@client_options
@global_options
def main(client: NominalClient) -> None:
    """Stream continuous data to the FSAE tutorial asset's dataset."""
    # Attach our own streaming-session tag to every enqueued point so this
    # session is easy to find if others share the workspace. Nominal also
    # attaches _nominal_ingest_rid automatically, but it is surfaced only
    # in the UI.
    session_id = str(uuid.uuid4())
    click.secho(
        f"\nTutorial session ID: {session_id}\n"
        f"Filter by tag 'tutorial_session_id' = '{session_id}' in Nominal\n"
        f"to isolate this stream.\n",
        fg="green",
        bold=True,
    )
    tags = {"tutorial_session_id": session_id}

    asset = get_or_create_asset_or_guide_cleanup(
        client,
        properties={"team": "Example Racing", "car_number": "8"},
        name="FSAE CT8 Vehicle",
        description="Asset for FSAE single asset tutorial",
        labels=["FSAE", "Vehicle"],
    )
    print(f"Asset: {asset.name}, RID: {asset.rid}")

    dataset = asset.get_or_create_dataset(
        # Data scope name (reference name) for this dataset within the asset.
        # This can be used later to retrieve this dataset by the same name.
        data_scope_name="Primary Data Source",
        # Human readable name shown in the Nominal UI.
        name="FSAE CT8 Vehicle dataset",
        # Optional long-form description shown on the dataset page.
        description="Dataset for FSAE single asset tutorial",
    )
    print(f"Dataset: {dataset.name}, RID: {dataset.rid}")

    # Streams through Rust when nominal-streaming is installed (it is by default on
    # supported platforms), and falls back to pure Python otherwise.
    with dataset.get_write_stream() as stream:
        print(
            "Streaming all set up and queueing has started! Press ctrl+c to terminate streaming."
        )
        iteration = 0
        modes = ["Mode A", "Mode B", "Mode C"]

        while True:
            iteration += 1
            now = datetime.now()

            stream.enqueue(
                channel_name="sine_wave",
                timestamp=now,
                value=math.sin(2 * math.pi * iteration / 90),
                tags=tags,
            )

            if iteration % 2 == 0:
                stream.enqueue_from_dict(
                    timestamp=now,
                    # Many channels written together at the same tick; useful when
                    # multiple sensors sample simultaneously.
                    channel_values={
                        # Simulated CPU temperature (degC): baseline 55 with a 10 degC
                        # sinusoidal load swing and small noise.
                        "temperature": 55
                        + 10 * math.sin(2 * math.pi * iteration / 60)
                        + random.uniform(-2, 2),
                        "mode": modes[iteration % 3],
                    },
                    tags=tags,
                )

            # Nominal creates channels lazily on first write, so wait ~10s for
            # the server's channel index to catch up before setting units.
            # Values are UCUM symbols.
            if iteration == 100:
                dataset.set_channel_units(
                    {
                        "sine_wave": "V",
                        "temperature": "Cel",
                        "cpu_utilization": "%",
                    }
                )

            if iteration % 5 == 0:
                stream.enqueue_struct(
                    # Channel stores one JSON-serializable dict per point.
                    channel_name="cpu_info",
                    timestamp=now,
                    # Whole dict is stored as one nested value at (channel, timestamp).
                    # Must be JSON-serializable.
                    value={
                        "frequency_ghz": round(2.4 + random.uniform(0, 2), 2),
                        "active_cores": random.randint(2, 8),
                        "governor": random.choice(
                            ["performance", "powersave", "schedutil"]
                        ),
                    },
                    tags=tags,
                )

            if iteration % 10 == 0:
                stream.enqueue_batch(
                    channel_name="cpu_utilization",
                    # Points are keyed by (channel, timestamp), so timestamps must
                    # be distinct or later writes overwrite earlier ones at the
                    # same key. Spread 5 points 200ms apart, ending at the
                    # current timestamp.
                    timestamps=[
                        now - timedelta(milliseconds=200 * (4 - i)) for i in range(5)
                    ],
                    # One value per timestamp, aligned by index. Scaled 0-100 to match the "%" unit.
                    values=[random.uniform(0, 100) for _ in range(5)],
                    tags=tags,
                )

            time.sleep(0.1)


if __name__ == "__main__":
    main()
