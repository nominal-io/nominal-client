"""Create an asset and upload a CSV dataset to it.

Creates (or reuses) an FSAE vehicle asset, uploads the racecar CSV from the
[quickstart](/guides/quickstart.md) as its dataset, and tags each channel with its unit.
Run it with `python fsae_asset_upload.py --file racecar_dataset.csv.gz --profile <profile>`.
"""

import logging
import sys
import uuid
from collections.abc import Mapping, Sequence

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
        return client.get_or_create_asset_by_properties(properties, name=name, description=description, labels=labels)
    except ValueError:
        conflicts = client.search_assets(properties=properties)
        conflict_lines = "\n".join(f"  - {a.name!r}: {a.nominal_url}" for a in conflicts)
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
@click.option(
    "-f",
    "--file",
    "filepath",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the racecar_dataset.csv.gz file you downloaded.",
)
@client_options
@global_options
def main(filepath: str, client: NominalClient) -> None:
    """Create an FSAE asset in Nominal and upload a CSV dataset to it."""
    # Attach our own ingest tag to every point so the upload is easy to find
    # if others share the workspace. Nominal also attaches _nominal_ingest_rid
    # automatically, but it is surfaced only in the UI.
    session_id = str(uuid.uuid4())
    click.secho(
        f"\nTutorial session ID: {session_id}\n"
        f"Filter by tag 'tutorial_session_id' = '{session_id}' in Nominal\n"
        f"to isolate this upload.\n",
        fg="green",
        bold=True,
    )

    my_asset = get_or_create_asset_or_guide_cleanup(
        client,
        properties={"team": "Example Racing", "car_number": "8"},
        name="FSAE CT8 Vehicle",
        description="Asset for FSAE single asset tutorial",
        labels=["FSAE", "Vehicle"],
    )
    print("Using asset with RID: ", my_asset.rid)

    my_dataset = my_asset.get_or_create_dataset(
        # Data scope name (reference name) for this dataset within the asset.
        # This can be used later to retrieve this dataset by the same name.
        data_scope_name="Primary Data Source",
        # Human readable name shown in the Nominal UI.
        name="FSAE CT8 Vehicle dataset",
        # Optional long-form description shown on the dataset page.
        description="Dataset for FSAE single asset tutorial",
    )
    print("Created dataset with RID: ", my_dataset.rid)

    dataset_file = my_dataset.add_tabular_data(
        filepath,
        timestamp_column="Timestamps",
        timestamp_type="iso_8601",
        tags={"tutorial_session_id": session_id},
    )
    dataset_file.poll_until_ingestion_completed()
    print("Added CSV data to dataset! All Systems Nominal")

    # Tag every CSV channel that carries a unit in its column name using
    # strict UCUM symbols so Nominal can do unit conversions. UCUM quirks:
    # - psi      -> [psi]       (bracketed special unit)
    # - mph      -> [mi_i]/h    (international mile per hour)
    # - g-force  -> [g]         (standard gravity; bare "g" is UCUM for grams)
    # - rpm      -> /min        (reciprocal minute; 1 rpm = 1/60 Hz)
    my_dataset.set_channel_units(
        {
            "Distance_km": "km",
            "RR_Shock_mm": "mm",
            "RL_Shock_mm": "mm",
            "FL_Shock_mm": "mm",
            "FR_Shock_mm": "mm",
            "FL_Shock_Pos_Zero_mm": "mm",
            "FR_Shock_Pos_Zero_mm": "mm",
            "RL_Shock_Pos_Zero_mm": "mm",
            "RR_Shock_Pos_Zero_mm": "mm",
            "FL_Shock_Speed_mm/s": "mm/s",
            "FR_Shock_Speed_mm/s": "mm/s",
            "RL_Shock_Speed_mm/s": "mm/s",
            "RR_Shock_Speed_mm/s": "mm/s",
            "FL_Shock_Accel_mm/s/s": "mm/s2",
            "GPS_Altitude_m": "m",
            "GPS_PosAccuracy_m": "m",
            "AIM_DistanceMeters_m": "m",
            "GPS_Elevation_cm": "cm",
            "GPS_Latitude_°": "deg",
            "GPS_Longitude_°": "deg",
            "GPS_Slope_deg": "deg",
            "GPS_Heading_deg": "deg",
            "Re_Roll_Gradient_Degree": "deg",
            "GPS_Gyro_deg/s": "deg/s",
            "Battery_V": "V",
            "F88_V BATT_V": "V",
            "F88_BARO_PR_mbar": "mbar",
            "Rear_Brake_psi": "[psi]",
            "Front_brake_psi": "[psi]",
            "F88_OIL_P1_psi": "[psi]",
            "F88_FUEL_PR1_psi": "[psi]",
            "Run_Oil_Pres_psi": "[psi]",
            "Run_Oil_Pres_Hi_psi": "[psi]",
            "Load_Oil_Pres_psi": "[psi]",
            "Load_Oil_Pres_Hi_psi": "[psi]",
            "Load_Oil_Pres_Hi2_psi": "[psi]",
            "F88_V_SPEED_mph": "[mi_i]/h",
            "F88_D_SPEED_mph": "[mi_i]/h",
            "F88_SPEED_FL_mph": "[mi_i]/h",
            "F88_SPEED_FR_mph": "[mi_i]/h",
            "F88_SPEED_RL_mph": "[mi_i]/h",
            "F88_SPEED_RR_mph": "[mi_i]/h",
            "GPS_Speed_mph": "[mi_i]/h",
            "Acc_Lateral_g": "[g]",
            "Acc_Longitudin_g": "[g]",
            "GPS_LatAcc_g": "[g]",
            "GPS_LonAcc_g": "[g]",
            "F88_RPM_rpm": "/min",
            "Fuel Used_Liters": "L",
            "Fuel Flow_cc/min": "cm3/min",
        },
        allow_display_only_units=True,
    )


if __name__ == "__main__":
    main()
