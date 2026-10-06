import datetime

from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Using utility from previous section on creating assets
# Hardcoding platform and serial number for this example, but typically this would be
# computed dynamically based on metadata about the data files, such as the filepath.
asset = retrieve_asset(client, platform="shimmer", serial_num="SN-001")

# Create the run in Nominal and associate with the Asset producing the data
run = client.create_run(
    name="Test Flight #2 10/09/2024",
    start=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=13,
        minute=3,
        second=5,
        tzinfo=datetime.timezone.utc,
    ),
    end=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=14,
        minute=2,
        second=17,
        tzinfo=datetime.timezone.utc,
    ),
    # Useful for looking up runs later on by asset properties
    properties={"platform": "shimmer", "serial_num": "SN-001"},
    # Link back to the asset containing data for this run
    assets=[asset],
    # Optional human description of the run, useful for describing what maneuvers we tested
    description="Testing unpowered glide",
)
