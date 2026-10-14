from datetime import datetime, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

asset = client.create_asset("Aircraft", "My flight simulator aircraft")

flight_simulator_run = asset.create_run(
    name="High precipitation flight",
    start=datetime(2024, 9, 9, 12, 35, tzinfo=timezone.utc),
    end=datetime(2024, 9, 9, 13, 18, tzinfo=timezone.utc),
)

print("Created run:", flight_simulator_run.rid)
