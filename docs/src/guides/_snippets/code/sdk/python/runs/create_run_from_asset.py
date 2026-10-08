from nominal.core import NominalClient

client = NominalClient.from_profile("default")

asset = client.create_asset("Aircraft", "My flight simulator aircraft")

flight_simulator_run = client.create_run(
    name="High precipitation flight",
    start="2024-09-09T12:35:00Z",
    end="2024-09-09T13:18:00Z",
    assets=[asset.rid],
)

print("Created run:", flight_simulator_run.rid)
