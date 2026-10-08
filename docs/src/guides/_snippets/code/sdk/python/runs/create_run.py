from nominal.core import NominalClient

client = NominalClient.from_profile("default")

flight_simulator_run = client.create_run(
    name="High precipitation flight",
    start="2024-06-08T05:58:42Z",
    end="2024-06-08T06:00:06Z",
)

print("Created run:", flight_simulator_run.rid)
