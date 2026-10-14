from nominal.core import NominalClient

client = NominalClient.from_profile("default")

found_runs = client.search_runs(name_substring="High precipitation flight")

if len(found_runs) == 0:
    flight_simulator_run = client.create_run(
        name="High precipitation flight",
        start="2024-09-09T12:35:00Z",
        end="2024-09-09T13:18:00Z",
    )
