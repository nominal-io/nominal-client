from nominal.core import NominalClient

client = NominalClient.from_profile("default")

engine_fire_run = client.create_run(
    name="Engine Fire Run",
    start=start_time,
    end=end_time,
    description="Run for Raptor engine fire.",
)

engine_fire_run
