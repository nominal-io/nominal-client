flight_simulator_run = client.create_run(
    name="Frosty Flight", start=df["source_time"].min(), end=df["source_time"].max()
)
flight_simulator_run.add_dataset("measurements", dataset)
