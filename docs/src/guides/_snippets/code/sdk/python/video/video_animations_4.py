from nominal.core import NominalClient

client = NominalClient.from_profile("default")

csv_dataset = client.create_dataset("double pendulum")
csv_dataset_file = csv_dataset.add_tabular_data(
    path="double_pendulum_full_headers.csv",
    timestamp_column="Timestamp (ISO8601)",
    timestamp_type="iso_8601",
)
csv_dataset_file.poll_until_ingestion_completed()
csv_dataset.refresh()

# Now create a run and attach the data to it

sim_run = client.create_run(
    name="double_pendulum_run",
    start=csv_dataset.bounds.start,
    end=csv_dataset.bounds.end,
)
sim_run.add_dataset(ref_name="measurements", dataset=csv_dataset)
