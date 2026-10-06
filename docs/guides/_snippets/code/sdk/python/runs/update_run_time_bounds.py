for f in csv_dataset.list_files(successful_only=False):
    f.poll_until_ingestion_completed()

# After ingestion, we can query the data bound timestamps
csv_dataset.refresh()
bounds = csv_dataset.bounds

# And update the run accordingly
flight_simulator_run.update(start=bounds.start, end=bounds.end)
