# Keep only the handles returned by uploads for this test, not every file on
# the dataset, which may also contain earlier tests.
file_bounds = []
for dataset_file in uploaded_files:
    dataset_file.poll_until_ingestion_completed()
    if dataset_file.bounds is None:
        raise ValueError(f"No time bounds available for {dataset_file.name!r}")
    file_bounds.append(dataset_file.bounds)

flight_simulator_run.update(
    start=min(bounds.start for bounds in file_bounds),
    end=max(bounds.end for bounds in file_bounds),
)
