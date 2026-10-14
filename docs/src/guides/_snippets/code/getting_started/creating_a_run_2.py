# Use the handles collected in this flight's upload stage, across every modality.
# The dataset's own bounds may also include earlier flights.
if not uploaded_files:
    raise ValueError("Upload at least one file before deriving run bounds")

file_bounds = []
for dataset_file in uploaded_files:
    dataset_file.poll_until_ingestion_completed()
    if dataset_file.bounds is None:
        raise ValueError(f"No time bounds available for {dataset_file.name!r}")
    file_bounds.append(dataset_file.bounds)

# File bounds and run timestamps use epoch nanoseconds, so no datetime conversion
# is needed.
run = asset.create_run(
    name="Test Flight #2 10/09/2024",
    start=min(bounds.start for bounds in file_bounds),
    end=max(bounds.end for bounds in file_bounds),
)
