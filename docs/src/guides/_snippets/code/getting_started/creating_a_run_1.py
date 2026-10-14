import datetime

# Wait for all files uploaded for this flight, across every modality.
for dataset_file in uploaded_files:
    dataset_file.poll_until_ingestion_completed()

# Use this recipe when the flight's absolute bounds are already known.
# Create the run in Nominal and associate with the Asset producing the data
run = asset.create_run(
    name="Test Flight #2 10/09/2024",
    start=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=13,
        minute=3,
        second=5,
        tzinfo=datetime.timezone.utc,
    ),
    end=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=14,
        minute=2,
        second=17,
        tzinfo=datetime.timezone.utc,
    ),
    # Useful for looking up runs later on by asset properties
    properties={"platform": "shimmer", "serial_num": "SN-001"},
    # Optional human description of the run, useful for describing what maneuvers we tested
    description="Testing unpowered glide",
)
