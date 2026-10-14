# Get or create the dataset linked to this asset.
dataset = asset.get_or_create_dataset(
    data_scope_name="flight_controller_logs",
    # Human readable name for the dataset
    name="Flight Controller Logs",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Systemd log data from the flight controller",
)

# Add the systemd journal JSON file to the dataset
log_file = dataset.add_journal_json("path/to/log.jsonl")
uploaded_files.append(log_file)
