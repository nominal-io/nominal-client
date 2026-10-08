from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Using utility from previous section on creating assets
# Hardcoding platform and serial number for this example, but typically this would be
# computed dynamically based on metadata about the data files, such as the filepath.
asset = retrieve_asset(client, platform="shimmer", serial_num="SN-001")

# Create dataset (only required for the first upload for a given data source)
dataset = client.create_dataset(
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
dataset.add_journal_json("path/to/log.jsonl")

# Attach the dataset to the asset
asset.add_dataset(
    # reference name for this dataset within the asset
    # This can be used later to retrieve a reference to this dataset via the same reference name for
    # future flight tests
    "flight_controller_logs",
    dataset,
)
