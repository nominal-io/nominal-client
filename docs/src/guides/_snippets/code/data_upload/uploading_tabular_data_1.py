# Get or create dataset linked to the asset.
# If a dataset with this data scope name already exists on the asset, it is returned.
# Otherwise, a new dataset is created and linked to the asset automatically.
dataset = asset.get_or_create_dataset(
    # Data scope name (reference name) for this dataset within the asset.
    # This can be used later to retrieve a reference to this dataset via the same reference name for
    # future flight tests.
    data_scope_name="flight_data",
    # Human readable name for the dataset
    name="Flight Data",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Flight data from onboard telemetry platform",
)
tabular_file = dataset.add_tabular_data(
    path="path/to/data.parquet",
    # Column within the parquet file containing absolute or relative timestamp information
    # for all other columns within the file
    timestamp_column="unix_timestamps",
    # Type of data contained within the timestamp column.
    # Using absolute floating point seconds from unix epoch for this example, but a wide variety
    # of formats are supported, such as other resolutions of time, relative timestamps, or even custom-formatted
    # string timestamps (e.g. ISO8601).
    timestamp_type="epoch_seconds",
)
uploaded_files.append(tabular_file)
