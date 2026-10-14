# Reuse the same refname for the next file.
dataset = asset.get_or_create_dataset("flight_data", name="Flight Data")

tabular_file = dataset.add_tabular_data(
    "path/to/other/data.parquet",
    # column containing timestamp information for this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_column="unix_timestamps",
    # type of timestamps stored in this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_type="epoch_seconds",
)
uploaded_files.append(tabular_file)
