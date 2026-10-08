# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("flight_data")

dataset.add_tabular_data(
    "path/to/other/data.parquet",
    # column containing timestamp information for this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_column="unix_timestamps",
    # type of timestamps stored in this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_type="epoch_seconds",
)
