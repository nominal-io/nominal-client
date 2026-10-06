from nominal.core import NominalClient

client = NominalClient.from_profile("default")

dataset = client.create_dataset(
    name="Frosty Flight measurements",
)
dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
